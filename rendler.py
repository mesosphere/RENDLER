#!/usr/bin/env python

from collections import deque
import json
import os
import signal
import sys
import time
from threading import Thread

import mesos
import mesos_pb2
import results
import task_state
import export_dot

TASK_CPUS = 0.1
TASK_MEM = 32

# See the Mesos Framework Development Guide:
# http://mesos.apache.org/documentation/latest/app-framework-development-guide

class RenderingCrawler(mesos.Scheduler):
    def __init__(self, seedUrl, crawlExecutor, renderExecutor):
        print "RENDLER"
        print "======="
        print "seedUrl: [%s]\n" % seedUrl
        self.seedUrl = seedUrl
        self.crawlExecutor  = crawlExecutor
        self.renderExecutor = renderExecutor
        self.crawlQueue = deque([seedUrl])
        self.renderQueue = deque([seedUrl])
        self.processedURLs = set([seedUrl])
        self.crawlResults = set([])
        self.renderResults = {}
        self.tasksCreated  = 0
        self.tasksRunning = 0
        self.shuttingDown = False

    def registered(self, driver, frameworkId, masterInfo):
        print "Registered with framework ID [%s]" % frameworkId.value

    def makeTaskPrototype(self, offer):
        task = mesos_pb2.TaskInfo()
        tid = self.tasksCreated
        self.tasksCreated += 1
        task.task_id.value = str(tid)
        task.slave_id.value = offer.slave_id.value
        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = TASK_CPUS
        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = TASK_MEM
        return task

    def makeCrawlTask(self, url, offer):
        task = self.makeTaskPrototype(offer)
        task.name = "crawl_%s" % task.task_id
        task.executor.MergeFrom(self.crawlExecutor)
        task.data = str(url)
        return task

    def makeRenderTask(self, url, offer):
        task = self.makeTaskPrototype(offer)
        task.name = "render_%s" % task.task_id
        task.executor.MergeFrom(self.renderExecutor)
        task.data = str(url)
        return task

    def printQueueStatistics(self):
        print "Crawl queue length: %d, Render queue length: %d, Running tasks: %d" % (
            len(self.crawlQueue), len(self.renderQueue), self.tasksRunning
        )

    def maxTasksForOffer(self, offer):
        count = 0
        cpus = next(rsc.scalar.value for rsc in offer.resources if rsc.name == "cpus")
        mem = next(rsc.scalar.value for rsc in offer.resources if rsc.name == "mem")
        while cpus >= TASK_CPUS and mem >= TASK_MEM:
            count += 1
            cpus -= TASK_CPUS
            mem -= TASK_MEM
        return count

    def resourceOffers(self, driver, offers):
        self.printQueueStatistics()

        for offer in offers:
            print "Got resource offer [%s]" % offer.id.value

            if self.shuttingDown:
                print "Shutting down: declining offer on [%s]" % offer.hostname
                driver.declineOffer(offer.id)
                continue

            maxTasks = self.maxTasksForOffer(offer)

            print "maxTasksForOffer: [%d]" % maxTasks

            tasks = []

            for i in range(maxTasks / 2):
                if self.crawlQueue:
                    crawlTaskUrl = self.crawlQueue.popleft()
                    task = self.makeCrawlTask(crawlTaskUrl, offer)
                    tasks.append(task)
                if self.renderQueue:
                    renderTaskUrl = self.renderQueue.popleft()
                    task = self.makeRenderTask(renderTaskUrl, offer)
                    tasks.append(task)

            if tasks:
                print "Accepting offer on [%s]" % offer.hostname
                driver.launchTasks(offer.id, tasks)
            else:
                print "Declining offer on [%s]" % offer.hostname
                driver.declineOffer(offer.id)

    def statusUpdate(self, driver, update):
        stateName = task_state.nameFor[update.state]
        print "Task [%s] is in state [%s]" % (update.task_id.value, stateName)

        if update.state == 1:  # Running
            self.tasksRunning += 1
        elif update.state == 3:  # Failed
            self.tasksRunning -= 1  # consider rescheduling the task
            print "Task [%s] failed with message \"%s\"" \
                % (update.task_id.value, update.message)
        elif self.tasksRunning > 0 and update.state > 1: # Terminal state
            self.tasksRunning -= 1

    def frameworkMessage(self, driver, executorId, slaveId, message):
        o = json.loads(message)

        if executorId.value == crawlExecutor.executor_id.value:
            result = results.CrawlResult(o['taskId'], o['url'], o['links'])
            for link in result.links:
                edge = (result.url, link)
                print "Appending [%s] to crawl results" % repr(edge)
                self.crawlResults.add(edge)
                if not link in self.processedURLs:
                    print "Enqueueing [%s]" % link
                    self.crawlQueue.append(link)
                    self.renderQueue.append(link)
                    self.processedURLs.add(link)

        elif executorId.value == renderExecutor.executor_id.value:
            result = results.RenderResult(o['taskId'], o['url'], o['imageUrl'])
            print "Appending [%s] to render results" % repr((result.url, result.imageUrl))
            self.renderResults[result.url] = result.imageUrl

def shutdown(signal, frame):
    print "Rendler is shutting down"
    rendler.shuttingDown = True
    while rendler.tasksRunning > 0:
        time.sleep(1)
    driver.stop()
    export_dot.dot(rendler.crawlResults, rendler.renderResults, "result.dot")
    print "Goodbye!"
    sys.exit(0)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "Usage: %s seedUrl mesosMasterUrl" % sys.argv[0]
        sys.exit(1)

    uris = [ "crawl_executor.py",
             "export_dot.py",
             "render.js",
             "render_executor.py",
             "results.py",
             "task_state.py" ]

    crawlExecutor = mesos_pb2.ExecutorInfo()
    crawlExecutor.executor_id.value = "crawl-executor"
    crawlExecutor.command.value = "python crawl_executor.py"

    for uri in uris:
        uri_proto = crawlExecutor.command.uris.add()
        uri_proto.value = "/home/vagrant/hostfiles/" + uri
        uri_proto.extract = False

    crawlExecutor.name = "Crawler"

    renderExecutor = mesos_pb2.ExecutorInfo()
    renderExecutor.executor_id.value = "render-executor"
    renderExecutor.command.value = "python render_executor.py --local"

    for uri in uris:
        uri_proto = renderExecutor.command.uris.add()
        uri_proto.value = "/home/vagrant/hostfiles/" + uri
        uri_proto.extract = False

    renderExecutor.name = "Renderer"

    framework = mesos_pb2.FrameworkInfo()
    framework.user = "" # Have Mesos fill in the current user.
    framework.name = "RENDLER"

    rendler = RenderingCrawler(sys.argv[1], crawlExecutor, renderExecutor)

    driver = mesos.MesosSchedulerDriver(rendler, framework, sys.argv[2])

    # driver.run() blocks; we run it in a separate thread
    def run_driver_async():
        status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1
        driver.stop()
        sys.exit(status)

    Thread(target = run_driver_async, args = ()).start()

    print "(Listening for Ctrl-C)"
    signal.signal(signal.SIGINT, shutdown)
    while True: time.sleep(1)
