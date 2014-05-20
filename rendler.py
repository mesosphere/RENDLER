#!/usr/bin/env python

from collections import deque
import json
import os
import signal
import sys
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

    def resourceOffers(self, driver, offers):
        for offer in offers:
            print "Got resource offer [%s]" % offer.id.value

            tasks = []

            if self.crawlQueue:
                crawlTaskUrl = self.crawlQueue.popleft()
                task = self.makeCrawlTask(crawlTaskUrl, offer)
                tasks.append(task)

            if self.renderQueue:
                renderTaskUrl = self.renderQueue.popleft()
                task = self.makeRenderTask(crawlTaskUrl, offer)
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

    def frameworkMessage(self, driver, executorId, slaveId, message):
        o = json.loads(message)

        if executorId.value == crawlExecutor.executor_id.value:
            result = results.CrawlResult(o['taskId'], o['url'], o['links'])
            for url in result.links:
                edge = (result.url, url)
                print "Appending [%s] to crawl results" % repr(edge)
                self.crawlResults.add(edge)
                if not url in self.processedURLs:
                    print "Enqueueing [%s]" % url
                    self.crawlQueue.append(url)
                    self.renderQueue.append(url)
                    self.processedURLs.add(url)

        elif executorId.value == renderExecutor.executor_id.value:
            result = results.RenderResult(o['taskId'], o['url'], o['imageUrl'])
            print "Appending [%s] to render results" % repr((result.url, result.imageUrl))
            self.renderResults[result.url] = result.imageUrl

if __name__ == "__main__":
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print "Usage: %s seedUrl mesosMasterUrl [--local]" % sys.argv[0]
        sys.exit(1)

    localMode = len(sys.argv) == 4 and sys.argv[3] == "--local"

    rendlerArtifact = "http://downloads.mesosphere.io/demo/rendler.tgz"

    crawlExecutor = mesos_pb2.ExecutorInfo()
    crawlExecutor.executor_id.value = "crawl-executor"
    crawlExecutor.command.value = "python crawl_executor.py"
    crawlExecutor.command.uris.add().value = rendlerArtifact
    crawlExecutor.name = "Crawler"
    crawlExecutor.source = "rendering-crawler"

    renderExecutor = mesos_pb2.ExecutorInfo()
    renderExecutor.executor_id.value = "render-executor"

    renderExecutor.command.value = "python render_executor.py"
    if localMode:
        renderExecutor.command.value += " --local"

    renderExecutor.command.uris.add().value = rendlerArtifact
    renderExecutor.name = "Renderer"
    renderExecutor.source = "rendering-crawler"

    framework = mesos_pb2.FrameworkInfo()
    framework.user = "" # Have Mesos fill in the current user.
    framework.name = "rendering-crawler"

    if os.getenv("MESOS_CHECKPOINT"):
        print "Enabling checkpoint for the framework"
        framework.checkpoint = True

    crawler = RenderingCrawler(sys.argv[1], crawlExecutor, renderExecutor)

    driver = mesos.MesosSchedulerDriver(
        crawler,
        framework,
        sys.argv[2])

    # driver.run() blocks; we run it in a separate thread
    def run_driver_async():
        status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1
        driver.stop()
        sys.exit(status)

    Thread(target = run_driver_async, args = ()).start()

    # Listen for CTRL+D
    while True:
        line = sys.stdin.readline()
        if not line:
            driver.stop()
            export_dot.dot(crawler.crawlResults, crawler.renderResults, "result.dot")
            print "Goodbye!"
            sys.exit(0)
