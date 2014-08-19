#!/usr/bin/env python

from collections import deque
import json
import os
import signal
import sys
import time
import datetime
from threading import Thread

try:
    from mesos.native import MesosExecutorDriver, MesosSchedulerDriver
    from mesos.interface import Executor, Scheduler
    from mesos.interface import mesos_pb2
except ImportError:
    from mesos import Executor, MesosExecutorDriver, MesosSchedulerDriver, Scheduler
    import mesos_pb2

import results
import task_state
import export_dot

TASK_CPUS = 0.1
TASK_MEM = 32
SHUTDOWN_TIMEOUT = 30  # in seconds
LEADING_ZEROS_COUNT = 5  # appended to task ID to facilitate lexicographical order
TASK_ATTEMPTS = 5  # how many times a task is attempted

CRAWLER_TASK_SUFFIX = "-crwl"
RENDER_TASK_SUFFIX = "-rndr"

# See the Mesos Framework Development Guide:
# http://mesos.apache.org/documentation/latest/app-framework-development-guide

class RenderingCrawler(Scheduler):
    def __init__(self, seedUrl, maxRenderTasks, crawlExecutor, renderExecutor):
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
        self.tasksFailed = 0
        self.tasksRetrying = {}
        self.renderLimitReached = False
        self.maxRenderTasks = maxRenderTasks
        self.shuttingDown = False

    def registered(self, driver, frameworkId, masterInfo):
        print "Registered with framework ID [%s]" % frameworkId.value

    def makeTaskPrototype(self, offer):
        task = mesos_pb2.TaskInfo()
        tid = self.tasksCreated
        self.tasksCreated += 1
        task.task_id.value = str(tid).zfill(LEADING_ZEROS_COUNT)
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
        task.name = "crawl task %s" % task.task_id.value
        task.task_id.value += CRAWLER_TASK_SUFFIX
        task.executor.MergeFrom(self.crawlExecutor)
        task.data = str(url)
        return task

    def makeRenderTask(self, url, offer):
        task = self.makeTaskPrototype(offer)
        task.name = "render task %s" % task.task_id.value
        task.task_id.value += RENDER_TASK_SUFFIX
        task.executor.MergeFrom(self.renderExecutor)
        task.data = str(url)
        return task
    
    def retryTask(self, task_id, url):
        if not url in self.tasksRetrying:
            self.tasksRetrying[url] = 1
            
        if self.tasksRetrying[url] < TASK_ATTEMPTS:
            self.tasksRetrying[url] += 1
            ordinal = lambda n: "%d%s" % (n, \
              "tsnrhtdd"[(n / 10 % 10 != 1) * (n % 10 < 4) * n % 10::4])
            print "%s try for \"%s\"" % \
              (ordinal(self.tasksRetrying[url]), url)

            # TODO(alex): replace this by checking TaskStatus.executor_id,
            # which is available in mesos 0.20
            if task_id.endswith(CRAWLER_TASK_SUFFIX):
              self.crawlQueue.append(url)
            elif task_id.endswith(RENDER_TASK_SUFFIX):
              self.renderQueue.append(url)
        else:
            self.tasksFailed += 1
            print "Task for \"%s\" cannot be completed, attempt limit reached" % url

    def printStatistics(self):
        print "Queue length: %d crawl, %d render; Tasks: %d running, %d failed" % (
          len(self.crawlQueue), len(self.renderQueue), self.tasksRunning, self.tasksFailed
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
        self.printStatistics()
        
        if not self.crawlQueue and not self.renderQueue and self.tasksRunning <= 0:
            print "Nothing to do, RENDLER is shutting down"
            hard_shutdown()

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
            
        elif update.state == 3:  # Failed, retry
            print "Task [%s] failed with message \"%s\"" \
              % (update.task_id.value, update.message)
            self.tasksRunning -= 1
            self.retryTask(update.task_id.value, update.data)
       
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
                if not self.renderLimitReached and self.maxRenderTasks > 0 and \
                  self.maxRenderTasks <= len(self.processedURLs):
                    print "Render task limit (%d) reached" % self.maxRenderTasks
                    self.renderLimitReached = True
                if not link in self.processedURLs and not self.renderLimitReached:
                    print "Enqueueing [%s]" % link
                    self.crawlQueue.append(link)
                    self.renderQueue.append(link)
                    self.processedURLs.add(link)

        elif executorId.value == renderExecutor.executor_id.value:
            result = results.RenderResult(o['taskId'], o['url'], o['imageUrl'])
            print "Appending [%s] to render results" % repr((result.url, result.imageUrl))
            self.renderResults[result.url] = result.imageUrl

def hard_shutdown():  
    driver.stop()

def graceful_shutdown(signal, frame):
    print "RENDLER is shutting down"
    rendler.shuttingDown = True
    
    wait_started = datetime.datetime.now()
    while (rendler.tasksRunning > 0) and \
      (SHUTDOWN_TIMEOUT > (datetime.datetime.now() - wait_started).total_seconds()):
        time.sleep(1)
    
    if (rendler.tasksRunning > 0):
        print "Shutdown by timeout, %d task(s) have not completed" % rendler.tasksRunning

    hard_shutdown()

#
# Execution entry point:
#
if __name__ == "__main__":
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print "Usage: %s seedUrl mesosMasterUrl [maxRenderTasks]" % sys.argv[0]
        sys.exit(1)

    baseURI = "/home/vagrant/sandbox/mesosphere/mesos-sdk/RENDLER"
    suffixURI = "python"
    uris = [ "crawl_executor.py",
             "export_dot.py",
             "render_executor.py",
             "results.py",
             "task_state.py" ]
    uris = [os.path.join(baseURI, suffixURI, uri) for uri in uris]
    uris.append(os.path.join(baseURI, "render.js"))

    crawlExecutor = mesos_pb2.ExecutorInfo()
    crawlExecutor.executor_id.value = "crawl-executor"
    crawlExecutor.command.value = "python crawl_executor.py"

    for uri in uris:
        uri_proto = crawlExecutor.command.uris.add()
        uri_proto.value = uri
        uri_proto.extract = False

    crawlExecutor.name = "Crawler"

    renderExecutor = mesos_pb2.ExecutorInfo()
    renderExecutor.executor_id.value = "render-executor"
    renderExecutor.command.value = "python render_executor.py --local"

    for uri in uris:
        uri_proto = renderExecutor.command.uris.add()
        uri_proto.value = uri
        uri_proto.extract = False

    renderExecutor.name = "Renderer"

    framework = mesos_pb2.FrameworkInfo()
    framework.user = "" # Have Mesos fill in the current user.
    framework.name = "RENDLER"

    try: maxRenderTasks = int(sys.argv[3])
    except: maxRenderTasks = 0
    rendler = RenderingCrawler(sys.argv[1], maxRenderTasks, crawlExecutor, renderExecutor)

    driver = MesosSchedulerDriver(rendler, framework, sys.argv[2])

    # driver.run() blocks; we run it in a separate thread
    def run_driver_async():
        status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1
        driver.stop()
        sys.exit(status)
    framework_thread = Thread(target = run_driver_async, args = ())
    framework_thread.start()

    print "(Listening for Ctrl-C)"
    signal.signal(signal.SIGINT, graceful_shutdown)
    while framework_thread.is_alive():
        time.sleep(1)

    export_dot.dot(rendler.crawlResults, rendler.renderResults, "result.dot")
    print "Goodbye!"
    sys.exit(0)
