#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections import deque
import os
import signal
import sys
from threading import Thread
import time
from urlparse import urlparse

import mesos
import mesos_pb2
import results

TASK_CPUS = 0.1
TASK_MEM = 32

class RenderingCrawler(mesos.Scheduler):
    def __init__(self, seedUrl, crawlExecutor, renderExecutor):
        self.seedUrl = seedUrl
        self.seedDomain = urlparse(seedUrl).netloc
        self.crawlExecutor  = crawlExecutor
        self.renderExecutor = renderExecutor
        self.crawlQueue = deque([seedUrl])
        self.renderQueue = deque([seedUrl])
        self.processedURLs = set([seedUrl])
        self.crawlResults = []
        self.renderResults = {}
        self.tasksCreated  = 0
        self.tasksLaunched  = 0
        self.tasksFinished  = 0

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
        task.data = url
        return task

    def makeRenderTask(self, url, offer):
        task = self.makeTaskPrototype(offer)
        task.name = "render_%s" % task.task_id
        task.executor.MergeFrom(self.renderExecutor)
        task.data = url
        return task

    def matchesSeedDomain(url):
        return urlparse(url).netloc == self.seedDomain

    def resourceOffers(self, driver, offers):
        print "Got %d resource offers" % len(offers)

        for offer in offers:
            print "Got resource offer [%s]" % offer.id.value
            print "Accepting offer on [%s]" % offer.hostname

            # TODO: this better
            tasks = []
            task = self.makeCrawlTask(self.crawlQueue[0], offer)
            tasks.append(task)

            tasks.append(self.makeRenderTask(self.crawlQueue[0], offer))

            driver.launchTasks(offer.id, tasks)

    def statusUpdate(self, driver, update):
        print "Task [%s] is in state [%d]" % (update.task_id.value, update.state)

    def frameworkMessage(self, driver, executorId, slaveId, message):
        print "Received message:", repr(str(message))
        if (type(message) is results.CrawlResult):
            for url in message.links:
                self.crawlResults.append((message.url, url))
                if matchesSeedDomain(url) and not url in self.processedURLs:
                    self.crawlQueue.append(url)
                    self.renderQueue.append(url)
                    self.processedUrls.add(url)

        elif (type(message) is results.RenderResult):
            self.renderResults[message.url] = message.imageUrl

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "Usage: %s seedUrl master" % sys.argv[0]
        sys.exit(1)

    crawlExecutor = mesos_pb2.ExecutorInfo()
    crawlExecutor.executor_id.value = "crawl-executor"
    crawlExecutor.command.value = "python crawl_executor.py"

    crawlSource = crawlExecutor.command.uris.add()
    crawlSource.value = "http://downloads.mesosphere.io/demo/laughing-adventure.tgz"

    crawlExecutor.name = "Crawler"
    crawlExecutor.source = "rendering-crawler"

    renderExecutor = mesos_pb2.ExecutorInfo()
    renderExecutor.executor_id.value = "render-executor"
    renderExecutor.command.value = "python render_executor.py"

    renderSource = renderExecutor.command.uris.add()
    renderSource.value = crawlSource.value

    renderExecutor.name = "Renderer"
    renderExecutor.source = "rendering-crawler"

    framework = mesos_pb2.FrameworkInfo()
    framework.user = "" # Have Mesos fill in the current user.
    framework.name = "rendering-crawler"

    if os.getenv("MESOS_CHECKPOINT"):
        print "Enabling checkpoint for the framework"
        framework.checkpoint = True

    if os.getenv("MESOS_AUTHENTICATE"):
        print "Enabling authentication for the framework"

        if not os.getenv("DEFAULT_PRINCIPAL"):
            print "Expecting authentication principal in the environment"
            sys.exit(1);

        if not os.getenv("DEFAULT_SECRET"):
            print "Expecting authentication secret in the environment"
            sys.exit(1);

        credential = mesos_pb2.Credential()
        credential.principal = os.getenv("DEFAULT_PRINCIPAL")
        credential.secret = os.getenv("DEFAULT_SECRET")

        driver = mesos.MesosSchedulerDriver(
            RenderingCrawler(sys.argv[1], crawlExecutor, renderExecutor),
            framework,
            sys.argv[2],
            credential)
    else:
        driver = mesos.MesosSchedulerDriver(
            RenderingCrawler(sys.argv[1], crawlExecutor, renderExecutor),
            framework,
            sys.argv[2])

    # driver.run() blocks; we run it in a separate thread
    def run_driver_async():
        status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1
        driver.stop()
        sys.exit(status)

    driverRunThread = Thread(target = run_driver_async, args = ())
    driverRunThread.start()

    # Listen for CTRL+D
    while True:
        line = sys.stdin.readline()
        if line:
            pass
        else:
            driver.stop()
            print "Goodbye!"
            sys.exit(0)
