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

import os
import sys
import time

import mesos
import mesos_pb2

TOTAL_TASKS = 5

TASK_CPUS = 0.25
TASK_MEM = 32

class RenderingCrawler(mesos.Scheduler):
    def __init__(self, crawlExecutor, renderExecutor):
        self.crawlExecutor  = crawlExecutor
        self.renderExecutor = renderExecutor
        self.taskData       = {}
        self.tasksLaunched  = 0
        self.tasksFinished  = 0

    def registered(self, driver, frameworkId, masterInfo):
        print "Registered with framework ID [%s]" % frameworkId.value

    def resourceOffers(self, driver, offers):
        print "Got %d resource offers" % len(offers)
        for offer in offers:
            tasks = []
            print "Got resource offer [%s]" % offer.id.value
            if self.tasksLaunched < TOTAL_TASKS:
                tid = self.tasksLaunched
                self.tasksLaunched += 1

                print "Accepting offer on [%s] to start task [%d]" % (offer.hostname, tid)

                task = mesos_pb2.TaskInfo()
                task.task_id.value = str(tid)
                task.slave_id.value = offer.slave_id.value
                task.name = "task %d" % tid
                task.executor.MergeFrom(self.crawlExecutor)

                cpus = task.resources.add()
                cpus.name = "cpus"
                cpus.type = mesos_pb2.Value.SCALAR
                cpus.scalar.value = TASK_CPUS

                mem = task.resources.add()
                mem.name = "mem"
                mem.type = mesos_pb2.Value.SCALAR
                mem.scalar.value = TASK_MEM

                tasks.append(task)
                self.taskData[task.task_id.value] = (
                    offer.slave_id, task.executor.executor_id)
            driver.launchTasks(offer.id, tasks)

    def statusUpdate(self, driver, update):
        print "Task [%s] is in state [%d]" % (update.task_id.value, update.state)
        slave_id, executor_id = self.taskData[update.task_id.value]

    def frameworkMessage(self, driver, executorId, slaveId, message):
        print "Received message:", repr(str(message))

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: %s master" % sys.argv[0]
        sys.exit(1)

    crawlExecutor = mesos_pb2.ExecutorInfo()
    crawlExecutor.executor_id.value = "crawl-executor"
    crawlExecutor.command.value = "python ./crawl_executor.py"

    source = crawlExecutor.command.uris.add()
    source.value = "http://downloads.mesosphere.io/demo/laughing-adventure.tgz"

    crawlExecutor.name = "Crawler"
    crawlExecutor.source = "rendering-crawler"

    renderExecutor = mesos_pb2.ExecutorInfo()
    renderExecutor.executor_id.value = "render-executor"
    renderExecutor.command.value = os.path.abspath("./render-executor.py")
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
            RenderingCrawler(crawlExecutor, renderExecutor),
            framework,
            sys.argv[1],
            credential)
    else:
        driver = mesos.MesosSchedulerDriver(
            RenderingCrawler(crawlExecutor, renderExecutor),
            framework,
            sys.argv[1])

    status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1

    # Ensure that the driver process terminates.
    driver.stop();

    sys.exit(status)
