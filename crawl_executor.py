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

import sys
import threading
import time

import mesos
import mesos_pb2
import results

class LaughingExecutor(mesos.Executor):
    def registered(self, driver, executorInfo, frameworkInfo, slaveInfo):
      pass

    def reregistered(self, driver, slaveInfo):
      pass

    def disconnected(self, driver):
      pass

    def launchTask(self, driver, task):
        def run_task():
            print "Running task %s" % task.task_id.value
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_RUNNING
            driver.sendStatusUpdate(update)

            # This is where one would perform the requested task.
            res = CrawlResult(
              "1234",
              "http://foo.co",
              ["http://foo.co/a", "http://foo.co/b"]
            )
            message = repr(res)
            driver.sendFrameworkMessage(message)

            print "Sending status update..."
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_FINISHED
            driver.sendStatusUpdate(update)
            print "Sent status update"

        thread = threading.Thread(target=run_task)
        thread.start()

    def killTask(self, driver, taskId):
      pass

    def frameworkMessage(self, driver, message):
      pass

    def shutdown(self, driver):
      pass

    def error(self, error, message):
      pass

if __name__ == "__main__":
    print "Starting Launching Executor (LE)"
    driver = mesos.MesosExecutorDriver(LaughingExecutor())
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)
