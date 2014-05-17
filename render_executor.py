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
from threading import Thread
from subprocess import call
import time
import uuid

import mesos
import mesos_pb2
import results

class RenderExecutor(mesos.Executor):
    def registered(self, driver, executorInfo, frameworkInfo, slaveInfo):
      pass

    def reregistered(self, driver, slaveInfo):
      pass

    def disconnected(self, driver):
      pass

    def launchTask(self, driver, task):
        def run_task():
            print "Running render task %s" % task.task_id.value
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_RUNNING
            driver.sendStatusUpdate(update)

            def render(url):
                # 1) Render picture to hash file name.
                destination = uuid.uuid4().hex + ".png"
                if call(["phantomjs", "render.js", url, destination]) != 0:
                    print "Could not render " + url
                    return

                # 2) Upload to s3.
                s3destination = "s3://downloads.mesosphere.io/demo/artifacts/" + destination
                if call(["s3cmd", "put", destination, s3destination]) != 0:
                    print "Could not upload " + destination + " to " + s3destination
                    return

                # 3) Announce render result to framework.
                res = results.RenderResult(
                    task.task_id.value,
                    url,
                    s3destination
                )
                message = repr(res)
                driver.sendFrameworkMessage(message)

                print "Sending status update..."
                update = mesos_pb2.TaskStatus()
                update.task_id.value = task.task_id.value
                update.state = mesos_pb2.TASK_FINISHED
                driver.sendStatusUpdate(update)
                print "Sent status update"
                return

            crawlThread = Thread(target = render, args = [task.data])
            crawlThread.start();


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
    print "Starting Render Executor (RE)"
    driver = mesos.MesosExecutorDriver(RenderExecutor())
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)
