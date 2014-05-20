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

from subprocess import call
from threading import Thread
import os
import sys
import threading
import time
import uuid

import mesos
import mesos_pb2
import results

class RenderExecutor(mesos.Executor):
    def __init__(self, local):
        self.local = local

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

            url = task.data

            # 1) Render picture to hash file name.
            destination = uuid.uuid4().hex + ".png"
            if call(["phantomjs", "render.js", url, destination]) != 0:
                print "Could not render " + url
                return

            if not self.local:
              # 2) Upload to s3.
              remote_destination = "s3://downloads.mesosphere.io/demo/artifacts/" + destination
              print "Uploading image to " + remote_destination
              if call(["s3cmd", "put", destination, remote_destination]) != 0:
                  print "Could not upload " + destination + " to " + remote_destination
                  return
            else:
              remote_destination = "file:///" + os.getcwd() + "/" + destination


            # 3) Announce render result to framework.
            print "Announcing render result"
            res = results.RenderResult(
                task.task_id.value,
                url,
                remote_destination
            )
            message = repr(res)
            driver.sendFrameworkMessage(message)

            print "Sending status update for task %s" % task.task_id.value
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_FINISHED
            driver.sendStatusUpdate(update)
            print "Sent status update for task %s" % task.task_id.value
            return



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
    local = False
    if len(sys.argv) == 2 and sys.argv[1] == "--local":
      local = True

    driver = mesos.MesosExecutorDriver(RenderExecutor(local))
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)
