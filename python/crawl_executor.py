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
import time

import urlparse, urllib, sys
from bs4 import BeautifulSoup

import mesos
import mesos_pb2
import results

class CrawlExecutor(mesos.Executor):
    def registered(self, driver, executorInfo, frameworkInfo, slaveInfo):
      print "CrawlExecutor registered"

    def reregistered(self, driver, slaveInfo):
      print "CrawlExecutor reregistered"

    def disconnected(self, driver):
      print "CrawlExecutor disconnected"

    def launchTask(self, driver, task):
        def run_task():
            print "Running crawl task %s" % task.task_id.value
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_RUNNING
            driver.sendStatusUpdate(update)

            url = task.data

            source = ""
            try:
                source = urllib.urlopen(url).read()
            except:
                error_msg = "Could not read resource at %s" % url
                update = mesos_pb2.TaskStatus()
                update.task_id.value = task.task_id.value
                update.state = mesos_pb2.TASK_FAILED
                update.message = error_msg
                update.data = url
                
                driver.sendStatusUpdate(update)
                print error_msg
                return
                
            soup = BeautifulSoup(source)

            links = []
            try:
              for item in soup.find_all('a'):
                  try:
                      links.append(urlparse.urljoin(url, item.get('href')))
                  except:
                      pass # Not a valid link
            except:
              print "Could not fetch any links from html"
              return

            res = results.CrawlResult(
              task.task_id.value,
              url,
              links
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

        thread = threading.Thread(target=run_task)
        thread.start()

    def killTask(self, driver, taskId):
      shutdown(self, driver)

    def frameworkMessage(self, driver, message):
      print "Ignoring framework message: %s" % message

    def shutdown(self, driver):
      print "Shutting down"
      sys.exit(0)

    def error(self, error, message):
      pass

if __name__ == "__main__":
    print "Starting Launching Executor (LE)"
    driver = mesos.MesosExecutorDriver(CrawlExecutor())
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)
