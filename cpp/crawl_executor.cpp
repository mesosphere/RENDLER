/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <iostream>
#include <string>
#include <vector>
#include <assert.h>
#include <curl/curl.h>
#include <boost/regex.hpp>

#include <stout/lambda.hpp>
#include <mesos/executor.hpp>
#include "rendler_helper.hpp"

using namespace mesos;

using std::cout;
using std::endl;
using std::string;

static int writer(char *data, size_t size, size_t nmemb, string *writerData)
{
  assert(writerData != NULL);
  writerData->append(data, size*nmemb);
  return size * nmemb;
}

static void runTask(ExecutorDriver* driver, const TaskInfo& task)
{
  string url = task.data();
  cout << "Running crawl task " << task.task_id().value()
       << " Fetch: " << url;

  string buffer;
  vector<string> result;
  result.push_back(task.task_id().value());
  result.push_back(url);

  CURL *conn;
  conn = curl_easy_init();
  assert(conn != NULL);
  assert(curl_easy_setopt(conn, CURLOPT_URL, url.c_str()) == CURLE_OK);
  assert(curl_easy_setopt(conn, CURLOPT_FOLLOWLOCATION, 1L) == CURLE_OK);
  assert(curl_easy_setopt(conn, CURLOPT_WRITEFUNCTION, writer) == CURLE_OK);
  assert(curl_easy_setopt(conn, CURLOPT_WRITEDATA, &buffer) == CURLE_OK);
  assert(curl_easy_perform(conn) == CURLE_OK);
  char *tmp;
  assert(curl_easy_getinfo(conn, CURLINFO_EFFECTIVE_URL, &tmp) == CURLE_OK);
  string redirectURL = url;
  if (tmp != NULL) {
    redirectURL = tmp;
  }
  curl_easy_cleanup(conn);

  size_t sp = redirectURL.find_first_of('/', 7); // skip the http:// part
  size_t lsp = redirectURL.find_last_of('/'); // skip the http:// part
  string baseUrl = redirectURL.substr(0, sp); // No trailing slash
  string dirUrl = redirectURL.substr(0, lsp); // No trailing slash

  cout << "redirectURL " << redirectURL << " baseURL: " << baseUrl << endl;
  cout << "dirUrl " << dirUrl  << endl;

  const boost::regex hrefRE("<a\\s+[^\\>]*?href\\s*=\\s*([\"'])(.*?)\\1");
  const boost::regex urlRE("^([a-zA-Z]+://).*");

  boost::smatch matchHref;
  string::const_iterator f = buffer.begin();
  string::const_iterator l = buffer.end();

  while (f != buffer.end() &&
         boost::regex_search(f, l, matchHref, hrefRE)) {
    string link = matchHref[2];
    f = matchHref[0].second;

    boost::smatch matchService;
    string::const_iterator lb = link.begin();
    string::const_iterator le = link.end();

    // Remove the anchor
    if (link.find_first_of('#') != string::npos) {
      link.erase(link.find_first_of('#'));
    }
    if (link.empty()) {
      continue;
    }
    if (link[0] == '/') {
      link = baseUrl + link;
    } else if (!boost::regex_search(lb, le, matchService, urlRE)) {
      // Relative URL
      link = dirUrl + "/" + link;
    }
    result.push_back(link);
  };

  driver->sendFrameworkMessage(vectorToString(result));

  TaskStatus status;
  status.mutable_task_id()->MergeFrom(task.task_id());
  status.set_state(TASK_FINISHED);
  driver->sendStatusUpdate(status);
}


void* start(void* arg)
{
  lambda::function<void(void)>* thunk = (lambda::function<void(void)>*) arg;
  (*thunk)();
  delete thunk;
  return NULL;
}

class CrawlExecutor : public Executor
{
  public:
    virtual ~CrawlExecutor() {}

    virtual void registered(ExecutorDriver* driver,
                            const ExecutorInfo& executorInfo,
                            const FrameworkInfo& frameworkInfo,
                            const SlaveInfo& slaveInfo)
    {
      cout << "Registered CrawlExecutor on " << slaveInfo.hostname() << endl;
    }

    virtual void reregistered(ExecutorDriver* driver,
                              const SlaveInfo& slaveInfo)
    {
      cout << "Re-registered CrawlExecutor on " << slaveInfo.hostname() << endl;
    }

    virtual void disconnected(ExecutorDriver* driver) {}


    virtual void launchTask(ExecutorDriver* driver, const TaskInfo& task)
    {
      cout << "Starting task " << task.task_id().value() << endl;

      lambda::function<void(void)>* thunk =
        new lambda::function<void(void)>(lambda::bind(&runTask, driver, task));

      pthread_t pthread;
      if (pthread_create(&pthread, NULL, &start, thunk) != 0) {
        TaskStatus status;
        status.mutable_task_id()->MergeFrom(task.task_id());
        status.set_state(TASK_FAILED);

        driver->sendStatusUpdate(status);
      } else {
        pthread_detach(pthread);

        TaskStatus status;
        status.mutable_task_id()->MergeFrom(task.task_id());
        status.set_state(TASK_RUNNING);

        driver->sendStatusUpdate(status);
      }
    }

  virtual void killTask(ExecutorDriver* driver, const TaskID& taskId) {}
  virtual void frameworkMessage(ExecutorDriver* driver, const string& data) {}
  virtual void shutdown(ExecutorDriver* driver) {}
  virtual void error(ExecutorDriver* driver, const string& message) {}
};


int main(int argc, char** argv)
{
  CrawlExecutor executor;
  MesosExecutorDriver driver(&executor);
  return driver.run() == DRIVER_STOPPED ? 0 : 1;
}
