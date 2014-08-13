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

#include <libgen.h>
#include <stdlib.h>
#include <limits.h>
#include <signal.h>
#include <unistd.h>
#include <iostream>
#include <functional>
#include <string>
#include <queue>
#include <vector>
#include <map>
#include <mesos/resources.hpp>
#include <mesos/scheduler.hpp>
#include "rendler_helper.hpp"

using namespace mesos;

using std::cout;
using std::endl;
using std::string;
using std::vector;
using std::queue;
using std::map;

using mesos::Resources;

const float CPUS_PER_TASK = 0.2;
const int32_t MEM_PER_TASK = 32;

static queue<string> crawlQueue;
static queue<string> renderQueue;
static map<string, vector<string> > crawlResults;
static map<string, string> renderResults;
static map<string, size_t> processed;
static size_t nextUrlId = 0;

MesosSchedulerDriver* schedulerDriver;

static void shutdown();
static void SIGINTHandler();

class Rendler : public Scheduler
{
public:
  Rendler(const ExecutorInfo& _crawler,
          const ExecutorInfo& _renderer,
          const string& _seedUrl)
    : crawler(_crawler),
      renderer(_renderer),
      seedUrl(_seedUrl),
      tasksLaunched(0),
      tasksFinished(0),
      frameworkMessagesReceived(0)
  {
    crawlQueue.push(seedUrl);
    renderQueue.push(seedUrl);
    processed[seedUrl] = nextUrlId++;
    size_t lsp = seedUrl.find_last_of('/'); // skip the http:// part
    baseUrl = seedUrl.substr(0, lsp); // No trailing slash
  }

  virtual ~Rendler() {}

  virtual void registered(SchedulerDriver*,
                          const FrameworkID&,
                          const MasterInfo&)
  {
    cout << "Registered!" << endl;
  }

  virtual void reregistered(SchedulerDriver*, const MasterInfo& masterInfo) {}

  virtual void disconnected(SchedulerDriver* driver) {}

  virtual void resourceOffers(SchedulerDriver* driver,
                              const vector<Offer>& offers)
  {
    for (size_t i = 0; i < offers.size(); i++) {
      const Offer& offer = offers[i];
      Resources remaining = offer.resources();

      static Resources TASK_RESOURCES = Resources::parse(
          "cpus:" + stringify<float>(CPUS_PER_TASK) +
          ";mem:" + stringify<size_t>(MEM_PER_TASK)).get();

      size_t maxTasks = 0;
      while (TASK_RESOURCES <= remaining) {
        maxTasks++;
        remaining -= TASK_RESOURCES;
      }

      // Launch tasks.
      vector<TaskInfo> tasks;
      for (size_t i = 0; i < maxTasks / 2 && crawlQueue.size() > 0; i++) {
        string url = crawlQueue.front();
        crawlQueue.pop();
        string urlId = "C" + stringify<size_t>(processed[url]);
        TaskInfo task;
        task.set_name("Crawler " + urlId);
        task.mutable_task_id()->set_value(urlId);
        task.mutable_slave_id()->MergeFrom(offer.slave_id());
        task.mutable_executor()->MergeFrom(crawler);
        task.mutable_resources()->MergeFrom(TASK_RESOURCES);
        task.set_data(url);
        tasks.push_back(task);
        tasksLaunched++;
        cout << "Crawler " << urlId << " " << url << endl;
      }
      for (size_t i = maxTasks/2; i < maxTasks && renderQueue.size() > 0; i++) {
        string url = renderQueue.front();
        renderQueue.pop();
        string urlId = "R" + stringify<size_t>(processed[url]);
        TaskInfo task;
        task.set_name("Renderer " + urlId);
        task.mutable_task_id()->set_value(urlId);
        task.mutable_slave_id()->MergeFrom(offer.slave_id());
        task.mutable_executor()->MergeFrom(renderer);
        task.mutable_resources()->MergeFrom(TASK_RESOURCES);
        task.set_data(url);
        tasks.push_back(task);
        tasksLaunched++;
        cout << "Renderer " << urlId << " " << url << endl;
      }

      driver->launchTasks(offer.id(), tasks);
    }
  }

  virtual void offerRescinded(SchedulerDriver* driver,
                              const OfferID& offerId) {}

  virtual void statusUpdate(SchedulerDriver* driver, const TaskStatus& status)
  {
    if (status.state() == TASK_FINISHED) {
      cout << "Task " << status.task_id().value() << " finished" << endl;
      tasksFinished++;
    }

    if (tasksFinished == tasksLaunched &&
        crawlQueue.empty() &&
        renderQueue.empty()) {
      // Wait to receive any pending framework messages
      // If some framework messages are lost, it may hang indefinitely.
      while (frameworkMessagesReceived != tasksFinished) {
        sleep(1);
      }
      shutdown();
      driver->stop();
    }
  }

  virtual void frameworkMessage(SchedulerDriver* driver,
                                const ExecutorID& executorId,
                                const SlaveID& slaveId,
                                const string& data)
  {
    vector<string> strVector = stringToVector(data);
    string taskId = strVector[0];
    string url = strVector[1];

    if (executorId.value() == crawler.executor_id().value()) {
      cout << "Crawler msg received: " << taskId << endl;

      for (size_t i = 2; i < strVector.size(); i++) {
        string& newURL = strVector[i];
        crawlResults[url].push_back(newURL);
        if (processed.find(newURL) == processed.end()) {
          processed[newURL] = nextUrlId++;
          if (newURL.substr(0, baseUrl.length()) == baseUrl) {
            crawlQueue.push(newURL);
          }
          renderQueue.push(newURL);
        }
      }
    } else {
      if (access(strVector[2].c_str(), R_OK) == 0) {
        renderResults[url] = strVector[2];
      }
    }
    frameworkMessagesReceived++;
  }

  virtual void slaveLost(SchedulerDriver* driver, const SlaveID& sid) {}

  virtual void executorLost(SchedulerDriver* driver,
                            const ExecutorID& executorID,
                            const SlaveID& slaveID,
                            int status) {}

  virtual void error(SchedulerDriver* driver, const string& message)
  {
    cout << message << endl;
  }

private:
  const ExecutorInfo crawler;
  const ExecutorInfo renderer;
  string seedUrl;
  string baseUrl;
  size_t tasksLaunched;
  size_t tasksFinished;
  size_t frameworkMessagesReceived;
};

static void shutdown()
{
  printf("Rendler is shutting down\n");
  printf("Writing results to result.dot\n");

  FILE *f = fopen("result.dot", "w");
  fprintf(f, "digraph G {\n");
  fprintf(f, "  node [shape=box];\n");

  // Add vertices.
  map<string, string>::iterator rit;
  for (rit = renderResults.begin(); rit != renderResults.end(); rit++) {
    // Prepend character as dot vertices cannot starting with a digit.
    string url_hash = "R" + stringify<size_t>(processed[rit->first]);
    string& filename = rit->second;
    fprintf(f,
            "  %s[label=\"\" image=\"%s\"];\n",
            url_hash.c_str(),
            filename.c_str());
  }

  // Add edges.
  map<string, vector<string> >::iterator cit;
  for (cit = crawlResults.begin(); cit != crawlResults.end(); cit++) {
    if (renderResults.find(cit->first) == renderResults.end()) {
      continue;
    }
    string from_hash = "R" + stringify<size_t>(processed[cit->first]);
    vector<string>& adjList = cit->second;

    for (size_t i = 0; i < adjList.size(); i++) {
      string to_hash = "R" + stringify<size_t>(processed[adjList[i]]);
      if (renderResults.find(adjList[i]) != renderResults.end()) {
        // DOT format is:
        // A -> B;
        fprintf(f, "  %s -> %s;\n", from_hash.c_str(), to_hash.c_str());
      }
    }
  }

  fprintf(f, "}\n");
  fclose(f);
}

static void SIGINTHandler(int signum)
{
  if (schedulerDriver != NULL) {
    shutdown();
    schedulerDriver->stop();
  }
  delete schedulerDriver;
  exit(0);
}

#define shift argc--,argv++
int main(int argc, char** argv)
{
  string seedUrl, master;
  shift;
  while (true) {
    string s = argc>0 ? argv[0] : "--help";
    if (argc > 1 && s == "--seedUrl") {
      seedUrl = argv[1];
      shift; shift;
    } else if (argc > 1 && s == "--master") {
      master = argv[1];
      shift; shift;
    } else {
      break;
    }
  }

  if (master.length() == 0 || seedUrl.length() == 0) {
    printf("Usage: rendler --seedUrl <URL> --master <ip>:<port>\n");
    exit(1);
  }

  // Find this executable's directory to locate executor.
  string path = realpath(dirname(argv[0]), NULL);
  string crawlerURI = path + "/crawl_executor";
  string rendererURI = path + "/render_executor";
  cout << crawlerURI << endl;
  cout << rendererURI << endl;

  ExecutorInfo crawler;
  crawler.mutable_executor_id()->set_value("Crawler");
  crawler.mutable_command()->set_value(crawlerURI);
  crawler.set_name("Crawl Executor (C++)");
  crawler.set_source("cpp");

  ExecutorInfo renderer;
  renderer.mutable_executor_id()->set_value("Renderer");
  renderer.mutable_command()->set_value(rendererURI);
  renderer.set_name("Render Executor (C++)");
  renderer.set_source("cpp");

  Rendler scheduler(crawler, renderer, seedUrl);

  FrameworkInfo framework;
  framework.set_user(""); // Have Mesos fill in the current user.
  framework.set_name("Rendler Framework (C++)");
  //framework.set_role(role);
  framework.set_principal("rendler-cpp");

  // Set up the signal handler for SIGINT for clean shutdown.
  struct sigaction action;
  action.sa_handler = SIGINTHandler;
  sigemptyset(&action.sa_mask);
  action.sa_flags = 0;
  sigaction(SIGINT, &action, NULL);

  schedulerDriver = new MesosSchedulerDriver(&scheduler, framework, master);

  int status = schedulerDriver->run() == DRIVER_STOPPED ? 0 : 1;

  // Ensure that the driver process terminates.
  schedulerDriver->stop();

  shutdown();

  delete schedulerDriver;
  return status;
}
