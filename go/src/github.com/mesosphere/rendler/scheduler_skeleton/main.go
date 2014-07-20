package main

import (
	"code.google.com/p/goprotobuf/proto"
	"container/list"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/mesosphere/mesos-go/mesos"
	"github.com/mesosphere/rendler"
	"log"
	"os"
	"os/signal"
	"path/filepath"
)

const TASK_CPUS = 0.1
const TASK_MEM = 32.0

// See the Mesos Framework Development Guide:
// http://mesos.apache.org/documentation/latest/app-framework-development-guide
//
// Scheduler, scheduler driver, executor, and executor driver definitions:
// https://github.com/apache/mesos/blob/master/src/python/src/mesos.py
// https://github.com/apache/mesos/blob/master/include/mesos/scheduler.hpp
//
// Mesos protocol buffer definitions for Python:
// https://github.com/mesosphere/deimos/blob/master/deimos/mesos_pb2.py
// https://github.com/apache/mesos/blob/master/include/mesos/mesos.proto
//
// NOTE: Feel free to strip out "_ = variable" stubs. They are in place to
// silence the Go compiler.
func main() {
	crawlQueue := list.New()  // list of string
	renderQueue := list.New() // list of string
	_ = renderQueue

	processedURLs := list.New() // list of string
	_ = processedURLs

	crawlResults := list.New() // list of CrawlEdge
	renderResults := make(map[string]string)

	seedUrl := flag.String("seed", "http://mesosphere.io", "The first URL to crawl")
	master := flag.String("master", "127.0.1.1:5050", "Location of leading Mesos master")
	localMode := flag.Bool("local", true, "If true, saves rendered web pages on local disk")
	// TODO(nnielsen): Add flag for artifacts.

	flag.Parse()

	crawlQueue.PushBack(*seedUrl)

	tasksCreated := 0
	tasksRunning := 0

	// TODO(nnielsen): based on `tasksRunning`, do
	// graceful shutdown of framework (allow ongoing render tasks to
	// finish).
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	go func(c chan os.Signal) {
		s := <-c
		fmt.Println("Got signal:", s)

		if s == os.Interrupt {
			rendler.WriteDOTFile(crawlResults, renderResults)
		}
		os.Exit(1)
	}(c)

	crawlCommand := "python crawl_executor.py"
	renderCommand := "python render_executor.py"

	if *localMode {
		renderCommand += " --local"
	}

	// TODO(nnielsen): In local mode, verify artifact locations.
	rendlerArtifacts := executorURIs()

	crawlExecutor := &mesos.ExecutorInfo{
		ExecutorId: &mesos.ExecutorID{Value: proto.String("crawl-executor")},
		Command: &mesos.CommandInfo{
			Value: proto.String(crawlCommand),
			Uris:  rendlerArtifacts,
		},
		Name: proto.String("Crawler"),
	}

	renderExecutor := &mesos.ExecutorInfo{
		ExecutorId: &mesos.ExecutorID{Value: proto.String("render-executor")},
		Command: &mesos.CommandInfo{
			Value: proto.String(renderCommand),
			Uris:  rendlerArtifacts,
		},
		Name: proto.String("Renderer"),
	}

	makeTaskPrototype := func(offer mesos.Offer) *mesos.TaskInfo {
		taskId := tasksCreated
		tasksCreated++
		return &mesos.TaskInfo{
			TaskId: &mesos.TaskID{
				Value: proto.String(fmt.Sprintf("RENDLER-%d", taskId)),
			},
			SlaveId: offer.SlaveId,
			Resources: []*mesos.Resource{
				mesos.ScalarResource("cpus", TASK_CPUS),
				mesos.ScalarResource("mem", TASK_MEM),
			},
		}
	}

	makeCrawlTask := func(url string, offer mesos.Offer) *mesos.TaskInfo {
		task := makeTaskPrototype(offer)
		task.Name = proto.String("CRAWL_" + *task.TaskId.Value)
		//
		// TODO
		//
		return task
	}
	_ = makeCrawlTask

	makeRenderTask := func(url string, offer mesos.Offer) *mesos.TaskInfo {
		task := makeTaskPrototype(offer)
		task.Name = proto.String("RENDER_" + *task.TaskId.Value)
		//
		// TODO
		//
		return task
	}
	_ = makeRenderTask

	maxTasksForOffer := func(offer mesos.Offer) int {
		// TODO(nnielsen): Parse offer resources.
		count := 0

		var cpus float64 = 0
		_ = cpus

		var mem float64 = 0
		_ = mem

		for _, resource := range offer.Resources {
			if resource.GetName() == "cpus" {
				cpus = *resource.GetScalar().Value
			}

			if resource.GetName() == "mem" {
				mem = *resource.GetScalar().Value
			}
		}

		//
		// TODO
		//

		return count
	}
	_ = maxTasksForOffer

	printQueueStatistics := func() {
		// TODO(nnielsen): Print queue lengths.
	}

	driver := mesos.SchedulerDriver{
		Master: *master,
		Framework: mesos.FrameworkInfo{
			Name: proto.String("RENDLER"),
			User: proto.String(""),
		},

		Scheduler: &mesos.Scheduler{

			Registered: func(
				driver *mesos.SchedulerDriver,
				frameworkId mesos.FrameworkID,
				masterInfo mesos.MasterInfo) {
				log.Printf("Registered")
			},

			ResourceOffers: func(driver *mesos.SchedulerDriver, offers []mesos.Offer) {
				printQueueStatistics()

				//
				// TODO
				//
			},

			StatusUpdate: func(driver *mesos.SchedulerDriver, status mesos.TaskStatus) {
				log.Printf("Received task status [%s] for task [%s]", rendler.NameFor(status.State), *status.TaskId.Value)

				if *status.State == mesos.TaskState_TASK_RUNNING {
					tasksRunning++
				} else if rendler.IsTerminal(status.State) {
					tasksRunning--
				}
			},

			FrameworkMessage: func(
				driver *mesos.SchedulerDriver,
				executorId mesos.ExecutorID,
				slaveId mesos.SlaveID,
				message string) {

				switch *executorId.Value {
				case *crawlExecutor.ExecutorId.Value:
					log.Print("Received framework message from crawler")
					var result rendler.CrawlResult
					err := json.Unmarshal([]byte(message), &result)
					if err != nil {
						log.Printf("Error deserializing CrawlResult: [%s]", err)
					} else {
						//
						// TODO
						//
					}

				case *renderExecutor.ExecutorId.Value:
					log.Printf("Received framework message from renderer")
					var result rendler.RenderResult
					err := json.Unmarshal([]byte(message), &result)
					if err != nil {
						log.Printf("Error deserializing RenderResult: [%s]", err)
					} else {
						//
						// TODO
						//
					}

				default:
					log.Printf("Received a framework message from some unknown source: %s", *executorId.Value)
				}
			},
		},
	}

	driver.Init()
	defer driver.Destroy()

	driver.Start()
	driver.Join()
	driver.Stop(false)
}

func executorURIs() []*mesos.CommandInfo_URI {
	basePath, err := filepath.Abs(filepath.Dir(os.Args[0]) + "/../..")
	if err != nil {
		log.Fatal("Failed to find the path to RENDLER")
	}
	baseURI := fmt.Sprintf("%s/", basePath)

	pathToURI := func(path string, extract bool) *mesos.CommandInfo_URI {
		return &mesos.CommandInfo_URI{
			Value:   &path,
			Extract: &extract,
		}
	}

	return []*mesos.CommandInfo_URI{
		pathToURI(baseURI+"crawl_executor.py", false),
		pathToURI(baseURI+"render.js", false),
		pathToURI(baseURI+"render_executor.py", false),
		pathToURI(baseURI+"results.py", false),
		pathToURI(baseURI+"task_state.py", false),
	}
}
