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
	"time"
	"path/filepath"
)

const TASK_CPUS = 0.1
const TASK_MEM = 32.0
const SHUTDOWN_TIMEOUT = 30  // in seconds

func main() {
	crawlQueue := list.New()  // list of string
	renderQueue := list.New() // list of string

	processedURLs := list.New() // list of string
	crawlResults := list.New()  // list of CrawlEdge
	renderResults := make(map[string]string)

	seedUrl := flag.String("seed", "http://mesosphere.io", "The first URL to crawl")
	master := flag.String("master", "127.0.1.1:5050", "Location of leading Mesos master")
	localMode := flag.Bool("local", true, "If true, saves rendered web pages on local disk")
	// TODO(nnielsen): Add flag for artifacts.

	flag.Parse()

	crawlQueue.PushBack(*seedUrl)

	tasksCreated := 0
	tasksRunning := 0
	shuttingDown := false

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
		task.Executor = crawlExecutor
		task.Data = []byte(url)
		return task
	}

	makeRenderTask := func(url string, offer mesos.Offer) *mesos.TaskInfo {
		task := makeTaskPrototype(offer)
		task.Name = proto.String("RENDER_" + *task.TaskId.Value)
		task.Executor = renderExecutor
		task.Data = []byte(url)
		return task
	}

	maxTasksForOffer := func(offer mesos.Offer) int {
		// TODO(nnielsen): Parse offer resources.
		count := 0

		var cpus float64 = 0
		var mem float64 = 0

		for _, resource := range offer.Resources {
			if resource.GetName() == "cpus" {
				cpus = *resource.GetScalar().Value
			}

			if resource.GetName() == "mem" {
				mem = *resource.GetScalar().Value
			}
		}

		for cpus >= TASK_CPUS && mem >= TASK_MEM {
			count++
			cpus -= TASK_CPUS
			mem -= TASK_MEM
		}

		return count
	}

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

				for _, offer := range offers {
					if shuttingDown {
						fmt.Println("Shutting down: declining offer on [", offer.Hostname, "]")
						driver.DeclineOffer(offer.Id)
						continue
					}

					tasks := []mesos.TaskInfo{}

					for i := 0; i < maxTasksForOffer(offer)/2; i++ {
						if crawlQueue.Front() != nil {
							url := crawlQueue.Front().Value.(string)
							crawlQueue.Remove(crawlQueue.Front())
							task := makeCrawlTask(url, offer)
							tasks = append(tasks, *task)
						}
						if renderQueue.Front() != nil {
							url := renderQueue.Front().Value.(string)
							renderQueue.Remove(renderQueue.Front())
							task := makeRenderTask(url, offer)
							tasks = append(tasks, *task)
						}
					}

					if len(tasks) == 0 {
						driver.DeclineOffer(offer.Id)
					} else {
						driver.LaunchTasks(offer.Id, tasks)
					}
				}
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
						for _, link := range result.Links {
							edge := rendler.Edge{From: result.URL, To: link}
							log.Printf("Appending [%s] to crawl results", edge)
							crawlResults.PushBack(edge)

							alreadyProcessed := false
							for e := processedURLs.Front(); e != nil && !alreadyProcessed; e = e.Next() {
								processedURL := e.Value.(string)
								if link == processedURL {
									alreadyProcessed = true
								}
							}

							if !alreadyProcessed {
								log.Printf("Enqueueing [%s]", link)
								crawlQueue.PushBack(link)
								renderQueue.PushBack(link)
								processedURLs.PushBack(link)
							}
						}
					}

				case *renderExecutor.ExecutorId.Value:
					log.Printf("Received framework message from renderer")
					var result rendler.RenderResult
					err := json.Unmarshal([]byte(message), &result)
					if err != nil {
						log.Printf("Error deserializing RenderResult: [%s]", err)
					} else {
						log.Printf(
							"Appending [%s] to render results",
							rendler.Edge{From: result.URL, To: result.ImageURL})
						renderResults[result.URL] = result.ImageURL
					}

				default:
					log.Printf("Received a framework message from some unknown source: %s", *executorId.Value)
				}
			},
		},
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	go func(c chan os.Signal) {
		s := <-c
		fmt.Println("Got signal:", s)

		if s == os.Interrupt {
			fmt.Println("RENDLER is shutting down")
			shuttingDown = true
			wait_started := time.Now()
			for tasksRunning > 0 && SHUTDOWN_TIMEOUT > int(time.Since(wait_started).Seconds()) {
				time.Sleep(time.Second)
			}

			if tasksRunning > 0 {
				fmt.Println("Shutdown by timeout,", tasksRunning, "task(s) have not completed")
			}

			driver.Stop(false)
		}
	}(c)

	driver.Init()
	defer driver.Destroy()

	driver.Start()
	driver.Join()
	driver.Stop(false)
	rendler.WriteDOTFile(crawlResults, renderResults)
	os.Exit(0)
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
		pathToURI(baseURI+"render.js", false),
		pathToURI(baseURI+"python/crawl_executor.py", false),
		pathToURI(baseURI+"python/render_executor.py", false),
		pathToURI(baseURI+"python/results.py", false),
		pathToURI(baseURI+"python/task_state.py", false),
	}
}
