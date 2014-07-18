package main

import (
	"code.google.com/p/goprotobuf/proto"
	"container/list"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/mesosphere/mesos-go/mesos"
	"log"
	"os"
	"os/signal"
	"path/filepath"
)

const TASK_CPUS = 0.1
const TASK_MEM = 32.0

type Edge struct {
	From string
	To   string
}

func (e Edge) String() string {
	return fmt.Sprintf("(%s, %s)", e.From, e.To)
}

func main() {
	crawlQueue := list.New()  // list of string
	renderQueue := list.New() // list of string

	processedURLs := list.New() // list of string
	crawlResults := list.New()  // list of CrawlEdge
	renderResults := make(map[string]string)

	seedUrl := flag.String("seed", "http://mesosphere.io", "The first URL to crawl")
	crawlQueue.PushBack(*seedUrl)

	tasksCreated := 0

	master := flag.String("master", "localhost:5050", "Location of leading Mesos master")
	localMode := flag.Bool("local", false, "If true, saves rendered web pages on local disk")
	// TODO(nnielsen): Add flag for artifacts.

	flag.Parse()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	go func(c chan os.Signal) {
		s := <-c
		fmt.Println("Got signal:", s)

		if s == os.Interrupt {
			writeDOTFile(crawlResults, renderResults)
		}
		os.Exit(1)
	}(c)

	crawlCommand := "python crawl_executor.py"
	renderCommand := "python render_executor.py"

	if *localMode {
		renderCommand += " --local"
		crawlCommand += " --local"
	}

	// TODO(nnielsen): In local mode, verify artifact locations.
	rendlerArtifacts := executorURIs()

	crawlExecutor := &mesos.ExecutorInfo{
		ExecutorId: &mesos.ExecutorID{Value: proto.String("crawl-executor")},
		Command: &mesos.CommandInfo{
			Value: proto.String(crawlCommand),
			Uris:  rendlerArtifacts,
		},
		Name:   proto.String("Crawler"),
		Source: proto.String("rendering-crawler"),
	}

	renderExecutor := &mesos.ExecutorInfo{
		ExecutorId: &mesos.ExecutorID{Value: proto.String("render-executor")},
		Command: &mesos.CommandInfo{
			Value: proto.String(renderCommand),
			Uris:  rendlerArtifacts,
		},
		Name:   proto.String("Renderer"),
		Source: proto.String("rendering-crawler"),
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
		cpus := 1.0
		mem := 64.0

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
				log.Printf("Received task status")

				if *status.State == mesos.TaskState_TASK_FINISHED {
					// TODO(nnielsen): Keep track of renderingers in flight and do
					// graceful shutdown of framework (allow ongoing render tasks to
					// finish).
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
					var result CrawlResult
					err := json.Unmarshal([]byte(message), &result)
					if err != nil {
						log.Printf("Error deserializing CrawlResult: [%s]", err)
					} else {
						for _, link := range result.Links {
							edge := Edge{From: result.URL, To: link}
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
					var result RenderResult
					err := json.Unmarshal([]byte(message), &result)
					if err != nil {
						log.Printf("Error deserializing RenderResult: [%s]", err)
					} else {
						log.Printf(
							"Appending [%s] to render results",
							Edge{From: result.URL, To: result.ImageURL})
						renderResults[result.URL] = result.ImageURL
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
	baseURI := fmt.Sprintf("file://%s/", basePath)

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
