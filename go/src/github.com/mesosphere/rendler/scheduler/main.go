package main

import (
	"container/list"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/mesosphere/rendler"
	"log"
	"os"
	"os/signal"
	"time"
	"path/filepath"
)

import (
	"github.com/gogo/protobuf/proto"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"
)

const TASK_CPUS = 0.1
const TASK_MEM = 32.0
const SHUTDOWN_TIMEOUT = 30  // in seconds

// ----------------- util ---------------

func maxTasksForOffer(offer *mesos.Offer) int {
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

func printQueueStatistics() {
	// TODO(nnielsen): Print queue lengths.
}

func makeTaskPrototype(offer *mesos.Offer, sched *RendlerScheduler) *mesos.TaskInfo {
		taskId := sched.tasksLaunched
		sched.tasksLaunched++
		return &mesos.TaskInfo{
			TaskId: &mesos.TaskID{
				Value: proto.String(fmt.Sprintf("RENDLER-%d", taskId)),
			},
			SlaveId: offer.SlaveId,
			Resources: []*mesos.Resource{
				util.NewScalarResource("cpus", TASK_CPUS),
				util.NewScalarResource("mem", TASK_MEM),
			},
		}
	}

func makeCrawlTask(url string, offer *mesos.Offer, sched *RendlerScheduler) *mesos.TaskInfo {
	task := makeTaskPrototype(offer, sched)
	task.Name = proto.String("CRAWL_" + *task.TaskId.Value)
	task.Executor = sched.crawlExecutor
	task.Data = []byte(url)
	return task
}

func makeRenderTask(url string, offer *mesos.Offer, sched *RendlerScheduler) *mesos.TaskInfo {
	task := makeTaskPrototype(offer, sched)
	task.Name = proto.String("RENDER_" + *task.TaskId.Value)
	task.Executor = sched.renderExecutor
	task.Data = []byte(url)
	return task
}

//--------------- scheduler ----------------

type RendlerScheduler struct {
	crawlExecutor 	*mesos.ExecutorInfo
	renderExecutor *mesos.ExecutorInfo
	tasksLaunched 	int
	tasksFinished	int
	shuttingDown	bool
	crawlQueue		*list.List
	renderQueue		*list.List
	processedURLs	*list.List
	crawlResults	*list.List
	renderResults	map[string]string
	seedUrl			string
}

func newRendlerScheduler(crawlExecutor *mesos.ExecutorInfo, renderExecutor *mesos.ExecutorInfo, seedUrl string) *RendlerScheduler {

	crawlQueue := list.New()  // list of string
	renderQueue := list.New() // list of string

	processedURLs := list.New() // list of string
	crawlResults := list.New()  // list of CrawlEdge
	renderResults := make(map[string]string)

	crawlQueue.PushBack(seedUrl)

	return &RendlerScheduler{
		crawlExecutor: crawlExecutor,
		renderExecutor: renderExecutor,
		tasksLaunched: 0, 
		tasksFinished: 0,
		shuttingDown: false,
		crawlQueue:	crawlQueue,
		renderQueue: renderQueue,
		processedURLs: processedURLs,
		crawlResults: crawlResults,
		renderResults: renderResults,
		seedUrl: seedUrl,
	}
}

func (sched *RendlerScheduler) Registered(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	log.Printf("Registered")
}

func (sched *RendlerScheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	log.Printf("Framework Re-Registered with Master ", masterInfo)
}

func (sched *RendlerScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
	printQueueStatistics()

	for _, offer := range offers {
		if sched.shuttingDown {
			fmt.Println("Shutting down: declining offer on [", offer.Hostname, "]")
			driver.DeclineOffer(offer.Id, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
			continue
		}

		var tasks []*mesos.TaskInfo

		for i := 0; i < maxTasksForOffer(offer)/2; i++ {
			if sched.crawlQueue.Front() != nil {
				url := sched.crawlQueue.Front().Value.(string)
				sched.crawlQueue.Remove(sched.crawlQueue.Front())
				task := makeCrawlTask(url, offer, sched)
				tasks = append(tasks, task)
			}
			if sched.renderQueue.Front() != nil {
				url := sched.renderQueue.Front().Value.(string)
				sched.renderQueue.Remove(sched.renderQueue.Front())
				task := makeRenderTask(url, offer, sched)
				tasks = append(tasks, task)
			}
		}

		if len(tasks) == 0 {
			driver.DeclineOffer(offer.Id, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
		} else {
			driver.LaunchTasks([]*mesos.OfferID{offer.Id}, tasks, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
		}
	}
}

func (sched *RendlerScheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	log.Printf("Received task status [%s] for task [%s]", rendler.NameFor(status.State), *status.TaskId.Value)

	if *status.State == mesos.TaskState_TASK_RUNNING {
		sched.tasksLaunched++
	} else if rendler.IsTerminal(status.State) {
		sched.tasksLaunched--
	}
}

func (sched *RendlerScheduler) FrameworkMessage(driver sched.SchedulerDriver, executorId *mesos.ExecutorID, slaveId *mesos.SlaveID, message string) {

	switch *executorId.Value {
	case *sched.crawlExecutor.ExecutorId.Value:
		log.Print("Received framework message from crawler")
		var result rendler.CrawlResult
		err := json.Unmarshal([]byte(message), &result)
		if err != nil {
			log.Printf("Error deserializing CrawlResult: [%s]", err)
		} else {
			for _, link := range result.Links {
				edge := rendler.Edge{From: result.URL, To: link}
				log.Printf("Appending [%s] to crawl results", edge)
				sched.crawlResults.PushBack(edge)

				alreadyProcessed := false
				for e := sched.processedURLs.Front(); e != nil && !alreadyProcessed; e = e.Next() {
					processedURL := e.Value.(string)
					if link == processedURL {
						alreadyProcessed = true
					}
				}

				if !alreadyProcessed {
					log.Printf("Enqueueing [%s]", link)
					sched.crawlQueue.PushBack(link)
					sched.renderQueue.PushBack(link)
					sched.processedURLs.PushBack(link)
				}
			}
		}

	case *sched.renderExecutor.ExecutorId.Value:
		log.Printf("Received framework message from renderer")
		var result rendler.RenderResult
		err := json.Unmarshal([]byte(message), &result)
		if err != nil {
			log.Printf("Error deserializing RenderResult: [%s]", err)
		} else {
			log.Printf(
				"Appending [%s] to render results",
				rendler.Edge{From: result.URL, To: result.ImageURL})
			sched.renderResults[result.URL] = result.ImageURL
		}

	default:
		log.Printf("Received a framework message from some unknown source: %s", *executorId.Value)
	}
}

func (sched *RendlerScheduler) Error(driver sched.SchedulerDriver, err string) {
	log.Printf("Scheduler received error:", err)
}
func (sched *RendlerScheduler) Disconnected(sched.SchedulerDriver) {}
func (sched *RendlerScheduler) OfferRescinded(sched.SchedulerDriver, *mesos.OfferID) {}
func (sched *RendlerScheduler) SlaveLost(sched.SchedulerDriver, *mesos.SlaveID) {}
func (sched *RendlerScheduler) ExecutorLost(sched.SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, int) {}

func main() {

	crawlCommand := "python crawl_executor.py"
	renderCommand := "python render_executor.py"

	seedUrl := flag.String("seed", "http://mesosphere.io", "The first URL to crawl")
	master := flag.String("master", "127.0.1.1:5050", "Location of leading Mesos master")
	localMode := flag.Bool("local", true, "If true, saves rendered web pages on local disk")
	// TODO(nnielsen): Add flag for artifacts.

	flag.Parse()

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

	scheduler := newRendlerScheduler(crawlExecutor, renderExecutor, *seedUrl)

	master = master

	fwInfo := &mesos.FrameworkInfo{
		Name: proto.String("RENDLER"),
		User: proto.String(""),
	}

	driver, err := sched.NewMesosSchedulerDriver(
		scheduler,
		fwInfo,
		*master,
		(*mesos.Credential)(nil),
	)

	if err != nil {
		log.Printf("Unable to create a SchedulerDriver ", err.Error())
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	go func(c chan os.Signal) {
		s := <-c
		fmt.Println("Got signal:", s)

		if s == os.Interrupt {
			fmt.Println("RENDLER is shutting down")
			scheduler.shuttingDown = true
			wait_started := time.Now()
			for scheduler.tasksLaunched > 0 && SHUTDOWN_TIMEOUT > int(time.Since(wait_started).Seconds()) {
				time.Sleep(time.Second)
			}

			if scheduler.tasksLaunched > 0 {
				fmt.Println("Shutdown by timeout,", scheduler.tasksLaunched, "task(s) have not completed")
			}

			driver.Stop(false)
		}
	}(c)

	driver.Run()
	rendler.WriteDOTFile(scheduler.crawlResults, scheduler.renderResults)
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
