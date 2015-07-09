package main

import (
	"container/list"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/golang/protobuf/proto"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"
	"github.com/mesosphere/rendler"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"time"
)

const (
	TaskCPUs        = 0.1
	TaskMem         = 32.0
	ShutdownTimeout = time.Duration(30) * time.Second
)

const (
	crawlCommand  = "python crawl_executor.py"
	renderCommand = "python render_executor.py"
)

var (
	DefaultFilter = &mesos.Filters{RefuseSeconds: proto.Float64(1)}
)

// maxTasksForOffer computes how many tasks can be launched using a given offer
func maxTasksForOffer(offer *mesos.Offer) int {
	count := 0

	var cpus, mem float64

	for _, resource := range offer.Resources {
		switch resource.GetName() {
		case "cpus":
			cpus = *resource.GetScalar().Value
		case "mem":
			mem = *resource.GetScalar().Value
		}
	}

	for cpus >= TaskCPUs && mem >= TaskMem {
		count++
		cpus -= TaskCPUs
		mem -= TaskMem
	}

	return count
}

// RendlerScheduler implements the Scheduler interface and stores
// the state needed for Rendler to function.
type RendlerScheduler struct {
	tasksCreated int
	tasksRunning int

	crawlQueue    *list.List
	renderQueue   *list.List
	processedURLs map[string]struct{}
	crawlResults  []*rendler.Edge
	renderResults map[string]string

	crawlExecutor  *mesos.ExecutorInfo
	renderExecutor *mesos.ExecutorInfo

	// This channel is closed when the program receives an interrupt,
	// signalling that the program should shut down.
	shutdown chan struct{}
	// This channel is closed after shutdown is closed, and only when all
	// outstanding tasks have been cleaned up
	done chan struct{}
}

// NewRendlerScheduler creates a new scheduler for Rendler.
func NewRendlerScheduler(seedUrl string, local bool) *RendlerScheduler {
	rendlerArtifacts := executorURIs()

	var actualRenderCommand string = renderCommand
	if local {
		actualRenderCommand += "--local"
	}
	s := &RendlerScheduler{
		crawlQueue:    list.New(),
		renderQueue:   list.New(),
		processedURLs: make(map[string]struct{}),
		crawlResults:  make([]*rendler.Edge, 0),
		renderResults: make(map[string]string),

		crawlExecutor: &mesos.ExecutorInfo{
			ExecutorId: &mesos.ExecutorID{Value: proto.String("crawl-executor")},
			Command: &mesos.CommandInfo{
				Value: proto.String(crawlCommand),
				Uris:  rendlerArtifacts,
			},
			Name: proto.String("Crawler"),
		},

		renderExecutor: &mesos.ExecutorInfo{
			ExecutorId: &mesos.ExecutorID{Value: proto.String("render-executor")},
			Command: &mesos.CommandInfo{
				Value: proto.String(actualRenderCommand),
				Uris:  rendlerArtifacts,
			},
			Name: proto.String("Renderer"),
		},

		shutdown: make(chan struct{}),
		done:     make(chan struct{}),
	}
	s.crawlQueue.PushBack(seedUrl)
	s.processedURLs[seedUrl] = struct{}{}
	return s
}

func (s *RendlerScheduler) newTaskPrototype(offer *mesos.Offer) *mesos.TaskInfo {
	taskID := s.tasksCreated
	s.tasksCreated++
	return &mesos.TaskInfo{
		TaskId: &mesos.TaskID{
			Value: proto.String(fmt.Sprintf("RENDLER-%d", taskID)),
		},
		SlaveId: offer.SlaveId,
		Resources: []*mesos.Resource{
			mesosutil.NewScalarResource("cpus", TaskCPUs),
			mesosutil.NewScalarResource("mem", TaskMem),
		},
	}
}

func (s *RendlerScheduler) newCrawlTask(url string, offer *mesos.Offer) *mesos.TaskInfo {
	task := s.newTaskPrototype(offer)
	task.Name = proto.String("CRAWL_" + *task.TaskId.Value)
	task.Executor = s.crawlExecutor
	task.Data = []byte(url)
	return task
}

func (s *RendlerScheduler) newRenderTask(url string, offer *mesos.Offer) *mesos.TaskInfo {
	task := s.newTaskPrototype(offer)
	task.Name = proto.String("RENDER_" + *task.TaskId.Value)
	task.Executor = s.renderExecutor
	task.Data = []byte(url)
	return task
}

func (s *RendlerScheduler) Registered(
	driver sched.SchedulerDriver,
	frameworkId *mesos.FrameworkID,
	masterInfo *mesos.MasterInfo) {
	log.Printf("Framework %s registered with master %s", frameworkId, masterInfo)
}

func (s *RendlerScheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	log.Printf("Framework re-registered with master %s", masterInfo)
}

func (s *RendlerScheduler) Disconnected(sched.SchedulerDriver) {
	log.Println("Framework disconnected with master")
}

func (s *RendlerScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
	log.Printf("Received %d resource offers", len(offers))
	for _, offer := range offers {
		select {
		case <-s.shutdown:
			log.Println("Shutting down: declining offer on [", offer.Hostname, "]")
			driver.DeclineOffer(offer.Id, DefaultFilter)
			continue
		default:
		}

		tasks := []*mesos.TaskInfo{}
		// += 2 because we create 2 tasks for each iteration
		tasksToLaunch := maxTasksForOffer(offer)
		for i := 0; i < tasksToLaunch; i += 2 {
			if s.crawlQueue.Front() != nil {
				url := s.crawlQueue.Front().Value.(string)
				s.crawlQueue.Remove(s.crawlQueue.Front())
				task := s.newCrawlTask(url, offer)
				tasks = append(tasks, task)
			}
			if s.renderQueue.Front() != nil {
				url := s.renderQueue.Front().Value.(string)
				s.renderQueue.Remove(s.renderQueue.Front())
				task := s.newRenderTask(url, offer)
				tasks = append(tasks, task)
			}
		}

		if len(tasks) == 0 {
			driver.DeclineOffer(offer.Id, DefaultFilter)
		} else {
			driver.LaunchTasks([]*mesos.OfferID{offer.Id}, tasks, DefaultFilter)
		}
	}
}

func (s *RendlerScheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	log.Printf("Received task status [%s] for task [%s]", rendler.NameFor(status.State), *status.TaskId.Value)

	if *status.State == mesos.TaskState_TASK_RUNNING {
		s.tasksRunning++
	} else if rendler.IsTerminal(status.State) {
		s.tasksRunning--
		if s.tasksRunning == 0 {
			select {
			case <-s.shutdown:
				close(s.done)
			default:
			}
		}
	}
}

func (s *RendlerScheduler) FrameworkMessage(
	driver sched.SchedulerDriver,
	executorId *mesos.ExecutorID,
	slaveId *mesos.SlaveID,
	message string) {

	log.Println("Getting a framework message")
	switch *executorId.Value {
	case *s.crawlExecutor.ExecutorId.Value:
		log.Print("Received framework message from crawler")
		var result rendler.CrawlResult
		err := json.Unmarshal([]byte(message), &result)
		if err != nil {
			log.Printf("Error deserializing CrawlResult: [%s]", err)
			return
		}
		for _, link := range result.Links {
			edge := &rendler.Edge{From: result.URL, To: link}
			log.Printf("Appending [%s] to crawl results", edge)
			s.crawlResults = append(s.crawlResults, edge)

			if _, ok := s.processedURLs[link]; !ok {
				log.Printf("Enqueueing [%s]", link)
				s.crawlQueue.PushBack(link)
				s.renderQueue.PushBack(link)
				s.processedURLs[link] = struct{}{}
			}
		}

	case *s.renderExecutor.ExecutorId.Value:
		log.Printf("Received framework message from renderer")
		var result rendler.RenderResult
		err := json.Unmarshal([]byte(message), &result)
		if err != nil {
			log.Printf("Error deserializing RenderResult: [%s]", err)
			return
		}
		log.Printf(
			"Appending [%s] to render results",
			rendler.Edge{From: result.URL, To: result.ImageURL})
		s.renderResults[result.URL] = result.ImageURL

	default:
		log.Printf("Received a framework message from some unknown source: %s", *executorId.Value)
	}
}

func (s *RendlerScheduler) OfferRescinded(driver sched.SchedulerDriver, offerID *mesos.OfferID) {
	log.Printf("Offer %s rescinded", offerID)
}
func (s *RendlerScheduler) SlaveLost(driver sched.SchedulerDriver, slaveID *mesos.SlaveID) {
	log.Printf("Slave %s lost", slaveID)
}
func (s *RendlerScheduler) ExecutorLost(driver sched.SchedulerDriver, executorID *mesos.ExecutorID, slaveID *mesos.SlaveID, status int) {
	log.Printf("Executor %s on slave %s was lost", executorID, slaveID)
}

func (sched *RendlerScheduler) Error(driver sched.SchedulerDriver, err string) {
	log.Printf("Receiving an error: %s", err)
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

func main() {
	seedUrl := flag.String("seed", "http://mesosphere.com", "The first URL to crawl")
	master := flag.String("master", "127.0.1.1:5050", "Location of leading Mesos master")
	localMode := flag.Bool("local", true, "If true, saves rendered web pages on local disk")

	flag.Parse()

	scheduler := NewRendlerScheduler(*seedUrl, *localMode)
	driver, err := sched.NewMesosSchedulerDriver(sched.DriverConfig{
		Master: *master,
		Framework: &mesos.FrameworkInfo{
			Name: proto.String("RENDLER"),
			User: proto.String(""),
		},
		Scheduler: scheduler,
	})
	if err != nil {
		log.Printf("Unable to create scheduler driver: %s", err)
		return
	}

	// Catch interrupt
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, os.Kill)
		s := <-c
		log.Println("Got signal:", s)
		if s != os.Interrupt {
			return
		}

		log.Println("RENDLER is shutting down")
		close(scheduler.shutdown)

		select {
		case <-scheduler.done:
		case <-time.After(ShutdownTimeout):
		}

		if err := rendler.WriteDOTFile(scheduler.crawlResults, scheduler.renderResults); err != nil {
			log.Printf("Could not write DOT file: %s", err)
		}
		// we have shut down
		driver.Stop(false)
	}()

	if status, err := driver.Run(); err != nil {
		log.Printf("Framework stopped with status %s and error: %s\n", status.String(), err.Error())
	}
	log.Println("Exiting...")
}
