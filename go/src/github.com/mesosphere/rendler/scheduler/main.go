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
	taskCPUs        = 0.1
	taskMem         = 32.0
	shutdownTimeout = time.Duration(30) * time.Second
)

const (
	crawlCommand  = "python crawl_executor.py"
	renderCommand = "python render_executor.py"
)

var (
	defaultFilter = &mesos.Filters{RefuseSeconds: proto.Float64(1)}
)

// maxTasksForOffer computes how many tasks can be launched using a given offer
func maxTasksForOffer(offer *mesos.Offer) int {
	count := 0

	var cpus, mem float64

	for _, resource := range offer.Resources {
		switch resource.GetName() {
		case "cpus":
			cpus += *resource.GetScalar().Value
		case "mem":
			mem += *resource.GetScalar().Value
		}
	}

	for cpus >= taskCPUs && mem >= taskMem {
		count++
		cpus -= taskCPUs
		mem -= taskMem
	}

	return count
}

// rendlerScheduler implements the Scheduler interface and stores
// the state needed for Rendler to function.
type rendlerScheduler struct {
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

// newRendlerScheduler creates a new scheduler for Rendler.
func newRendlerScheduler(seedURL string, local bool) *rendlerScheduler {
	rendlerArtifacts := executorURIs()

	actualRenderCommand := renderCommand
	if local {
		actualRenderCommand += " --local"
	}
	s := &rendlerScheduler{
		crawlQueue:    list.New(),
		renderQueue:   list.New(),
		processedURLs: map[string]struct{}{},
		crawlResults:  []*rendler.Edge{},
		renderResults: map[string]string{},

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
	s.crawlQueue.PushBack(seedURL)
	s.processedURLs[seedURL] = struct{}{}
	return s
}

func (s *rendlerScheduler) newTaskPrototype(offer *mesos.Offer) *mesos.TaskInfo {
	taskID := s.tasksCreated
	s.tasksCreated++
	return &mesos.TaskInfo{
		TaskId: &mesos.TaskID{
			Value: proto.String(fmt.Sprintf("RENDLER-%d", taskID)),
		},
		SlaveId: offer.SlaveId,
		Resources: []*mesos.Resource{
			mesosutil.NewScalarResource("cpus", taskCPUs),
			mesosutil.NewScalarResource("mem", taskMem),
		},
	}
}

func (s *rendlerScheduler) newCrawlTask(url string, offer *mesos.Offer) *mesos.TaskInfo {
	task := s.newTaskPrototype(offer)
	task.Name = proto.String("CRAWL_" + *task.TaskId.Value)
	task.Executor = s.crawlExecutor
	task.Data = []byte(url)
	return task
}

func (s *rendlerScheduler) newRenderTask(url string, offer *mesos.Offer) *mesos.TaskInfo {
	task := s.newTaskPrototype(offer)
	task.Name = proto.String("RENDER_" + *task.TaskId.Value)
	task.Executor = s.renderExecutor
	task.Data = []byte(url)
	return task
}

func (s *rendlerScheduler) Registered(
	_ sched.SchedulerDriver,
	frameworkID *mesos.FrameworkID,
	masterInfo *mesos.MasterInfo) {
	log.Printf("Framework %s registered with master %s", frameworkID, masterInfo)
}

func (s *rendlerScheduler) Reregistered(_ sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	log.Printf("Framework re-registered with master %s", masterInfo)
}

func (s *rendlerScheduler) Disconnected(sched.SchedulerDriver) {
	log.Println("Framework disconnected with master")
}

func (s *rendlerScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
	log.Printf("Received %d resource offers", len(offers))
	for _, offer := range offers {
		select {
		case <-s.shutdown:
			log.Println("Shutting down: declining offer on [", offer.Hostname, "]")
			driver.DeclineOffer(offer.Id, defaultFilter)
			if s.tasksRunning == 0 {
				close(s.done)
			}
			continue
		default:
		}

		tasks := []*mesos.TaskInfo{}
		// += 2 because we create 2 tasks for each iteration
		tasksToLaunch := maxTasksForOffer(offer)
		for tasksToLaunch > 0 {
			if s.crawlQueue.Front() != nil {
				url := s.crawlQueue.Front().Value.(string)
				s.crawlQueue.Remove(s.crawlQueue.Front())
				task := s.newCrawlTask(url, offer)
				tasks = append(tasks, task)
				tasksToLaunch--
			}
			if s.renderQueue.Front() != nil {
				url := s.renderQueue.Front().Value.(string)
				s.renderQueue.Remove(s.renderQueue.Front())
				task := s.newRenderTask(url, offer)
				tasks = append(tasks, task)
				tasksToLaunch--
			}
			if s.crawlQueue.Front() == nil && s.renderQueue.Front() == nil {
				break
			}
		}

		if len(tasks) == 0 {
			driver.DeclineOffer(offer.Id, defaultFilter)
		} else {
			driver.LaunchTasks([]*mesos.OfferID{offer.Id}, tasks, defaultFilter)
		}
	}
}

func (s *rendlerScheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
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

func (s *rendlerScheduler) FrameworkMessage(
	driver sched.SchedulerDriver,
	executorID *mesos.ExecutorID,
	slaveID *mesos.SlaveID,
	message string) {

	log.Println("Getting a framework message")
	switch *executorID.Value {
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
			rendler.Edge{From: result.URL, To: result.ImageURL},
		)
		s.renderResults[result.URL] = result.ImageURL

	default:
		log.Printf("Received a framework message from some unknown source: %s", *executorID.Value)
	}
}

func (s *rendlerScheduler) OfferRescinded(_ sched.SchedulerDriver, offerID *mesos.OfferID) {
	log.Printf("Offer %s rescinded", offerID)
}
func (s *rendlerScheduler) SlaveLost(_ sched.SchedulerDriver, slaveID *mesos.SlaveID) {
	log.Printf("Slave %s lost", slaveID)
}
func (s *rendlerScheduler) ExecutorLost(_ sched.SchedulerDriver, executorID *mesos.ExecutorID, slaveID *mesos.SlaveID, status int) {
	log.Printf("Executor %s on slave %s was lost", executorID, slaveID)
}

func (s *rendlerScheduler) Error(_ sched.SchedulerDriver, err string) {
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
	seedURL := flag.String("seed", "http://mesosphere.com", "The first URL to crawl")
	master := flag.String("master", "127.0.1.1:5050", "Location of leading Mesos master")
	localMode := flag.Bool("local", true, "If true, saves rendered web pages on local disk")

	flag.Parse()

	scheduler := newRendlerScheduler(*seedURL, *localMode)
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
		if s != os.Interrupt {
			return
		}

		log.Println("RENDLER is shutting down")
		close(scheduler.shutdown)

		select {
		case <-scheduler.done:
		case <-time.After(shutdownTimeout):
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
