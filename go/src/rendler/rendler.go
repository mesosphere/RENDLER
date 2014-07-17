package main

import (
	"code.google.com/p/goprotobuf/proto"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/mesosphere/mesos-go/mesos"
	"strconv"
)

func main() {
	taskLimit := 5
	taskId := 0
	exit := make(chan bool)

	master := flag.String("master", "localhost:5050", "Location of leading Mesos master")
	localMode := flag.Bool("--local", false, "If true, saves rendered web pages on local disk")
	flag.Parse()

	rendlerArtifact := "http://downloads.mesosphere.io/demo/rendler.tgz"

	crawlExecutor := &mesos.ExecutorInfo{
		ExecutorId: &mesos.ExecutorID{Value: proto.String("crawl-executor")},
		Command: &mesos.CommandInfo{
			Value: proto.String("python crawl_executor.py"),
			Uris: []*mesos.CommandInfo_URI{
				&mesos.CommandInfo_URI{Value: &rendlerArtifact},
			},
		},
		Name:   proto.String("Crawler"),
		Source: proto.String("rendering-crawler"),
	}

	renderCommand := "python render_executor.py"

	if *localMode {
		renderCommand += " --local"
	}

	renderExecutor := &mesos.ExecutorInfo{
		ExecutorId: &mesos.ExecutorID{Value: proto.String("render-executor")},
		Command: &mesos.CommandInfo{
			Value: proto.String(renderCommand),
			Uris: []*mesos.CommandInfo_URI{
				&mesos.CommandInfo_URI{Value: &rendlerArtifact},
			},
		},
		Name:   proto.String("Renderer"),
		Source: proto.String("rendering-crawler"),
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
				// TODO
			},

			ResourceOffers: func(driver *mesos.SchedulerDriver, offers []mesos.Offer) {
				for _, offer := range offers {
					taskId++
					fmt.Printf("Launching task: %d\n", taskId)

					tasks := []mesos.TaskInfo{
						mesos.TaskInfo{
							Name: proto.String("go-task"),
							TaskId: &mesos.TaskID{
								Value: proto.String("go-task-" + strconv.Itoa(taskId)),
							},
							SlaveId:  offer.SlaveId,
							Executor: crawlExecutor,
							Resources: []*mesos.Resource{
								mesos.ScalarResource("cpus", 1),
								mesos.ScalarResource("mem", 512),
							},
						},
					}

					driver.LaunchTasks(offer.Id, tasks)
				}
			},

			StatusUpdate: func(driver *mesos.SchedulerDriver, status mesos.TaskStatus) {
				fmt.Println("Received task status: " + *status.Message)

				if *status.State == mesos.TaskState_TASK_FINISHED {
					taskLimit--
					if taskLimit <= 0 {
						exit <- true
					}
				}
			},

			FrameworkMessage: func(
				driver *mesos.SchedulerDriver,
				executorId mesos.ExecutorID,
				slaveId mesos.SlaveID,
				message string) {

				switch executorId.Value {
				case crawlExecutor.ExecutorId.Value:
					fmt.Printf("Received framework message from crawler")
					var crawlResult CrawlResult
					err := json.Unmarshal([]byte(message), &crawlResult)
					if err != nil {
						fmt.Printf("Error deserializing CrawlResult: [%s]", err)
					} else {
						fmt.Printf("CrawlResult: [%s]", crawlResult)
					}

				case renderExecutor.ExecutorId.Value:
					fmt.Printf("Received framework message from renderer")
					var renderResult RenderResult
					err := json.Unmarshal([]byte(message), &renderResult)
					if err != nil {
						fmt.Printf("Error deserializing RenderResult: [%s]", err)
					} else {
						fmt.Printf("RenderResult: [%s]", renderResult)
					}

				default:
					fmt.Printf("Received a framework message from some unknown source")
				}
			},
		},
	}

	driver.Init()
	defer driver.Destroy()

	driver.Start()
	<-exit
	driver.Stop(false)
}

func makeTaskPrototype(offer mesos.Offer) {
	// TODO
}

func makeCrawlTask(url string, offer mesos.Offer) {
	// TODO
}

func makeRenderTask(url string, offer mesos.Offer) {
	// TODO
}

func maxTasksForOffer(offer mesos.Offer) {
	// TODO
}

func printQueueStatistics(
	crawlQueue []string,
	renderQueue []string,
	runningTasks int32) {
	// TODO
}
