RENDLER :interrobang:
=====================

A rendering web-crawler framework for [Apache Mesos](http://mesos.apache.org/).

![YES RENDLER](./riddler.jpg?raw=true "RENDLER") 

See the [accompanying slides](http://mesosphere.github.io/oscon-mesos-2014/#/) for more context.

RENDLER consists of three main components:

- `CrawlExecutor` extends `mesos.Executor`
- `RenderExecutor` extends `mesos.Executor`
- `RenderingCrawler` extends `mesos.Scheduler` and launches tasks with the executors

## Quick Start with Vagrant

### Requirements

- [VirtualBox](http://www.virtualbox.org/) 4.1.18+
- [Vagrant](http://www.vagrantup.com/) 1.3+
- [git](http://git-scm.com/downloads) (command line tool)

### Start the `mesos-demo` VM

```bash
$ wget http://downloads.mesosphere.io/demo/mesos.box -O /tmp/mesos.box
$ vagrant box add --name mesos-demo /tmp/mesos.box
$ git clone https://github.com/mesosphere/RENDLER.git
$ cd RENDLER
$ vagrant up
```

Now that the VM is running, you can view the Mesos Web UI here:
[http://10.141.141.10:5050](http://10.141.141.10:5050)

You can see that 1 slave is registered and you've got some idle CPUs and Memory. So let's start the Rendler!

### Run RENDLER in the `mesos-demo` VM

Check implementations of the RENDLER scheduler in the `python`, `go`,
`scala`, and `cpp` directories. Run instructions are here:

- [Python RENDLER framework](python/README.md)
- [Go RENDLER framework](go/README.md)
- [Scala RENDLER framework](scala/README.md)
- [C++ RENDLER framework](cpp/README.md)

Feel free to contribute your own!

### Generating a pdf of your render graph output
With [GraphViz](http://www.graphviz.org) (`which dot`) installed:

```bash
vagrant@mesos:hostfiles $ bin/make-pdf
Generating '/home/vagrant/hostfiles/result.pdf'
```

Open `result.pdf` in your favorite viewer to see the rendered result!

**Sample Output**

![Sample Crawl Crawl](http://downloads.mesosphere.io/demo/sample_output.png)

### Shutting down the `mesos-demo` VM

```bash
# Exit out of the VM
vagrant@mesos:hostfiles $ exit
# Stop the VM
$ vagrant halt
# To delete all traces of the vagrant machine
$ vagrant destroy
```

## Rendler Architecture

### Crawl Executor

- Interprets incoming tasks' `task.data` field as a URL
- Fetches the resource, extracts links from the document
- Sends a framework message to the scheduler containing the crawl result.

### Render Executor

- Interprets incoming tasks' `task.data` field as a URL
- Fetches the resource, saves a png image to a location accessible to the scheduler.
- Sends a framework message to the scheduler containing the render result.

### Intermediate Data Structures

We define some common data types to facilitate communication between the scheduler
and the executors.  Their default representation is JSON.

```python
results.CrawlResult(
    "1234",                                 # taskId
    "http://foo.co",                        # url
    ["http://foo.co/a", "http://foo.co/b"]  # links
)
```

```python
results.RenderResult(
    "1234",                                 # taskId
    "http://foo.co",                        # url
    "http://dl.mega.corp/foo.png"           # imageUrl
)
```

### Rendler Scheduler

#### Data Structures

- `crawlQueue`: list of urls
- `renderQueue`: list of urls
- `processedURLs`: set or urls
- `crawlResults`: list of url tuples
- `renderResults`: map of urls to imageUrls

#### Scheduler Behavior

The scheduler accepts one URL as a command-line parameter to seed the render
and crawl queues.

1. For each URL, create a task in both the render queue and the crawl queue.

1. Upon receipt of a crawl result, add an element to the crawl results
   adjacency list.  Append to the render and crawl queues each URL that is
   _not_ present in the set of processed URLs.  Add these enqueued urls to
   the set of processed URLs.

1. Upon receipt of a render result, add an element to the render results map.

1. The crawl and render queues are drained in FCFS order at a rate determined
   by the resource offer stream.  When the queues are empty, the scheduler
   declines resource offers to make them available to other frameworks running
   on the cluster.

