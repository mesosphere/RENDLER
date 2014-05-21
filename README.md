RENDLER :interrobang:
=====================

A rendering web crawler for Apache Mesos.

![YES RENDLER](http://img.pandawhale.com/57451-Jim-Carrey-Riddler-upvote-gif-NVsA.gif)

RENDLER consists of three main components:

- `CrawlExecutor` extends `mesos.Executor`
- `RenderExecutor` extends `mesos.Executor`
- `RenderingCrawler` extends `mesos.Scheduler` and launches tasks with the executors

## Quick Start with Vagrant

**Start the `mesos-demo` VM**

```bash
$ wget http://downloads.mesosphere.io/demo/mesos-demo.box -O /tmp/mesos-demo.box
$ vagrant box add --name mesos-demo /tmp/mesos-demo.box
$ git clone https://github.com/mesosphere/RENDLER.git
$ cd RENDLER
$ vagrant up
```

Now that the VM is running, you can view the Mesos Web UI here:
[http://10.141.141.10:5050](http://10.141.141.10:5050)

**Run RENDLER in the `mesos-demo` VM**

```bash
$ vagrant ssh
vagrant@mesos $ cd hostfiles
vagrant@mesos $ python rendler.py http://wikipedia.org 127.0.1.1:5050 --local
# <ctrl+D> to stop...
vagrant@mesos $ bin/make-pdf
Wrote graph to '/home/vagrant/hostfiles/result.pdf'
```

Open `result.pdf` in your favorite viewer to see the rendered result!

### Installing Dependencies:
**Beautiful Soup:** Used by the Crawl Executor to extract links from webpages.

```bash
$ sudo easy_install beautifulsoup4
```

**Phantomjs:** Used by the Render Executor to render a webpage into a png.

```bash
$ sudo brew install phantomjs
```

**Wget:** Used by the Dot Export Script to fetch rendered images from s3.

```bash
$ sudo easy_install wget
```

## Crawl Executor

- Interprets incoming tasks' `task.data` field as a URL
- Fetches the resource, extracts links from the document
- Sends a framework message to the scheduler containing the crawl result.

## Render Executor

- Interprets incoming tasks' `task.data` field as a URL
- Fetches the resource, saves a png image to a location accessible to the scheduler.
- Sends a framework message to the scheduler containing the render result.

## Intermediate Data Structures

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

## Scheduler

### Data Structures

- `crawlQueue`: list of urls
- `renderQueue`: list of urls
- `processedURLs`: set or urls
- `crawlResults`: list of url tuples
- `renderResults`: map of urls to imageUrls

### Scheduler Behavior

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

### Generating a pdf of your render graph output
**With [GraphViz](http://www.graphviz.org) installed:**

```bash
$ ./bin/make-pdf
Wrote graph to '/home/vagrant/hostfiles/result.pdf'
```

### Sample Output

![Sample Crawl Crawl](http://downloads.mesosphere.io/demo/sample_output.png)
