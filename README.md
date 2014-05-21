RENDLER :interrobang:
=====================

A rendering web crawler for Apache Mesos.

![YES RENDLER](http://img.pandawhale.com/57451-Jim-Carrey-Riddler-upvote-gif-NVsA.gif)

## Quick Start with Vagrant

```bash
$ wget http://downloads.mesosphere.io/demo/mesos-demo.box -O /tmp/mesos-demo.box
$ vagrant box add --name mesos-demo /tmp/mesos-demo.box
$ git clone https://github.com/mesosphere/RENDLER.git
$ cd RENDLER
$ vagrant up
$ vagrant ssh
vagrant@mesos $ cd hostfiles
vagrant@mesos $ python rendler.py http://wikipedia.org 127.0.1.1:5050 --local
# <ctrl+D> to stop...
```

Once you run `vagrant up` you can see the Mesos Web UI at [http://10.141.141.10:5050](http://10.141.141.10:5050)

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
