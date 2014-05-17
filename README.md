laughing-adventure
==================

A rendering web crawler for Apache Mesos.

## Crawl Executor

    sudo easy_install beautifulsoup4

## Render Executor

    sudo brew install phantomjs

## Scheduler

### Data Structures

- Render queue
- Crawl queue
- Results dictionary (resource -> image link)

### Scheduler Behavior

The scheduler accepts one URL as a command-line parameter to seed the crawler

1. For each URL, create a task in both the render queue and the crawl queue.

1. Upon receipt of a crawl result, filter the set to retain only URLs that
   match the domain of the seed URL and process each URL in the result.

1. Upon receipt of a render result , add an element to the results dictionary.

1. The crawl and render queues are drained in FCFS order at a rate determined
   by the resource offer stream.
