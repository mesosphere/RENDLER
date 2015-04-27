package com.mesosphere.rendler.scheduler;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Protos.Value;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import com.google.protobuf.ByteString;
import com.mesosphere.rendler.util.GraphWriter;

public class RendlerScheduler implements Scheduler {

  private final ExecutorInfo executorCrawler, executorRender;
  private final int totalTasks;
  private int launchedTasks = 0;
  private int finishedTasks = 0;
  private List<String> crawlQueue;
  private List<String> completedCrawlQueue;
  private Map<String, Set<String>> edgeList;
  private Map<String, String> urlToFileNameMap;

  public RendlerScheduler(ExecutorInfo executorCrawler, ExecutorInfo executorRender) {
    this(executorCrawler, executorRender, 5, "https://mesosphere.com");
  }

  public RendlerScheduler(ExecutorInfo executorCrawler, ExecutorInfo executorRender,
      int totalTasks, String url) {
    this.executorCrawler = executorCrawler;
    this.executorRender = executorRender;
    this.totalTasks = totalTasks;
    this.crawlQueue = Collections.synchronizedList(new ArrayList<String>());
    this.completedCrawlQueue = Collections.synchronizedList(new ArrayList<String>());
    this.edgeList = Collections.synchronizedMap(new HashMap<String, Set<String>>());
    this.urlToFileNameMap = Collections.synchronizedMap(new HashMap<String, String>());
    this.crawlQueue.add(url);

  }

  @Override
  public void registered(SchedulerDriver driver, FrameworkID frameworkId, MasterInfo masterInfo) {
    System.out.println("Registered! ID = " + frameworkId.getValue());
  }

  @Override
  public void reregistered(SchedulerDriver driver, MasterInfo masterInfo) {
  }

  @Override
  public void disconnected(SchedulerDriver driver) {
  }

  @Override
  public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
    for (Offer offer : offers) {
      List<TaskInfo> tasks = new ArrayList<TaskInfo>();
      if (launchedTasks < totalTasks && !crawlQueue.isEmpty()) {
        TaskID taskId = TaskID.newBuilder().setValue(Integer.toString(launchedTasks++)).build();
        // Create the crawl and render tasks with the first task on the
        // queue
        String urlData = crawlQueue.get(0);

        System.out.println("Launching task " + taskId.getValue() + " with input: " + urlData);
        // Task for crawler
        TaskInfo task = TaskInfo
            .newBuilder()
            .setName("task " + taskId.getValue())
            .setTaskId(taskId)
            .setSlaveId(offer.getSlaveId())
            .addResources(
                Resource.newBuilder().setName("cpus").setType(Value.Type.SCALAR)
                    .setScalar(Value.Scalar.newBuilder().setValue(1)))
            .addResources(
                Resource.newBuilder().setName("mem").setType(Value.Type.SCALAR)
                    .setScalar(Value.Scalar.newBuilder().setValue(128)))
            .setData(ByteString.copyFromUtf8(crawlQueue.get(0)))
            .setExecutor(ExecutorInfo.newBuilder(executorCrawler)).build();

        taskId = TaskID.newBuilder().setValue(Integer.toString(launchedTasks++)).build();

        System.out.println("Launching task " + taskId.getValue() + " with input: " + urlData);

        // Task for render
        TaskInfo taskRender = TaskInfo
            .newBuilder()
            .setName("task " + taskId.getValue())
            .setTaskId(taskId)
            .setSlaveId(offer.getSlaveId())
            .addResources(
                Resource.newBuilder().setName("cpus").setType(Value.Type.SCALAR)
                    .setScalar(Value.Scalar.newBuilder().setValue(1)))
            .addResources(
                Resource.newBuilder().setName("mem").setType(Value.Type.SCALAR)
                    .setScalar(Value.Scalar.newBuilder().setValue(128)))
            .setData(ByteString.copyFromUtf8(urlData))
            .setExecutor(ExecutorInfo.newBuilder(executorRender)).build();

        tasks.add(task);
        tasks.add(taskRender);
        // Dequeue and update completed list
        completedCrawlQueue.add(urlData);
        crawlQueue.remove(0);

      }
      driver.launchTasks(offer.getId(), tasks);
    }
  }

  @Override
  public void offerRescinded(SchedulerDriver driver, OfferID offerId) {
  }

  @Override
  public void statusUpdate(SchedulerDriver driver, TaskStatus status) {

    if (status.getState() == TaskState.TASK_FINISHED || status.getState() == TaskState.TASK_LOST) {
      System.out.println("Status update: task " + status.getTaskId().getValue()
          + " has completed with state " + status.getState());
      finishedTasks++;
      System.out.println("Finished tasks: " + finishedTasks);
      if (finishedTasks == totalTasks) {
        // Once the total allowed tasks are finished, write the graph
        // file
        String parentDir = new File(System.getProperty("user.dir")).getParent();
        GraphWriter graphWriter = new GraphWriter();
        graphWriter.writeDotFile(parentDir + "/result.dot", urlToFileNameMap, edgeList);
        driver.stop();
      }
    } else {
      System.out.println("Status update: task " + status.getTaskId().getValue() + " is in state "
          + status.getState());
    }
  }

  @Override
  public void frameworkMessage(SchedulerDriver driver, ExecutorID executorId, SlaveID slaveId,
      byte[] data) {
    String childUrlStr = new String(data);
    // Find all child links and process these
    if (childUrlStr.startsWith("crawl")) {
      childUrlStr = childUrlStr.substring(5);
      // Add all links found to the queue
      if (childUrlStr.length() >= 2) {
        // Parse and remove whitespace. Add the queue, if not already in
        // the queue to crawl
        // and render the child pages
        childUrlStr = childUrlStr.substring(1, childUrlStr.length() - 1);
        String[] arrListUrls = childUrlStr.split(",");
        LinkedList<String> listUrls = new LinkedList<String>(Arrays.asList(arrListUrls));
        for (int i = 0; i < listUrls.size(); i++) {
          String listUrl = listUrls.get(i);
          listUrl = listUrl.replaceAll("\\s+", "");
          listUrls.set(i, listUrl);
          if (!completedCrawlQueue.contains(listUrl) && !crawlQueue.contains(listUrl)) {
            crawlQueue.add(listUrl);
          }
        }
        // Create the edges from the source to all child links. Also
        // create the edges to all
        // related nodes in the graph, siblings, parents, great grand
        // parents, etc
        String sourceUrl = listUrls.get(0);
        listUrls.remove(0);
        Set<String> existingLinks = edgeList.keySet();
        for (String existingLink : existingLinks) {
          Set<String> childLinks = edgeList.get(existingLink);
          childLinks.addAll(listUrls);
          edgeList.put(existingLink, childLinks);
        }
        if (edgeList.get(sourceUrl) == null) {
          edgeList.put(sourceUrl, new HashSet<String>(listUrls));
        }
      }
    }
    // Find all urls that have been rendered in image files
    else if (childUrlStr.startsWith("render")) {
      childUrlStr = childUrlStr.substring(6);
      String[] arrListUrls = childUrlStr.split(",");
      urlToFileNameMap.put(arrListUrls[0], arrListUrls[1]);
    }
  }

  @Override
  public void slaveLost(SchedulerDriver driver, SlaveID slaveId) {
  }

  @Override
  public void executorLost(SchedulerDriver driver, ExecutorID executorId, SlaveID slaveId,
      int status) {
  }

  @Override
  public void error(SchedulerDriver driver, String message) {
    System.out.println("Error: " + message);
  }

}
