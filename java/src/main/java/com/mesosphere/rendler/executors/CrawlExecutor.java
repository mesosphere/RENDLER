package com.mesosphere.rendler.executors;

import org.apache.mesos.*;
import org.apache.mesos.Protos.*;
import java.util.*;
import java.util.regex.*;
import java.io.*;
import java.net.*;

public class CrawlExecutor implements Executor {

  @Override
  public void registered(ExecutorDriver driver, ExecutorInfo executorInfo,
      FrameworkInfo frameworkInfo, SlaveInfo slaveInfo) {
    System.out.println("Registered executor on " + slaveInfo.getHostname());
  }

  @Override
  public void reregistered(ExecutorDriver driver, SlaveInfo executorInfo) {
  }

  @Override
  public void disconnected(ExecutorDriver driver) {
  }

  @Override
  public void launchTask(ExecutorDriver pDriver, TaskInfo pTaskInfo) {
    // Start task with status running
    TaskStatus status = TaskStatus.newBuilder().setTaskId(pTaskInfo.getTaskId())
        .setState(TaskState.TASK_RUNNING).build();
    pDriver.sendStatusUpdate(status);
    String url = pTaskInfo.getData().toStringUtf8();
    byte[] message = new byte[0];

    try {
      // Parse the links from the url
      String urlData = getUrlSource(url);
      List<String> links = getLinks(urlData);
      links.add(0, url);
      // Write list of links to byte array
      String linkStr = "crawl" + links.toString();
      message = linkStr.getBytes();
    } catch (IOException e) {
      System.out.println("Link may not be valid.  Error parsing the html: " + e);
    }
    // Send framework message and mark the task as finished
    pDriver.sendFrameworkMessage(message);
    status = TaskStatus.newBuilder().setTaskId(pTaskInfo.getTaskId())
        .setState(TaskState.TASK_FINISHED).build();
    pDriver.sendStatusUpdate(status);

  }

  /**
   * Extract the html source code from a webpage.
   * 
   * @param pURL
   *          the page url
   * @return the source code
   * 
   **/
  private String getUrlSource(String pURL) throws IOException {
    URL siteURL = new URL(pURL);
    URLConnection connection = siteURL.openConnection();
    BufferedReader myBufReader = new BufferedReader(new InputStreamReader(
        connection.getInputStream(), "UTF-8"));
    String inputLine;
    StringBuilder mySB = new StringBuilder();
    while ((inputLine = myBufReader.readLine()) != null)
      mySB.append(inputLine);
    myBufReader.close();

    return mySB.toString();
  }

  /**
   * Extract the links from a webpage.
   * 
   * @param pHtml
   *          the html source
   * @return the list of links
   * 
   **/
  private List<String> getLinks(String pHtml) {
    Pattern linkPattern = Pattern.compile("<a[^>]+href=[\"']?([\"'>]+)[\"']?[^>]*>(.+?)</a>",
        Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    Matcher pageMatcher = linkPattern.matcher(pHtml);
    ArrayList<String> links = new ArrayList<String>();
    while (pageMatcher.find()) {
      String fullLink = pageMatcher.group();
      int startQuoteIndex = fullLink.indexOf("\"");
      int endQuoteIndex = fullLink.indexOf("\"", startQuoteIndex + 1);
      String link = fullLink.substring(startQuoteIndex + 1, endQuoteIndex);
      // Heuristic used to check for valid urls
      if (link.contains("http") && !link.endsWith("signup")) {
        links.add(link);
      }
    }
    return links;
  }

  @Override
  public void killTask(ExecutorDriver driver, TaskID taskId) {
  }

  @Override
  public void frameworkMessage(ExecutorDriver driver, byte[] data) {
  }

  @Override
  public void shutdown(ExecutorDriver driver) {
  }

  @Override
  public void error(ExecutorDriver driver, String message) {
  }

  public static void main(String[] args) throws Exception {
    MesosExecutorDriver driver = new MesosExecutorDriver(new CrawlExecutor());
    System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
  }

}
