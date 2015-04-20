package com.mesosphere.rendler.executors;

import org.apache.mesos.*;
import org.apache.mesos.Protos.*;
import java.util.*;
import java.util.regex.*;
import java.io.*;
import java.net.*;

public class RenderExecutor implements Executor {

  String currPath;

  @Override
  public void registered(ExecutorDriver driver, ExecutorInfo executorInfo,
      FrameworkInfo frameworkInfo, SlaveInfo slaveInfo) {
    System.out.println("Registered executor on " + slaveInfo.getHostname());
    currPath = executorInfo.getData().toStringUtf8();
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
    String renderJSPath = currPath + "/render.js";
    String workPathDir = currPath + "/rendleroutput/";
    // Run phantom js
    String filename = workPathDir + pTaskInfo.getTaskId().getValue() + ".png";
    String cmd = "phantomjs " + renderJSPath + " " + url + " " + filename;

    try {
      runProcess(cmd);
      // If successful, send the framework message
      if (new File(filename).exists()) {
        String myStatus = "render" + url + "," + filename;
        pDriver.sendFrameworkMessage(myStatus.getBytes());
      }

    } catch (Exception e) {
      System.out.println("Exception executing phantomjs: " + e);
    }

    // Set the task with status finished
    status = TaskStatus.newBuilder().setTaskId(pTaskInfo.getTaskId())
        .setState(TaskState.TASK_FINISHED).build();
    pDriver.sendStatusUpdate(status);

  }

  /**
   * Print lines for any input stream, i.e. stdout or stderr.
   * 
   * @param name
   *          the label for the input stream
   * @param ins
   *          the input stream containing the data
   */
  private void printLines(String name, InputStream ins) throws Exception {
    String line = null;
    BufferedReader in = new BufferedReader(new InputStreamReader(ins));
    while ((line = in.readLine()) != null) {
      System.out.println(name + " " + line);
    }
  }

  /**
   * Execute a command with error logging.
   * 
   * @param the
   *          string containing the command that needs to be executed
   */
  private void runProcess(String command) throws Exception {
    Process pro = Runtime.getRuntime().exec(command);
    printLines(command + " stdout:", pro.getInputStream());
    printLines(command + " stderr:", pro.getErrorStream());
    pro.waitFor();
    System.out.println(command + " exitValue() " + pro.exitValue());
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
    MesosExecutorDriver driver = new MesosExecutorDriver(new RenderExecutor());
    System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
  }

}
