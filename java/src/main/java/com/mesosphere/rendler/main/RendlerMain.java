package com.mesosphere.rendler.main;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.Credential;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Scheduler;

import com.google.protobuf.ByteString;
import com.mesosphere.rendler.scheduler.RendlerScheduler;

public class RendlerMain {
  public static void main(String[] args) throws Exception {
    if (args.length < 1 || args.length > 3) {
      usage();
      System.exit(1);
    }

    String path = System.getProperty("user.dir")
        + "/target/rendler-1.0-SNAPSHOT-jar-with-dependencies.jar";

    CommandInfo.URI uri = CommandInfo.URI.newBuilder().setValue(path).setExtract(false).build();

    String commandCrawler = "java -cp rendler-1.0-SNAPSHOT-jar-with-dependencies.jar com.mesosphere.rendler.executors.CrawlExecutor";
    CommandInfo commandInfoCrawler = CommandInfo.newBuilder().setValue(commandCrawler).addUris(uri)
        .build();

    String commandRender = "java -cp rendler-1.0-SNAPSHOT-jar-with-dependencies.jar com.mesosphere.rendler.executors.RenderExecutor";
    CommandInfo commandInfoRender = CommandInfo.newBuilder().setValue(commandRender).addUris(uri)
        .build();

    ExecutorInfo executorCrawl = ExecutorInfo.newBuilder()
        .setExecutorId(ExecutorID.newBuilder().setValue("CrawlExecutor"))
        .setCommand(commandInfoCrawler).setName("Crawl Executor (Java)").setSource("java").build();

    ExecutorInfo executorRender = ExecutorInfo.newBuilder()
        .setExecutorId(ExecutorID.newBuilder().setValue("RenderExecutor"))
        .setCommand(commandInfoRender)
        .setData(ByteString.copyFromUtf8(System.getProperty("user.dir")))
        .setName("Render Executor (Java)").setSource("java").build();

    FrameworkInfo.Builder frameworkBuilder = FrameworkInfo.newBuilder().setFailoverTimeout(120000)
        .setUser("") // Have Mesos fill in
        // the current user.
        .setName("Rendler Framework (Java)");

    if (System.getenv("MESOS_CHECKPOINT") != null) {
      System.out.println("Enabling checkpoint for the framework");
      frameworkBuilder.setCheckpoint(true);
    }

    Scheduler scheduler = args.length == 1
        ? new RendlerScheduler(executorCrawl, executorRender)
        : new RendlerScheduler(executorCrawl, executorRender, Integer.parseInt(args[1]), args[2]);

    MesosSchedulerDriver driver = null;
    if (System.getenv("MESOS_AUTHENTICATE") != null) {
      System.out.println("Enabling authentication for the framework");

      if (System.getenv("DEFAULT_PRINCIPAL") == null) {
        System.err.println("Expecting authentication principal in the environment");
        System.exit(1);
      }

      if (System.getenv("DEFAULT_SECRET") == null) {
        System.err.println("Expecting authentication secret in the environment");
        System.exit(1);
      }

      Credential credential = Credential.newBuilder()
          .setPrincipal(System.getenv("DEFAULT_PRINCIPAL"))
          .setSecret(ByteString.copyFrom(System.getenv("DEFAULT_SECRET").getBytes())).build();

      frameworkBuilder.setPrincipal(System.getenv("DEFAULT_PRINCIPAL"));

      driver = new MesosSchedulerDriver(scheduler, frameworkBuilder.build(), args[0], credential);
    } else {
      frameworkBuilder.setPrincipal("test-framework-java");

      driver = new MesosSchedulerDriver(scheduler, frameworkBuilder.build(), args[0]);
    }

    int status = driver.run() == Status.DRIVER_STOPPED ? 0 : 1;

    // Ensure that the driver process terminates.
    driver.stop();

    System.exit(status);
  }

  private static void usage() {
    String name = RendlerScheduler.class.getName();
    System.err.println("Usage: " + name + " master <tasks> <url>");
  }

}
