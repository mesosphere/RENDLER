package mesosphere.rendler

import org.apache.mesos
import mesos._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import java.io.File
import java.nio.charset.Charset

class SchedulerSkeleton(val rendlerHome: File, seedURL: String)
    extends mesos.Scheduler
    with ResultProtocol
    with TaskUtils
    with GraphVizUtils {

  protected[this] val crawlQueue = mutable.Queue[String](seedURL)
  protected[this] val renderQueue = mutable.Queue[String](seedURL)

  protected[this] val processedURLs = mutable.Set[String]()
  protected[this] val crawlResults = mutable.Buffer[Edge]()
  protected[this] val renderResults = mutable.Map[String, String]()

  private[this] var tasksCreated = 0
  private[this] var tasksRunning = 0
  private[this] var shuttingDown: Boolean = false

  def shutdown[T](callback: => T): Future[T] =
    Future {
      shuttingDown = true
      println("Scheduler shutting down...")
      while (tasksRunning > 0) Thread.sleep(500)
      writeDot(crawlResults, renderResults.toMap, new File(rendlerHome, "result.dot"))
      callback
    }

  def printQueueStatistics(): Unit = println(s"""
    |Queue Statistics:
    |  Crawl queue length:  [${crawlQueue.size}]
    |  Render queue length: [${renderQueue.size}]
    |  Running tasks:       [$tasksRunning]
  """.stripMargin)

  def disconnected(driver: SchedulerDriver): Unit =
    println("Disconnected from the Mesos master...")

  def error(driver: SchedulerDriver, msg: String): Unit =
    println(s"ERROR: [$msg]")

  def executorLost(
    driver: SchedulerDriver,
    executorId: Protos.ExecutorID,
    slaveId: Protos.SlaveID,
    status: Int): Unit =
    println(s"EXECUTOR LOST: [${executorId.getValue}]")

  def frameworkMessage(
    driver: SchedulerDriver,
    executorId: Protos.ExecutorID,
    slaveId: Protos.SlaveID,
    data: Array[Byte]): Unit = {

    import play.api.libs.json._

    println(s"Received a framework message from [${executorId.getValue}]")
    val jsonString = new String(data, Charset.forName("UTF-8"))

    ??? // TODO
  }

  def offerRescinded(
    driver: SchedulerDriver,
    offerId: Protos.OfferID): Unit =
    println(s"Offer [${offerId.getValue}] has been rescinded")

  def registered(
    driver: SchedulerDriver,
    frameworkId: Protos.FrameworkID,
    masterInfo: Protos.MasterInfo): Unit = {
    val host = masterInfo.getHostname
    val port = masterInfo.getPort
    println(s"Registered with Mesos master [$host:$port]")
  }

  def reregistered(
    driver: SchedulerDriver,
    masterInfo: Protos.MasterInfo): Unit = ???

  def resourceOffers(
    driver: SchedulerDriver,
    offers: java.util.List[Protos.Offer]): Unit = {

    printQueueStatistics()

    for (offer <- offers.asScala) {
      println(s"Got resource offer [$offer]")
    }

    ??? // TODO
  }

  def slaveLost(
    driver: SchedulerDriver,
    slaveId: Protos.SlaveID): Unit =
    println("SLAVE LOST: [${slaveId.getValue}]")

  def statusUpdate(
    driver: SchedulerDriver,
    taskStatus: Protos.TaskStatus): Unit = {
    val taskId = taskStatus.getTaskId.getValue
    val state = taskStatus.getState
    println(s"Task [$taskId] is in state [$state]")

    ??? // TODO
  }

}