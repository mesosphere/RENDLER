package mesosphere.rendler

import org.apache.mesos._
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import java.io.File

object Rendler {

  lazy val frameworkInfo: Protos.FrameworkInfo =
    Protos.FrameworkInfo.newBuilder
      .setName("RENDLER")
      .setFailoverTimeout(60.seconds.toMillis)
      .setCheckpoint(false)
      .setUser("") // Mesos can do this for us
      .build

  def printUsage(): Unit = {
    println("""
      |Usage:
      |  run <seed-url> <mesos-master>
    """.stripMargin)
  }

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      printUsage()
      sys.exit(1)
    }

    val Seq(seedURL, mesosMaster) = args.toSeq

    println(s"""
      |RENDLER
      |=======
      |
      |    seedURL: [$seedURL]
      |mesosMaster: [$mesosMaster]
      |
    """.stripMargin)

    // TODO: get RENDLER_HOME from environment or args
    val rendlerHome = new File("/home/vagrant/hostfiles")

    val scheduler = new Scheduler(rendlerHome, seedURL)

    val driver: SchedulerDriver =
      new MesosSchedulerDriver(scheduler, frameworkInfo, mesosMaster)

    // driver.run blocks; therefore run in a separate thread
    Future { driver.run }

    // wait for input
    System.in.read()

    // graceful shutdown
    val maxWait = 2.minutes
    try {
      Await.ready(scheduler.shutdown(driver.stop()), maxWait)
    }
    catch {
      case toe: java.util.concurrent.TimeoutException =>
        println(s"Shutdown timed out after [${maxWait.toSeconds}] seconds")
    }
    finally {
      sys.exit(0)
    }
  }
}
