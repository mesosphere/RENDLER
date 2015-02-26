package mesosphere.rendler

import java.io.File

import org.apache.mesos._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

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

    // wait for the enter key
    val NEWLINE = '\n'.toInt
    while (System.in.read != NEWLINE) {
      Thread.sleep(1000)
    }

    // graceful shutdown
    scheduler.shutdown(5.minutes) { driver.stop() }
    sys.exit(0)
  }
}
