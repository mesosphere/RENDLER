package mesosphere.rendler

import org.apache.mesos._
import scala.concurrent.duration._
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

    driver.run
  }
}
