package mesosphere.rendler

import org.apache.mesos
import mesos._

import scala.collection.JavaConverters._

class Scheduler extends mesos.Scheduler with TaskUtils with ResultProtocol {

  def disconnected(driver: SchedulerDriver): Unit = ???

  def error(driver: SchedulerDriver, msg: String): Unit = ???

  def executorLost(
    driver: SchedulerDriver,
    executorId: Protos.ExecutorID,
    slaveId: Protos.SlaveID,
    x$4: Int): Unit = ???

  def frameworkMessage(
    driver: SchedulerDriver,
    executorId: Protos.ExecutorID,
    slaveId: Protos.SlaveID,
    data: Array[Byte]): Unit = ???

  def offerRescinded(
    driver: SchedulerDriver,
    offerId: Protos.OfferID): Unit = ???

  def registered(
    driver: SchedulerDriver,
    frameworkId: Protos.FrameworkID,
    masterInfo: Protos.MasterInfo): Unit = ???

  def reregistered(
    driver: SchedulerDriver,
    masterInfo: Protos.MasterInfo): Unit = ???

  def resourceOffers(
    driver: SchedulerDriver,
    offers: java.util.List[Protos.Offer]): Unit = ???

  def slaveLost(
    driver: SchedulerDriver,
    slaveId: Protos.SlaveID): Unit = ???

  def statusUpdate(
    driver: SchedulerDriver,
    taskStatus: Protos.TaskStatus): Unit = ???

}