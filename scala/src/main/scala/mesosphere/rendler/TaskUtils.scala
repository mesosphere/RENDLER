package mesosphere.rendler

import org.apache.mesos._
import com.google.protobuf.ByteString
import scala.collection.JavaConverters._

trait TaskUtils {

  val TASK_CPUS = 0.1
  val TASK_MEM = 32.0

  def crawlExecutor(): Protos.ExecutorInfo = {
    val command = Protos.CommandInfo.newBuilder
      .setValue("python crawl_executor.py")
    // .addAllUris(...) // TODO
    Protos.ExecutorInfo.newBuilder
      .setExecutorId(Protos.ExecutorID.newBuilder.setValue("crawl-executor"))
      .setName("Crawler")
      .setSource("rendering-crawler")
      .setCommand(command)
      .build
  }

  def renderExecutor(): Protos.ExecutorInfo = {
    val command = Protos.CommandInfo.newBuilder
      .setValue("python render_executor.py")
    // .addAllUris(...) // TODO
    Protos.ExecutorInfo.newBuilder
      .setExecutorId(Protos.ExecutorID.newBuilder.setValue("render-executor"))
      .setName("Renderer")
      .setSource("rendering-crawler")
      .setCommand(command)
      .build
  }

  def makeTaskPrototype(id: String, offer: Protos.Offer): Protos.TaskInfo =
    Protos.TaskInfo.newBuilder
      .setTaskId(Protos.TaskID.newBuilder.setValue(id))
      .setSlaveId((offer.getSlaveId))
      .addAllResources(
        Seq(
          scalarResource("cpus", TASK_CPUS),
          scalarResource("mem", TASK_MEM)
        ).asJava
      )
      .build

  protected def scalarResource(name: String, value: Double): Protos.Resource =
    Protos.Resource.newBuilder
      .setType(Protos.Value.Type.SCALAR)
      .setName(name)
      .setScalar(Protos.Value.Scalar.newBuilder.setValue(value))
      .build

  def makeCrawlTask(
    id: String,
    url: String,
    offer: Protos.Offer): Protos.TaskInfo =
    makeTaskPrototype(id, offer).toBuilder
      .setName(s"render_$id")
      .setExecutor(renderExecutor())
      .setData(ByteString.copyFromUtf8(url))
      .build

  def makeRenderTask(
    id: String,
    url: String,
    offer: Protos.Offer): Protos.TaskInfo =
    makeTaskPrototype(id, offer).toBuilder
      .setName(s"crawl_$id")
      .setExecutor(renderExecutor())
      .setData(ByteString.copyFromUtf8(url))
      .build

  def maxTasksForOffer(
    offer: Protos.Offer,
    cpusPerTask: Double,
    memPerTask: Double): Int =
    ???

}
