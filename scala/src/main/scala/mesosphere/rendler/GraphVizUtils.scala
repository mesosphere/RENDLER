package mesosphere.rendler

import java.io.{ File, FileWriter }

trait GraphVizUtils {

  def writeDot(
    graph: Seq[Edge],
    images: Map[String, String],
    outFile: File): Unit = {
    println(s"Writing results to [${outFile.getAbsolutePath}]")
    val fout = new FileWriter(outFile)

    // TODO

  }

}