package mesosphere.rendler

import scala.collection.mutable
import java.io.{ File, FileWriter }
import java.nio.charset.Charset
import java.security.MessageDigest

trait GraphVizUtils {

  protected val digest = MessageDigest getInstance "SHA-256"

  protected def hash(value: String): String = {
    val utf8 = Charset.forName("UTF-8")
    val hashBytes = digest.digest(value.getBytes(utf8))
    hashBytes.map("%02X" format _).mkString
  }

  def writeDot(
    linkGraph: Seq[Edge],
    imageMap: Map[String, String],
    outFile: File): Unit = {
    println(s"Writing results to [${outFile.getAbsolutePath}]")

    val f = new FileWriter(outFile)

    imageMap foreach { case (from, to) => println(s"$from -> $to") }

    f write "digraph G {\n"
    f write "  node [shape=box];\n"

    val renderedURLs: Set[String] = imageMap.collect {
      case (url, imageUrl) if imageUrl startsWith "file:///" => {
        val fileName = imageUrl drop 8
        val hashedUrl = s"X${hash(url)}"
        f write s"""  $hashedUrl[label="" image="$fileName"];"""
        f write "\n"
        hashedUrl
      }
    }.toSet

    // add vertices
    for (Edge(from, to) <- linkGraph) {
      val fromHash = s"X${hash(from)}"
      val toHash = s"X${hash(to)}"
      if (renderedURLs.contains(fromHash) && renderedURLs.contains(toHash))
        f write s"  $fromHash -> $toHash\n"
    }

    f write "}\n"
    f.flush
    f.close

    println(s"Wrote results to [${outFile.getAbsolutePath}]")

  }

}