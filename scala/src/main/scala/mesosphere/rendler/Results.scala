package mesosphere.rendler

import play.api.libs.json._

/**
  * The type of crawl results
  */
case class CrawlResult(taskId: String, url: String, links: Seq[String])

/**
  * The type of render results
  */
case class RenderResult(taskId: String, url: String, imageUrl: String)

/**
  * JSON Formatters for executor results
  */
trait ResultProtocol {
  implicit val crawlResultFormat = Json.format[CrawlResult]
  implicit val renderResultFormat = Json.format[RenderResult]
}
