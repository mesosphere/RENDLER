package mesosphere.rendler

case class Edge(from: String, to: String) {
  override def toString(): String = s"($from, $to)"
}
