package dataflow


import dataflow.graph.*
import dataflow.graph.id


def main(args: Array[String]): Unit =
  val graph = Graph("Dataflow")

  val a  = graph.newStream[Int]()
  val ap = graph.newSource(0, x => (x + 1, x), a)

  val b  = graph.newStream[String]()
  val bp = graph.newStatelessTask((x: Int) => (x + 1).toString, Set(a), b)

  val c  = graph.newStream[String]()
  val cp = graph.newStatelessTask((x: Int) => (x * 100).toString, Set(a), c)

  val sink = graph.newStatelessSink((x: String) => println(x), Set(b, c))

  graph.run()
