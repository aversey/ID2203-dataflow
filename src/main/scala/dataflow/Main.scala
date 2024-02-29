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

  val sink = graph.newSink(-1, throughput, Set(b, c))

  graph.run()


var startTime = 0L


def throughput(count: Int, x: String): Int =
  if count == -1 then startTime = System.currentTimeMillis()
  if count % 1000 == 1000 - 1 then
    val endTime = System.currentTimeMillis()
    val time    = endTime - startTime
    val rate    = count.toDouble / time * 1000
    println(rate)
  count + 1
