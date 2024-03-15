package dataflow


import dataflow.graph.*
import dataflow.graph.id
import scala.collection.mutable


def main(args: Array[String]): Unit =
  if args.length != 1 && args.length != 2 then
    System.err.println("Usage: dataflow.Main <example> [errorRate]")
    System.exit(1)
  var errors = 1.0
  if args.length == 2 then
    errors = args(1).toDouble
    if errors <= 0.0 || errors > 100.0 then
      System.err.println("Error rate must be in (0, 100]")
      System.exit(1)
  val graph = Graph("Dataflow")
  args(0) match
    case "simple" =>
      val a = graph.newStream[Event]()
      benchmarkSource(graph, a)

      val b = graph.newStream[Event]()
      benchmarkStatelessTask(graph, noop, Set(a), b)

      benchmarkSink(graph, 1, Set(b))
    case "basic" =>
      val a = graph.newStream[Event]()
      benchmarkSource(graph, a)

      val b = graph.newStream[Event]()
      benchmarkStatelessTask(graph, noop, Set(a), b)

      val c = graph.newStream[Event]()
      benchmarkStatelessTask(graph, noop, Set(a), c)

      val d = graph.newStream[Event]()
      benchmarkStatelessTask(graph, noop, Set(b, c), d)

      val e = graph.newStream[Event]()
      benchmarkStatelessTask(graph, noop, Set(a, b, d), e)

      val f = graph.newStream[Event]()
      benchmarkStatelessTask(graph, noop, Set(e), f)

      benchmarkSink(graph, 4, Set(f))
    case "wide" =>
      val a = graph.newStream[Event]()
      benchmarkSource(graph, a)

      var tasks = Set[Stream[Event]]()
      for i <- 1 to 10 do
        val b = graph.newStream[Event]()
        benchmarkStatelessTask(graph, noop, Set(a), b)
        tasks += b

      benchmarkSink(graph, 10, tasks)
    case "tall" =>
      val a = graph.newStream[Event]()
      benchmarkSource(graph, a)

      var old = a
      for i <- 1 to 10 do
        val b = graph.newStream[Event]()
        benchmarkStatelessTask(graph, noop, Set(old), b)
        old = b

      benchmarkSink(graph, 1, Set(old))
    case _ =>
      System.err.println(s"Unknown example ${args(0)}")
      System.exit(1)
  withFailures(graph.run(), errors)
