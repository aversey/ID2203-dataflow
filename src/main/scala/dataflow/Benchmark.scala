package dataflow


import dataflow.graph.*
import org.apache.pekko.actor.typed.ActorRef
import scala.collection.mutable


def withFailures(coordinator: ActorRef[CoordinatorCommand], rate: Double) =
  while true do
    Thread.sleep(10)
    if Math.random() < rate / 100 then
      println("--- Failure! --------------------------------------")
      coordinator ! Fail()


case class Event(x: Int, ts: Long)


def benchmarkSink(graph: Graph, expected: Int, inputs: Set[Stream[Event]]) =
  graph.newEpochSink(State(), event, epoch(expected), inputs)


def benchmarkSource(graph: Graph, out: Stream[Event]) =
  graph.newSource(0, x => (x + 1, Event(x, System.currentTimeMillis())), out)


def benchmarkStatelessTask(
  graph: Graph,
  f: Int => Int,
  inputs: Set[Stream[Event]],
  out: Stream[Event]) =
  graph.newStatelessTask((e: Event) => e.copy(x = f(e.x)), inputs, out)


def noop(x: Int) = ("0" + x.toString).toInt


case class State(
  events: mutable.Map[Int, Int] = mutable.Map().withDefaultValue(0),
  max: Int = Int.MinValue,
  epoch: Int = 0,
  deltas: Long = 0L,
  count: Long = 0L,
  totalDeltas: Long = 0L,
  totalOutput: Long = 0L,
  lastEpoch: Long = 0L,
  firstEpoch: Long = 0L,
  total: Int = 0,
  failed: Int = 0
)


def event(state: State, x: Event) =
  state.events(x._1) += 1
  state.copy(
    deltas = state.deltas + (System.currentTimeMillis() - x._2),
    count = state.count + 1)


def epoch(expected: Int)(state: State) =
  var failed     = 0
  var firstEpoch = state.firstEpoch
  val count      = state.events.size
  if state.epoch >= 1 then
    val missing = state.events.filterNot(_._2 == expected)
    if missing.nonEmpty then
      System.err.println(s"Epoch ${state.epoch} misses events $missing")
      failed = 1
    if count > 1 then
      state.events.keys.toList.sorted
        .sliding(2)
        .foreach: x =>
          if x(0) + 1 != x(1) then
            System.err.println(
              s"Epoch ${state.epoch} has gap between ${x(0)} and ${x(1)}")
            failed = 1
    if state.events.nonEmpty then
      val m = state.events.keys.min
      if m != state.max + 1 then
        System.err.println(
          s"Epoch ${state.epoch} vs previous : $m vs ${state.max}")
        failed = 1
    print("%5s".format("Epoch"))
    print("%12s".format("Throughput"))
    print("%8s".format("Latency"))
    print("%4s".format("Bad"))
    print(" |")
    print("%12s".format("Throughput"))
    print("%8s".format("Latency"))
    println()
    print("%5s".format(state.epoch))
    var rate = count.toDouble * 1000 /
      (System.currentTimeMillis() - state.lastEpoch)
    if rate.isFinite && !rate.isNaN then print("%12.0f".format(rate))
    else print("%6s".format("---"))
    var latency = state.deltas.toDouble / state.count
    if latency.isFinite && !latency.isNaN then print("%8.0f".format(latency))
    else print("%8s".format("---"))
    print("%4s".format(state.failed + failed))
    print(" |")
    rate = state.total.toDouble * 1000 /
      (System.currentTimeMillis() - state.firstEpoch)
    if rate.isFinite && !rate.isNaN then print("%12.0f".format(rate))
    else print("%6s".format("---"))
    latency =
      (state.totalDeltas.toDouble + state.deltas) / (state.totalOutput + state.count)
    if latency.isFinite && !latency.isNaN then print("%8.0f".format(latency))
    else print("%8s".format("---"))
    println()
  else
    println(s"Initializing...")
    firstEpoch = System.currentTimeMillis()
  state.copy(
    epoch = state.epoch + 1,
    max = if state.events.nonEmpty then state.events.keys.max else state.max,
    lastEpoch = System.currentTimeMillis(),
    events = mutable.Map().withDefaultValue(0),
    total = state.total + count,
    deltas = 0,
    count = 0,
    totalDeltas = state.totalDeltas + state.deltas,
    totalOutput = state.totalOutput + state.count,
    failed = failed + state.failed,
    firstEpoch = firstEpoch
  )
