package dataflow.graph


import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.scaladsl.adapter.*
import scala.collection.mutable
import scala.compiletime.ops.double
import scala.reflect.Typeable


case class Stream[A] private[graph] (val id: Int)

private type Node = Source[?] | Task[?] | Sink[?]


extension (n: Node)
  private[graph] def id: Int = n match
    case n: Source[?] => n.id
    case n: Task[?]   => n.id
    case n: Sink[?]   => n.id
  private[graph] def actor(
    context: ActorContext[FullCommand]
  ): Behavior[FullCommand] =
    n match
      case n: Source[?] => new SourceActor(n, context)
      case n: Task[?]   => new TaskActor(n, context)
      case n: Sink[?]   => new SinkActor(n, context)


class Graph(name: String):
  private var running = false

  // Streams /////////////////////////////////////////////////////////////////
  private var streams = 0
  def newStream[A: Typeable](): Stream[A] =
    if running then throw IllegalStateException("Graph is already running")
    streams += 1
    Stream[A](streams - 1)

  private var streamsWithProducers = Set[Int]()
  private var streamsWithConsumers = Set[Int]()

  // Nodes ///////////////////////////////////////////////////////////////////
  private var nodes = List[Node]()
  private def newNode[I, O, N <: Node](n: N): N =
    nodes :+= n
    n

  def newSource[D, O](d: D, f: D => (D, O), outStream: Stream[O]): Source[?] =
    if running then throw IllegalStateException("Graph is already running")
    if streamsWithProducers(outStream.id) then
      throw IllegalArgumentException(s"$outStream already has a producer")
    streamsWithProducers += outStream.id
    newNode(Source(nodes.length, d, f, outStream.id))
  def newStatelessSource[O](f: () => O, outStream: Stream[O]): Source[?] =
    newSource((), _ => ((), f()), outStream)

  def newTask[D, I: Typeable, O](
    d: D,
    f: (D, I) => (D, O),
    inStreams: Set[Stream[I]],
    outStream: Stream[O]
  ): Task[?] =
    if running then throw IllegalStateException("Graph is already running")
    if streamsWithProducers(outStream.id) then
      throw IllegalArgumentException(s"$outStream already has a producer")
    streamsWithProducers += outStream.id
    streamsWithConsumers |= inStreams.map(_.id)
    newNode(
      Task(
        nodes.length,
        d,
        { case (d, x: I) => f(d, x) },
        inStreams.map(_.id),
        outStream.id))
  def newStatelessTask[I: Typeable, O](
    f: I => O,
    inStreams: Set[Stream[I]],
    outStream: Stream[O]
  ): Task[?] =
    newTask((), (_, x: I) => ((), f(x)), inStreams, outStream)

  def newSink[D, I: Typeable](
    d: D,
    f: (D, I) => D,
    inStreams: Set[Stream[I]]
  ): Sink[?] =
    if running then throw IllegalStateException("Graph is already running")
    streamsWithConsumers |= inStreams.map(_.id)
    newNode(
      Sink(nodes.length, d, { case (d, x: I) => f(d, x) }, inStreams.map(_.id)))
  def newStatelessSink[I: Typeable](
    f: I => Unit,
    inStreams: Set[Stream[I]]
  ): Sink[?] =
    newSink((), (_, x: I) => f(x), inStreams)

  // Packing /////////////////////////////////////////////////////////////////
  def check =
    for i <- 0 until streams do
      if !streamsWithProducers(i) then
        throw IllegalStateException(s"Stream $i has no producer")
      if !streamsWithConsumers(i) then
        throw IllegalStateException(s"Stream $i has no consumer")
    // TODO? check for loops

  private def pack() =
    check
    val streamConsumers =
      mutable.ArraySeq.make((0 until streams).map(_ => Set[Int]()).toArray)
    val streamProducer = mutable.Map[Int, Int]()
    for n <- nodes do
      n match
        case n: Source[?] => streamProducer(n.outStream) = n.id
        case n: Task[?] =>
          streamProducer(n.outStream) = n.id
          n.inStreams.foreach: s =>
            streamConsumers(s) += n.id
        case n: Sink[?] =>
          n.inStreams.foreach: s =>
            streamConsumers(s) += n.id
    for n <- nodes do
      n match
        case n: Source[?] => n.outNodes = streamConsumers(n.outStream)
        case n: Task[?] =>
          n.outNodes = streamConsumers(n.outStream)
          n.inNodes = n.inStreams.map(streamProducer)
        case n: Sink[?] => n.inNodes = n.inStreams.map(streamProducer)

  def run() =
    if running then throw IllegalStateException("Graph is already running")
    pack()
    running = true
    val system = ActorSystem(name)
    val actors = nodes.map: n =>
      system.spawn(
        Behaviors.setup[FullCommand](ctx => n.actor(ctx)),
        n.toString)

    actors.foreach(actorRef => actorRef ! Init(actors))

    val sources = nodes.collect { case s: Source[?] => s }
    while true do
      Thread.sleep(1000)
      sources.foreach: s =>
        actors(s.id) ! Border(s.outStream)
