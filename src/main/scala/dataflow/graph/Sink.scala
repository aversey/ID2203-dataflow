package dataflow.graph


import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.scaladsl.AbstractBehavior
import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import scala.collection.mutable


class Sink[D] private[graph] (
  val id: Int,
  val initialData: D,
  val f: (D, Any) => D,
  val inStreams: Set[Int]
):
  override def toString: String = s"Sink_$id"
  private[graph] var inNodes    = Set[Int]()


private class SinkActor[D](sink: Sink[D], context: ActorContext[FullCommand])
  extends NodeActor(context):
  override def toString = sink.toString

  val barrier = Barrier(sink.inStreams, onEvent, commit)
  override def onCommand(msg: Command) = barrier.onCommand(msg)

  var buffer   = List[Any]()
  var data     = sink.initialData
  val maxQueue = 100
  var consumed = 0

  override def onInit(): Unit = sink.inNodes.foreach: inNode =>
    actors(inNode) ! Credit(sink.id, maxQueue)

  def onEvent(e: Event) =
    buffer :+= e.data
    consumed += 1
    if consumed == maxQueue then
      onInit()
      consumed = 0

  def commit() =
    // TODO: commit
    buffer.foreach: d =>
      data = sink.f(data, d)
    buffer = List()
