package dataflow.graph


import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.scaladsl.AbstractBehavior
import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.actor.typed.scaladsl.AskPattern.Askable
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt


class Sink[D] private[graph] (
  val id: Int,
  val initialData: D,
  val event: (D, Any) => D,
  val epoch: D => D,
  val inStreams: Set[Int]
):
  override def toString: String = s"Sink_$id"
  private[graph] var inNodes    = Set[Int]()


private class SinkActor[D](sink: Sink[D], context: ActorContext[FullCommand])
  extends NodeActor(context):
  override def toString = sink.toString

  val barrier: Barrier = Barrier(sink.inStreams, onEvent, doPrecommit, commit)
  override def onCommand(msg: Command) = barrier.onCommand(msg)

  var buffer = mutable.ListBuffer[mutable.ListBuffer[Any]](mutable.ListBuffer())
  var data   = sink.initialData
  val maxQueue = 500
  var consumed = mutable.Map[Int, Int](sink.inNodes.map(_ -> 0).toSeq*)

  override def onInit(): Unit = sink.inNodes.foreach: inNode =>
    actors(inNode) ! credit(sink.id, maxQueue)

  override def recover(e: Int, state: Any): Unit =
    buffer.clear()
    buffer.append(mutable.ListBuffer())
    data = state.asInstanceOf[D]
    consumed.mapValuesInPlace((_, _) => 0)
    barrier.recover(e)
    onInit()

  def onEvent(e: Event) =
    buffer.last.append(e.data)
    consumed(e.from) += 1
    if consumed(e.from) == maxQueue then
      actors(e.from) ! credit(sink.id, maxQueue)
      consumed(e.from) = 0

  def doPrecommit() =
    val pc = precommit(sink.id, barrier.epoch)
    storage
      .ask(r => Write(barrier.epoch, data, r))
      .onComplete(_ => coordinator ! pc)
    buffer.append(mutable.ListBuffer())

  def commit(msg: Commit) =
    buffer.head.foreach: d =>
      data = sink.event(data, d)
    data = sink.epoch(data)
    Await.result(storage.ask(r => Write(barrier.epoch, data, r)), 10.seconds)
    buffer.dropInPlace(1)
