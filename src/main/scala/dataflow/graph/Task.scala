package dataflow.graph


import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.scaladsl.AbstractBehavior
import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import scala.collection.mutable


class Task[D] private[graph] (
  val id: Int,
  val initialData: D,
  val f: (D, Any) => (D, Any),
  val inStreams: Set[Int],
  val outStream: Int
):
  override def toString: String = s"Task_$id"
  private[graph] var outNodes   = Set[Int]()


private class TaskActor[D](task: Task[D], context: ActorContext[FullCommand])
  extends NodeActor(context):
  val barrier = Barrier(task.inStreams, onEvent, commit)
  override def onCommand(msg: Command) = barrier.onCommand(msg)

  var data = task.initialData

  def onEvent(e: Event) =
    val (newData, res) = task.f(data, e.data)
    data = newData
    output(Event(task.outStream, res))

  def commit() =
    // TODO: commit
    output(Border(task.outStream))

  def output(msg: FullCommand) = task.outNodes.foreach: outNode =>
    actors(outNode) ! msg
