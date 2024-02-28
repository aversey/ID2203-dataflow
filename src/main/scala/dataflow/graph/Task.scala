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


private class TaskActor[D](task: Task[D], context: ActorContext[Command])
  extends AbstractBehavior[Command](context):
  var actors  = List[ActorRef[Command]]()
  var started = false
  val buffers = mutable.Map[Int, List[Command]]()
  var border  = Set[Int]()
  var data    = task.initialData
  override def onMessage(msg: Command) =
    msg match
      case Start(messageActors) =>
        if started then throw IllegalStateException("Task already started")
        actors = messageActors
        started = true
      case e: Event =>
        if !started then throw IllegalStateException("Task not started")
        if border(e.from) then
          if buffers.contains(e.from)
          then buffers(e.from) :+= e
          else buffers(e.from) = List(e)
        else
          val (newData, res) = task.f(data, e.data)
          data = newData
          task.outNodes.foreach: outNode =>
            actors(outNode) ! Event(task.id, res)
      case Border(from) =>
        if !started then throw IllegalStateException("Task not started")
        if border(from) then
          if buffers.contains(from)
          then buffers(from) :+= Border(from)
          else buffers(from) = List(Border(from))
        else
          border += from
          if border.size == task.inStreams.size then
            // TODO: commit
            task.outNodes.foreach: outNode =>
              actors(outNode) ! Border(task.id)
            border = Set()
            buffers.mapValuesInPlace: (from, buffer) =>
              buffer
                .takeWhile(_.isInstanceOf[Event])
                .foreach(onMessage)
              buffer.dropWhile(_.isInstanceOf[Event])
            buffers.mapValuesInPlace: (from, buffer) =>
              if buffer.isEmpty || buffer.head != Border(from) then buffer
              else
                border += from
                buffer.tail
    Behaviors.same
