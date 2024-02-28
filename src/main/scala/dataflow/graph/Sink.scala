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


private class SinkActor[D](sink: Sink[D], context: ActorContext[Command])
  extends AbstractBehavior[Command](context):
  var actors  = List[ActorRef[Command]]()
  var started = false
  val buffers = mutable.Map[Int, List[Command]]()
  var buffer  = List[Any]()
  var border  = Set[Int]()
  var data    = sink.initialData
  var i       = 0
  override def onMessage(msg: Command) =
    msg match
      case Start(messageActors) =>
        if started then throw IllegalStateException("Sink already started")
        actors = messageActors
        started = true
      case e: Event =>
        if !started then throw IllegalStateException("Sink not started")
        if border(e.from) then
          if buffers.contains(e.from)
          then buffers(e.from) :+= e
          else buffers(e.from) = List(e)
        else buffer :+= e.data
      case Border(from) =>
        if !started then throw IllegalStateException("Sink not started")
        if border(from) then
          if buffers.contains(from)
          then buffers(from) :+= Border(from)
          else buffers(from) = List(Border(from))
        else
          border += from
          if border.size == sink.inStreams.size then
            i += 1
            // TODO: commit
            println(s"vvv epoch $i vvv")
            buffer.foreach: d =>
              data = sink.f(data, d)
            println(s"^^^ epoch $i ^^^")
            buffer = List()
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
