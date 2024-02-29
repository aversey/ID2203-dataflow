package dataflow.graph


import org.apache.pekko.actor.typed.Behavior
import scala.collection.mutable


class Barrier(
  inStreams: Set[Int],
  onEvent: Event => Unit,
  commit: () => Unit
):
  val buffers        = mutable.Map[Int, List[Command]]()
  var borderReceived = Set[Int]()

  def onCommand(msg: Command) = msg match
    case msg: Event  => event(msg)
    case msg: Border => border(msg)

  def event(e: Event) =
    if borderReceived(e.from) then
      if buffers.contains(e.from)
      then buffers(e.from) :+= e
      else buffers(e.from) = List(e)
    else onEvent(e)

  def border(b: Border) =
    if borderReceived(b.from) then
      if buffers.contains(b.from)
      then buffers(b.from) :+= b
      else buffers(b.from) = List(b)
    else
      borderReceived += b.from
      if borderReceived.size == inStreams.size then
        commit()
        borderReceived = Set()
        buffers.mapValuesInPlace: (from, buffer) =>
          buffer
            .takeWhile(_.isInstanceOf[Event])
            .foreach(e => onEvent(e.asInstanceOf[Event]))
          buffer.dropWhile(_.isInstanceOf[Event])
        buffers.mapValuesInPlace: (from, buffer) =>
          if buffer.isEmpty || buffer.head != Border(from) then buffer
          else
            borderReceived += from
            buffer.tail
