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
  var epoch          = 0

  def onCommand(g: () => Int)(msg: Command) = msg match
    case msg: Event  => if msg.g == g() then event(msg)
    case msg: Border => if msg.g == g() then border(msg)

  def recover(e: Int) =
    buffers.clear()
    borderReceived = Set()
    epoch = e

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
        epoch += 1
        commit()
        borderReceived = Set()
        buffers.mapValuesInPlace: (from, buffer) =>
          buffer
            .takeWhile(_.isInstanceOf[Event])
            .foreach(e => onEvent(e.asInstanceOf[Event]))
          buffer.dropWhile(_.isInstanceOf[Event])
        buffers.mapValuesInPlace: (from, buffer) =>
          if buffer.isEmpty || !buffer.head.isInstanceOf[Border] then buffer
          else
            borderReceived += from
            buffer.tail
