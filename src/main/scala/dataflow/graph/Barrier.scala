package dataflow.graph


import org.apache.pekko.actor.typed.Behavior
import scala.collection.mutable


class Barrier(
  inStreams: Set[Int],
  onEvent: Event => Unit,
  doPrecommit: () => Unit,
  commit: Commit => Unit
):
  val buffers = mutable.Map[Int, List[Command]]()
  for inStream <- inStreams do buffers(inStream) = List()
  var borderReceived = Set[Int]()
  var epoch          = 0

  def onCommand(msg: Command) = msg match
    case msg: Event  => event(msg)
    case msg: Border => border(msg)
    case msg: Commit => commit(msg)

  def recover(e: Int) =
    for inStream <- inStreams do buffers(inStream) = List()
    borderReceived = Set()
    epoch = e

  def event(e: Event) =
    if borderReceived(e.from) then buffers(e.from) :+= e
    else onEvent(e)

  def border(b: Border) =
    if borderReceived(b.from) then buffers(b.from) :+= b
    else
      borderReceived += b.from
      if borderReceived == inStreams then
        epoch += 1
        doPrecommit()
        borderReceived = Set()
        buffers.mapValuesInPlace: (from, buffer) =>
          buffer
            .takeWhile(_.isInstanceOf[Event])
            .foreach(e => onEvent(e.asInstanceOf[Event]))
          buffer.dropWhile(_.isInstanceOf[Event])
        buffers.mapValuesInPlace: (from, buffer) =>
          if buffer.isEmpty then buffer
          else
            borderReceived += from
            buffer.tail
