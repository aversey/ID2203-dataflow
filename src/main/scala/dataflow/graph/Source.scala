package dataflow.graph


import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.scaladsl.AbstractBehavior
import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.actor.typed.scaladsl.Behaviors


class Source[D] private[graph] (
  val id: Int,
  val initialData: D,
  val f: D => (D, Any),
  val outStream: Int
):
  override def toString: String = s"Source_$id"
  private[graph] var outNodes   = Set[Int]()


private class SourceActor[D](
  source: Source[D],
  context: ActorContext[FullCommand]
) extends NodeActor(context):
  override def onInit() =
    var data = source.initialData
    for i <- 0 until 100 do
      val (newData, res) = source.f(data)
      data = newData
      source.outNodes.foreach: outNode =>
        actors(outNode) ! Event(source.outStream, res)
        if i % 10 == 10 - 1 then
          // TODO: batching
          actors(outNode) ! Border(source.outStream)
