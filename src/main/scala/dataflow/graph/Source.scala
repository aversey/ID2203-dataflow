package dataflow.graph


import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.scaladsl.AbstractBehavior
import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import scala.collection.mutable


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
  override def toString = source.toString

  val outNodesCredits = mutable.Map[Int, Int]()
  source.outNodes.foreach: outNode =>
    outNodesCredits(outNode) = 0

  override def onInit() =
    // This is hacky
    actors(source.id) ! Event(source.outStream, source.initialData)

  var sleepTime = 10
  override def onCommand(msg: Command): Unit = msg match
    case msg: Event =>
      var data = msg.data.asInstanceOf[D]
      if outNodesCredits.values.forall(_ > 0) then
        outNodesCredits.mapValuesInPlace((_, x) => x - 1)
        val (d, res) = source.f(data)
        data = d
        source.outNodes.foreach: outNode =>
          actors(outNode) ! Event(source.outStream, res)
        sleepTime = 10
      else
        Thread.sleep(sleepTime)
        sleepTime *= 2
      // This is hacky
      actors(source.id) ! Event(source.outStream, data)
    case msg: Border =>
      source.outNodes.foreach: outNode =>
        actors(outNode) ! Border(source.outStream)

  override def onCredit(msg: Credit): Unit =
    outNodesCredits(msg.fromNode) += msg.amount
