package dataflow.graph


import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.scaladsl.AbstractBehavior
import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.actor.typed.scaladsl.AskPattern.Askable
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global


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

  var data = source.initialData
  override def onInit() =
    // This is hacky
    actors(source.id) ! event(source.outStream, ())

  override def recover(e: Int, state: Any): Unit =
    data = state.asInstanceOf[D]
    epoch = e
    sleepTime = 10
    outNodesCredits.mapValuesInPlace((_, _) => 0)
    onInit()

  var epoch     = 0
  var sleepTime = 10
  override def onCommand(msg: Command): Unit = msg match
    case _: Event =>
      if outNodesCredits.values.forall(_ > 0) then
        outNodesCredits.mapValuesInPlace((_, x) => x - 1)
        val (d, res) = source.f(data)
        data = d
        output(event(source.outStream, res))
        sleepTime = 10
      else
        Thread.sleep(sleepTime)
        sleepTime += 5
      // This is hacky
      actors(source.id) ! event(source.outStream, ())
    case _: Border =>
      epoch += 1
      val pc = precommit(source.id, epoch)
      storage
        .ask(r => Write(epoch, data, r))
        .onComplete(_ => coordinator ! pc)
      output(border(source.outStream))
      sleepTime = 10
    case _: Commit => ()

  def output(msg: FullCommand) = source.outNodes.foreach: outNode =>
    actors(outNode) ! msg

  override def onCredit(msg: Credit): Unit =
    outNodesCredits(msg.from) += msg.amount
