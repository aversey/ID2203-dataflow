package dataflow.graph


import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.scaladsl.AbstractBehavior
import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.actor.typed.scaladsl.AskPattern.Askable
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global


class Task[D] private[graph] (
  val id: Int,
  val initialData: D,
  val f: (D, Any) => (D, Any),
  val inStreams: Set[Int],
  val outStream: Int
):
  override def toString: String = s"Task_$id"
  private[graph] var outNodes   = Set[Int]()
  private[graph] var inNodes    = Set[Int]()


private class TaskActor[D](task: Task[D], context: ActorContext[FullCommand])
  extends NodeActor(context):
  override def toString = task.toString

  val barrier: Barrier = Barrier(task.inStreams, onEvent, doPrecommit, commit)
  var buffer           = List[Command]()
  override def onCommand(msg: Command) =
    if outNodesCredits.values.forall(_ > 0) then barrier.onCommand(msg)
    else buffer :+= msg

  var data     = task.initialData
  val maxQueue = 500
  var consumed = mutable.Map[Int, Int](task.inNodes.map(_ -> 0).toSeq*)

  val outNodesCredits = mutable.Map[Int, Int]()
  task.outNodes.foreach: outNode =>
    outNodesCredits(outNode) = 0

  override def onInit(): Unit = task.inNodes.foreach: inNode =>
    actors(inNode) ! credit(task.id, maxQueue)

  override def recover(e: Int, state: Any): Unit =
    data = state.asInstanceOf[D]
    outNodesCredits.mapValuesInPlace((_, _) => 0)
    consumed.mapValuesInPlace((_, _) => 0)
    buffer = List()
    barrier.recover(e)
    onInit()

  def onEvent(e: Event) =
    outNodesCredits.mapValuesInPlace((_, x) => x - 1)
    val (newData, res) = task.f(data, e.data)
    data = newData
    output(event(task.outStream, res))
    consumed(e.from) += 1
    if consumed(e.from) == maxQueue then
      actors(e.from) ! credit(task.id, maxQueue)
      consumed(e.from) = 0

  def doPrecommit() =
    val pc = precommit(task.id, barrier.epoch)
    storage
      .ask(r => Write(barrier.epoch, data, r))
      .onComplete(_ => coordinator ! pc)
    output(border(task.outStream))

  def commit(msg: Commit) = ()

  def output(msg: FullCommand) = task.outNodes.foreach: outNode =>
    actors(outNode) ! msg

  override def onCredit(msg: Credit): Unit =
    outNodesCredits(msg.from) += msg.amount
    if outNodesCredits.values.forall(_ > 0) then
      val buf = buffer
      buffer = List()
      buf.foreach(onCommand)
