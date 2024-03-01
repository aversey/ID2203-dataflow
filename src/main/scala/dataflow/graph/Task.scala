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
  private[graph] var inNodes    = Set[Int]()


private class TaskActor[D](task: Task[D], context: ActorContext[FullCommand])
  extends NodeActor(context):
  override def toString = task.toString

  val barrier: Barrier = Barrier(task.inStreams, onEvent, precommit, commit)
  override def onCommand(msg: Command) =
    barrier.onCommand(() => generation)(msg)

  var data     = task.initialData
  val maxQueue = 100
  var consumed = 0

  val outNodesCredits = mutable.Map[Int, Int]()
  task.outNodes.foreach: outNode =>
    outNodesCredits(outNode) = 0

  override def onInit(): Unit = task.inNodes.foreach: inNode =>
    actors(inNode) ! Credit(generation, task.id, maxQueue)

  override def recover(e: Int, state: Any): Unit =
    data = state.asInstanceOf[D]
    outNodesCredits.mapValuesInPlace((_, _) => 0)
    consumed = 0
    barrier.recover(e)
    onInit()

  def onEvent(e: Event) =
    if outNodesCredits.values.forall(_ > 0) then
      outNodesCredits.mapValuesInPlace((_, x) => x - 1)
      val (newData, res) = task.f(data, e.data)
      data = newData
      output(Event(generation, task.outStream, res))
      consumed += 1
      if consumed == maxQueue then
        onInit()
        consumed = 0
    else
      Thread.sleep(10)
      actors(task.id) ! e

  def precommit() =
    storage ! Write(generation, task.id, barrier.epoch, data)
    output(Border(generation, task.outStream))

  def commit(msg: Commit) = ()

  def output(msg: FullCommand) = task.outNodes.foreach: outNode =>
    actors(outNode) ! msg

  override def onCredit(msg: Credit): Unit =
    outNodesCredits(msg.from) += msg.amount
