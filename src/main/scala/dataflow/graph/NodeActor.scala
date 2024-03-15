package dataflow.graph


import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.Scheduler
import org.apache.pekko.actor.typed.scaladsl.AbstractBehavior
import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.actor.typed.scaladsl.AskPattern.Askable
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.pattern.StatusReply.Success
import org.apache.pekko.util.Timeout
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt


class NodeActor(context: ActorContext[FullCommand])
  extends AbstractBehavior[FullCommand](context):
  implicit val timeout: Timeout     = Timeout(1.second)
  implicit val scheduler: Scheduler = context.system.scheduler
  private var started               = false
  var actors                        = List[ActorRef[FullCommand]]()
  var storage                       = null: ActorRef[StorageCommand]
  var coordinator                   = null: ActorRef[CoordinatorCommand]
  private var generation            = 0

  def credit(from: Int, amount: Int) = Credit(generation, from, amount)
  def event(from: Int, data: Any)    = Event(generation, from, data)
  def border(from: Int)              = Border(generation, from)
  def precommit(n: Int, e: Int)      = Precommit(generation, n, e)

  override def onMessage(msg: FullCommand) =
    msg match
      case msg: Init =>
        if started then throw IllegalStateException("Actor already started")
        actors = msg.actors
        storage = msg.storage
        coordinator = msg.coordinator
        started = true
        onInit()
      case msg: Credit =>
        if msg.g == generation then onCredit(msg)
      case msg: Recover =>
        if !started then throw IllegalStateException("Actor not started")
        generation = msg.g
        val v = Await.result(storage.ask(r => Read(msg.e, r)), 10.seconds)
        Await.result(storage.ask(r => Clear(r)), 10.seconds)
        Await.result(storage.ask(r => Write(msg.e, v, r)), 10.seconds)
        recover(msg.e, v)
      case msg: Command =>
        if !started then throw IllegalStateException("Actor not started")
        if msg.g == generation then onCommand(msg)
    Behaviors.same[FullCommand]
  def onInit(): Unit                    = ()
  def onCredit(msg: Credit): Unit       = ()
  def recover(e: Int, state: Any): Unit = ()
  def onCommand(msg: Command): Unit     = ???
