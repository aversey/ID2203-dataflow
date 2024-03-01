package dataflow.graph


import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.AbstractBehavior
import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.actor.typed.scaladsl.Behaviors


class NodeActor(context: ActorContext[FullCommand])
  extends AbstractBehavior[FullCommand](context):
  var started    = false
  var actors     = List[ActorRef[FullCommand]]()
  var storage    = null: ActorRef[StorageCommand]
  var generation = 0
  override def onMessage(msg: FullCommand) =
    msg match
      case msg: Init =>
        if started then throw IllegalStateException("Actor already started")
        actors = msg.actors
        storage = msg.storage
        started = true
        onInit()
      case msg: Credit =>
        onCredit(msg)
      case msg: Recover =>
        if !started then throw IllegalStateException("Actor not started")
        generation = msg.g
        recover(msg.e, msg.state)
      case msg: Command =>
        if !started then throw IllegalStateException("Actor not started")
        onCommand(msg)
    Behaviors.same[FullCommand]
  def onInit(): Unit                    = ()
  def onCredit(msg: Credit): Unit       = ()
  def recover(e: Int, state: Any): Unit = ()
  def onCommand(msg: Command): Unit     = ???
