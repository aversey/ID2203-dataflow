package dataflow.graph


import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.AbstractBehavior
import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.actor.typed.scaladsl.Behaviors


class NodeActor(context: ActorContext[FullCommand])
  extends AbstractBehavior[FullCommand](context):
  var started = false
  var actors  = List[ActorRef[FullCommand]]()
  override def onMessage(msg: FullCommand) =
    msg match
      case msg: Init =>
        if started then throw IllegalStateException("Actor already started")
        actors = msg.actors
        started = true
        onInit()
      case msg: Command =>
        if !started then throw IllegalStateException("Actor not started")
        onCommand(msg)
    Behaviors.same[FullCommand]
  def onInit(): Unit                = ()
  def onCommand(msg: Command): Unit = ???
