package dataflow.graph


import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.AbstractBehavior
import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import scala.collection.mutable


sealed trait StorageCommand

case class Write(k: Any, v: Any, r: ActorRef[Unit]) extends StorageCommand

case class Clear(r: ActorRef[Unit]) extends StorageCommand

case class Read(k: Any, r: ActorRef[Any]) extends StorageCommand


/* Assumed not to fail, local for each task */
class Storage(
  initial: Map[Any, Any],
  context: ActorContext[StorageCommand]
) extends AbstractBehavior[StorageCommand](context):
  val storage = mutable.Map[Any, Any](initial.toSeq*)
  override def onMessage(msg: StorageCommand) =
    msg match
      case msg: Write =>
        storage(msg.k) = msg.v
        msg.r ! ()
      case msg: Clear =>
        storage.clear()
        msg.r ! ()
      case msg: Read =>
        msg.r ! storage(msg.k)
    Behaviors.same[StorageCommand]
