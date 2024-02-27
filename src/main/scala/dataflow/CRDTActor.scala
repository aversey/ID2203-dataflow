package dataflow


import java.util.concurrent.ThreadLocalRandom
import org.apache.pekko.actor.Address
import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.AbstractBehavior
import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.UniqueAddress
import org.apache.pekko.cluster.ddata.LWWMap
import org.apache.pekko.cluster.ddata.ReplicatedDelta
import org.apache.pekko.cluster.ddata.SelfUniqueAddress


object CRDTActor:
  // The type of messages that the actor can handle
  sealed trait Command

  // Messages containing the CRDT delta state exchanged between actors
  case class DeltaMsg(from: ActorRef[Command], delta: ReplicatedDelta)
      extends Command

  // Triggers the actor to start the computation (do this only once!)
  case class Start(actors: Map[Int, ActorRef[Command]]) extends Command

  // Triggers the actor to consume an operation (do this repeatedly!)
  case object ConsumeOperation extends Command


import CRDTActor.*


class CRDTActor(
    id: Int,
    ctx: ActorContext[Command]
) extends AbstractBehavior[Command](ctx):
  // The CRDT state of this actor, mutable var as LWWMap is immutable
  private var crdtstate = LWWMap.empty[String, Int]

  /* The CRDT address of this actor/node, used for the CRDT state to identify
   * the nodes */
  private var selfNode = SelfUniqueAddress(
    UniqueAddress(
      Address("crdtactor", "crdt"),
      ThreadLocalRandom.current.nextLong()))

  private var others = Map[Int, ActorRef[Command]]()

  // Note: you probably want to modify this method to be more efficient
  private def broadcastAndResetDeltas(): Unit =
    val deltaOption = crdtstate.delta
    deltaOption match
      case None => ()
      case Some(delta) =>
        crdtstate = crdtstate.resetDelta // May be omitted
        others.foreach: (name, actorRef) =>
          actorRef !
            DeltaMsg(ctx.self, delta)

  // This is the event handler of the actor, implement its logic here
  // Note: the current implementation is rather inefficient, you can probably
  // do better by not sending as many delta update messages
  override def onMessage(msg: Command): Behavior[Command] = msg match
    case Start(actors) =>
      others = actors - id
      ctx.log.info(s"CRDTActor-$id started")
      ctx.self ! ConsumeOperation // start consuming operations
      Behaviors.same

    case ConsumeOperation =>
      val key   = scala.util.Random.alphanumeric.take(1).mkString
      val value = scala.util.Random.nextInt(10)
      ctx.log.info(s"CRDTActor-$id: Consuming operation $key -> $value")
      crdtstate = crdtstate.put(selfNode, key, value)
      ctx.log.info(s"CRDTActor-$id: CRDT state: $crdtstate")
      broadcastAndResetDeltas()
      ctx.self ! ConsumeOperation // continue consuming operations, loops sortof
      Behaviors.same

    case DeltaMsg(from, delta) =>
      ctx.log.info(s"CRDTActor-$id: Received delta from ${from.path.name}")
      // Merge the delta into the local CRDT state
      crdtstate = crdtstate.mergeDelta(delta.asInstanceOf) // do you trust me?
      Behaviors.same
  Behaviors.same
