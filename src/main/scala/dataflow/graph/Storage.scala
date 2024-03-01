package dataflow.graph


import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.AbstractBehavior
import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import scala.collection.mutable


sealed trait StorageCommand

case class Write(g: Int, n: Int, e: Int, s: Any) extends StorageCommand

case class Fail() extends StorageCommand


class Storage(
  actors: List[ActorRef[FullCommand]],
  initials: List[Any],
  context: ActorContext[StorageCommand]
) extends AbstractBehavior[StorageCommand](context):
  val states = mutable.Map[Int, mutable.Map[Int, Any]]()
  actors.indices.foreach: i =>
    states(i) = mutable.Map[Int, Any](0 -> initials(i))
  var generation = 0
  var gce        = 0
  override def onMessage(msg: StorageCommand) =
    msg match
      case msg: Write =>
        if msg.g == generation then
          states(msg.n) += (msg.e -> msg.s)
          val newGce = states.map(_._2.maxBy(_._1)._1).min
          if newGce > gce then
            gce = newGce
            states.foreach: (n, a) =>
              actors(n) ! Commit(generation, gce)
              a.filterInPlace((e, _) => e >= gce)
      case msg: Fail =>
        generation += 1
        states.mapValuesInPlace: (n, a) =>
          actors(n) ! Recover(generation, gce, a(gce))
          mutable.Map(gce -> a(gce))
    Behaviors.same[StorageCommand]
