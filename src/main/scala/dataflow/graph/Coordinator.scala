package dataflow.graph


import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.AbstractBehavior
import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import scala.collection.mutable
import scala.runtime.stdLibPatches.language.experimental.genericNumberLiterals


sealed trait CoordinatorCommand

case class Fail() extends CoordinatorCommand

case class Precommit(g: Int, n: Int, e: Int) extends CoordinatorCommand


/* Assumed not to fail, global */
class Coordinator(
  actors: List[ActorRef[FullCommand]],
  sources: List[Source[_]],
  context: ActorContext[CoordinatorCommand]
) extends AbstractBehavior[CoordinatorCommand](context):
  var generation = 0
  val epochs     = mutable.Map[Int, Int]()
  (0 until actors.size).foreach: i =>
    epochs(i) = -1
  var gce = -1
  override def onMessage(msg: CoordinatorCommand) =
    msg match
      case msg: Fail =>
        generation += 1
        epochs.mapValuesInPlace((_, _) => gce)
        actors.foreach(_ ! Recover(generation, gce))
      case msg: Precommit =>
        if msg.g == generation then
          epochs(msg.n) = msg.e
          val newGce = epochs.map(_._2).min
          if newGce > gce then
            gce = newGce
            actors.foreach(_ ! Commit(generation, gce))
    Behaviors.same[CoordinatorCommand]
  val borderer = new Thread:
    override def run = while true do
      Thread.sleep(100)
      sources.foreach: s =>
        actors(s.id) ! Border(generation, s.outStream)
  borderer.start()
