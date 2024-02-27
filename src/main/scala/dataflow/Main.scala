package dataflow


import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.scaladsl.adapter.*


def main(args: Array[String]): Unit =
  val N_ACTORS = 8

  val system = ActorSystem("CRDTActor")

  // Create the actors
  val actors = (0 until N_ACTORS)
    .map: i =>
      val name = s"CRDTActor-$i"
      val actorRef = system.spawn(
        Behaviors.setup[CRDTActor.Command] { ctx => new CRDTActor(i, ctx) },
        name)
      i -> actorRef
    .toMap

  // Start the actors
  actors.foreach((_, actorRef) => actorRef ! CRDTActor.Start(actors))

  // Sleep for a few seconds, then quit :)
  Thread.sleep(5000)

  // Force quit
  System.exit(0)
