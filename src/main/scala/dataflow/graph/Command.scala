package dataflow.graph

import org.apache.pekko.actor.typed.ActorRef

sealed private trait Command
private case class Start(actors: List[ActorRef[Command]]) extends Command
private case class Event(from: Int, data: Any)            extends Command
private case class Border(from: Int)                      extends Command
