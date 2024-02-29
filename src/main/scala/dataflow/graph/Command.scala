package dataflow.graph

import org.apache.pekko.actor.typed.ActorRef

sealed private trait FullCommand
private case class Init(actors: List[ActorRef[FullCommand]]) extends FullCommand
sealed private trait Command                                 extends FullCommand
private case class Event(from: Int, data: Any)               extends Command
private case class Border(from: Int)                         extends Command
