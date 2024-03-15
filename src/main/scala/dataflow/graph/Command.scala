package dataflow.graph

import org.apache.pekko.actor.typed.ActorRef

sealed private trait FullCommand


private case class Init(
  actors: List[ActorRef[FullCommand]],
  storage: ActorRef[StorageCommand],
  coordinator: ActorRef[CoordinatorCommand]
) extends FullCommand


private case class Credit(g: Int, from: Int, amount: Int) extends FullCommand

private case class Recover(g: Int, e: Int) extends FullCommand


sealed private trait Command extends FullCommand:
  def g: Int


private case class Event(g: Int, from: Int, data: Any) extends Command
private case class Border(g: Int, from: Int)           extends Command
private case class Commit(g: Int, e: Int)              extends Command
