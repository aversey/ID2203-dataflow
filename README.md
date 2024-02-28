# Dataflow

- Notes:
  - If back pressure is not implemented, then make sure to not produce too many
    events. Can cause large intermediate buffers of events.
- Bootstrapped project in Akka
  - Input: Actor Graph, sources -> actors -> sinks
- Workflow Graph
  - Every actor computes some random function
  - Source
    - Produce events, insert border every X ms
    - 2PC log index (same as Task, Sink)
  - Task
    - Processes events
    - implements ABS protocol
    - 2PC state (same 2PC commit as sink)
  - Sink
    - implements ABS protocol
    - 2PC produced events (same 2PC commit as Task)
- // Opt1: Coordinator
  - Could be an actor
  - Assumed to never crash: "backed by RSM"
  - Snapshotting
    - Tasks -> send "PC" -> coordinator
    - Coordinator waits for all, then -> send "commit" -> tasks
    - ...
  - Recovery
    - When task fails, coordinator notices failure, asks all to restart from
      latest snapshot.
- Opt2: Consensus / KV-Store
  - Recovery: triggered by manager
    - When task fails, all tasks are notified, and asked to restart from latest
      stable snapshot
  - Snapshotting: using consensus layer
    - Tasks write towards the consensus layer.
  - Consensus layer is assumed to not crash.
    - Implemented as a thread-safe singleton object or as an actor.
- Failure
  - System user sends "Failure<TaskID>" -> to all Tasks
    - "We are emulating the Failure Detector"
    - System may send failures concurrently for different tasks, may arrive in
      different orders
    - Tasks start recovery process upon receiving the message
- Task Interface
  - Start<>
  - // Stop<>
  - NextEvent<StreamID, SeqNR, GenerationNR, Event>
  - Failure<TaskID>
  - ConsensusReply<Reply>
- Global Variables
  - ActorGraph
  - Actors: Map[ID -> ActorRef]
  - Snapshot states
    - SeqNR
    - States
    - ...
- Roadmap / Grading
  - ABS
  - 2PC
  - Evaluation
    - Throughput
    - Latency
    - For 1 workload
  - Not-so-rigorous testing, more like a demonstration
  - Bonus Tasks
    - Testing:
      - Correctness testing
    - Evaluation:
      - comparison to processing with ABS/snapshotting disabled
      - (less important) comparison to batched processing
    - Backpressure
- https://github.com/delftdata/checkmate

```scala
Actor { def receive(x) = x match
  case ... => ...
  case ... =>
    val f = Future{ ... }
    f.onCompleted{ ... }
}
```

`onCompleted` may execute concurrently to `receive` for the same actor.

```
Src1 -> T1 -> Snk1
Src1 -> T2 -> Snk1
```

Commit:

- Log-position of sources
  - "consume index"
- States of tasks
- Output at sinks
