## Assumption hooks (KV store, crash model)

### Hook A1: Reliable delivery (TCP eventually delivers)
- Assumption: Messages between correct nodes are eventually delivered (no loss; retries hidden by TCP).
- Why we need it: Replication/ack paths must complete to make progress; otherwise writes/heartbeats can stall forever.
- Failure-first trace if false:
  1) Client PUT(k,v) → primary
  2) primary → backup: REPLICATE(k,v) (lost forever)
  3) primary waits for ack (or backup never catches up) ⇒ write never completes / failover unsafe
- What to observe:
  - outstanding replication RPCs age, retry counts, TCP resets, “in-flight > T” alerts
  - backup lag (lastApplied index / op-id gap)

### Hook A2: Bounded delay / no timeouts used
- Assumption: Message latency is bounded enough that “eventually delivered” happens soon (we aren’t modeling timeouts).
- Why we need it: Liveness expectation (requests don’t hang “forever”); leader/primary health checks are meaningless without timing.
- Failure-first trace if false:
  1) primary is alive but network delay spikes for minutes
  2) clients see indefinite hangs (since we have no timeout/failure detection) ⇒ liveness failure at the client level
- What to observe:
  - end-to-end request latency (p99/p999), time-in-state for pending requests
  - heartbeat gaps / RPC latency histograms vs any configured SLO

### Hook A3: Bounded processing time (no overload)
- Assumption: Nodes process requests in bounded time (no unbounded queueing/GC/overload).
- Why we need it: Prevents false “failure symptoms” and ensures replication/catch-up completes.
- Failure-first trace if false:
  1) backup overloaded, apply loop stalls
  2) primary continues acknowledging (or blocks) ⇒ either unsafe failover (stale backup) or write stalls
- What to observe:
  - event-loop lag / queue depth, CPU saturation, GC pause time
  - replication apply lag (seconds behind), backlog length
