Run:
   ./gradlew build
   ./gradlew run


### How to trigger at least one failure (example)
**Failure to trigger:** crash the backup (or secondary) while a client request is in flight.

#### Steps
1. Start the system normally (view server / coordinator + primary + backup).
2. Start a client request (e.g., a `Put`/`Append`/write operation).
3. **Kill the backup process** before the primary gets its ack.  
4. Retry the same client operation (or let the client retry automatically).

#### Expected result
- The in-flight request may time out or be retried.
- The system should **not** commit conflicting state.
- After recovery/view change, the operation should either:
  - succeed once, or
  - return a retry/error cleanly (without corrupting the log/state).
