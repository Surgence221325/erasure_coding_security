# AI interaction log (lightweight)

Keep this lightweight. The goal is traceability, not volume.

For each meaningful use of GenAI, record:
- Goal (what you were trying to decide/build)
- Prompt summary (no need to paste everything)
- Key output (the one idea/claim that mattered)
- Decision (what you adopted/rejected and why)
- Evidence link (commit/PR/experiment that resulted)
- Unknowns (what remains uncertain and how you will test it)


Goal: setup repo
Prompt Summary: Pasted instructions into GPT to provide context window, upon error (mainly with signing) asked for help deciphering bugs
Key Output: Provided command to save key locally.
Decision: N/A
Evidence Link: Very first PR (lost in git hard reset, replacing that evidence trail)
Unknowns: N/A

Goal: Understand course requirements
Prompt Summary: Having trouble understanding exactly what needs to be committed especially in the earlier weeks. (Paste course syllabus)
Key Output: Provided examples of intermediary responses I may want to leave behind as evidence trail.
Decision: N/A
Evidence: this commit
Unknowns: N/A

Goal: Week 04 Goal Move - Use GenAI to enumerate the hidden synchrony/failure-detector assumptions in a non-blocking commit story.
Prompt Summary:  Pasted the question into GPT. Asked followups.
Key Output: Understanding of 3PC commit advantage, and assumptions.
Decision: N/A
Evidence Link: this commit
Unknowns: N/A

Goal: Design project 1 discussion. Specifically discussions about protocol surrounding availability and avoiding constant ViewServer pings.
Prompt Summary:  One concern I had regarding the new design is that what if both the primary and backup views are stale so we mistakenly return
a positive response to a client? How can we manage perfect information in this system, or a consistency between the primary and backup when we don't know
we are wrong and the client doesn't know either.
Key Output: As long as the primary and backup are consistent this is not a concern. In the worst case we will have to retry the operation. It will not corrupt
our log.
Decision: Did not need to include ViewServer interaction in every single request.
Evidence Link: this commit (check new DP1 PDF)
Unknowns: Not sure how to prove this, although it seems intuitively right.

Goal: Implement DP1
Prompt Summary: I gave the AI (ChatGPT) context on the problem, and my design choices. I then gave it rough pseudocode
and asked it to implement the pseudocode into actual Java code. On errors I helped it parse the actual issue and help me iterate.
Key Output: Somewhat functioning DP1 assignment
Decision: Included its suggestions, but it made debugging way harder and made it so I was unable to complete it 100% (one error).
Unknowns: How to fix client desync in DP1.

Goal: Implement DP2
Prompt Summary: I gave the AI (ChatGPT) context on the problem, and my design choices. This time I was much more iterative. Generally,
I would ask the AI to help me "plan" out my tasks at a high level. And then iteratively work through them line by line.
Decision: Debugging was a lot easier this time, but I still noticed some level of hallucination and some inefficiency I didn't originally consider.
Unknowns: How to fix time out errors.

Goal: Capstone Design
Prompt Summary: I was given an idea by the Professor on how to "spice up" a distributed key value store. I chose to use erasure coding to ensure confidentiality of stored objects. ChatGPT was helpful in iteratively helping me make decisions about the design and understand intricacies regarding this concept I was unaware of.
Decision: (k, m) erasure coding scheme, and key-share threshold encryption as core of distributed decision
Unknowns: alternatives to this scheme? any ways to improve latency/tradeoffs.

Goal: Implement capstone source code
Prompt Summary: Gave AI (Claude) the design doc and dslabs framework context. It helped implement CoordinatorNode, RegionalNode, CapstoneClient — translating the protocol design into dslabs framework message handlers.
Key Output: Working implementation of write path (encrypt → erasure code → Shamir split → distribute) and read path (gather → verify → reconstruct → decrypt).
Decision: Adopted the implementation, iteratively fixed bugs discovered through testing.
Evidence Link: capstone/project/lab/src/capstone/
Unknowns: Whether the protocol was correct under all message orderings (later verified by search tests).

Goal: Build test suite modeled on dp2 PaxosTest
Prompt Summary: Used dp2's PaxosTest.java as reference. AI helped structure tests across categories (correctness, fault tolerance, multi-client, auth, search). Discussed which Paxos patterns apply to our system and which don't.
Key Output: 36 tests across 9 categories including deterministic BFS/DFS search tests.
Decision: Adopted run + search test dual approach. Search tests use smaller config (k=1,m=1) for tractable state space.
Evidence Link: capstone/project/lab/tst/dslabs.capstone/CapstoneTest.java
Unknowns: Whether search test configuration is representative enough of real config.

Goal: Add client authentication
Prompt Summary: Discovered mid-implementation that any node could read any key — no auth existed. Discussed options (pre-shared token, challenge-response, per-request HMAC) with AI. Chose challenge-response HMAC with session tokens.
Key Output: HMAC-SHA256 handshake where shared secret never travels over the wire. Per-key ownership (first writer wins).
Decision: Adopted challenge-response + ownership. This was a reactive discovery, not planned — the design doc said "clients are untrusted" but we didn't implement auth until we saw the vulnerability.
Evidence Link: Auth messages in Messages.java, handlers in CoordinatorNode/CapstoneClient
Unknowns: No token expiry mechanism. Permanent ownership means decommissioned clients lock their keys.

Goal: Add deterministic search tests (BFS/DFS model checking)
Prompt Summary: AI helped adapt BFS/DFS patterns from PaxosTest. Discovered that heartbeat timers cause infinite state expansion in BFS. Found auth nonce race condition only visible under exhaustive ordering exploration.
Key Output: Nonce race bug — auth retry overwrites pending nonce, invalidating in-flight HMAC. Fixed by making handleAuthRequest idempotent.
Decision: Disabled timers in search, used smaller config, added proactive re-send on auth completion. The nonce race was the strongest "system pushed back" moment.
Evidence Link: Search tests (test26-30), auth nonce fix in CoordinatorNode
Unknowns: Whether timer-disabled search tests miss timer-dependent bugs.

Goal: Implement dynamic region membership
Prompt Summary: Discussed design extensively with AI — whether to change k or m on join, how old keys interact, how to coordinate the join without blocking forever. Evaluated alternatives (re-encoding old data, region removal, concurrent joins).
Key Output: Add-only join protocol with reconfiguration pause, per-version metadata for backward compatibility, fire-and-forget approach.
Decision: k stays fixed, m increases. Old keys keep original encoding. No re-encoding, no removal. Reconfiguration timeout (2s) prevents indefinite write rejection.
Evidence Link: JoinRequest handling in CoordinatorNode, dynamic membership tests (test22-25)
Unknowns: Whether 2s reconfig timeout is sufficient for production. Per-key encoding creates long-tail maintenance burden.

Goal: Implement garbage collection
Prompt Summary: Discussed GC approaches with AI — confirmed deletes (blocks on downed regions), region-initiated reconciliation (complex), heartbeat piggyback (slow scan), fire-and-forget (simple, harmless orphans). Chose fire-and-forget.
Key Output: On commit, delete old versions locally + send DeleteVersionData to regions. On write timeout, delete uncommitted version. Orphaned region data is unreachable without coordinator metadata.
Decision: Fire-and-forget. Production would add periodic reconciliation. Verified via getVersionCount() — 20 overwrites → 1 version retained.
Evidence Link: GC in CoordinatorNode (gcOldVersions, gcUncommittedVersion), tests 33-36
Unknowns: Orphaned fragments on regions that miss delete messages. Harmless but wastes storage.

Goal: Implement Raft coordinator replication
Prompt Summary: Extensive design discussion with AI (Claude) — evaluated Paxos vs Raft, what state to replicate (state deltas not commands), strict vs relaxed reads, log compaction. Planned 10-phase iterative implementation to avoid "getting lost in the sauce." Each phase ended with a compile+test checkpoint ensuring existing 36 tests still passed.
Key Output: Full Raft implementation: leader election (deterministic stagger), log replication (AppendEntries/RequestVote), strict read verification (leadership confirmation before serving reads), log compaction (snapshot + InstallSnapshot), auth/ownership/GC all through Raft log. 4 new bugs found and fixed during implementation.
Decision: State deltas in Raft log (not commands — followers can't re-execute writes with different AES keys). Leader-only auth (prevents unreplicable sessions). Two-phase write timeout (region 300ms + Raft 500ms). Client knows all coordinators (round-robin on auth failure). Fixed coordinator set (no Raft membership changes).
Evidence Link: Raft code in CoordinatorNode, LogEntry.java, StateDelta.java, Raft messages/timers, tests 37-45
Unknowns: DFS search test (test45) found the two-phase timeout interaction (Bug 10) — fixed, but raises question of what other timing interactions exist. Deterministic election stagger may not be sufficient under adversarial scheduling. Log compaction not tested under heavy load. Coordinator set fixed at startup (joint consensus for membership changes is too complex).