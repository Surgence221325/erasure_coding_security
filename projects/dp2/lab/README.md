# Paxos
*Adapted from the [MIT 6.824
Labs](http://nil.csail.mit.edu/6.824/2015/labs/lab-3.html)*


## Introduction
Lab 3 depends on a single master view server to pick the primary. If the view
server is not available (crashes or has network problems), then your service
won't work, even if both primary and backup are available. It also has the less
critical defect that it copes with a server (primary or backup) that's briefly
unavailable (e.g. due to a lost packet) by either blocking or declaring it dead;
the latter is very expensive because it requires a complete state transfer.

We are going to enhance the consistency property by increasing the number of
servers (>= 3). How to ensure the ordering consistency of client requests in all
participates would be a critical issue in a distributed system without a global
clock or sharing memory. We are expecting the system will fight against network
failures such as partitions and message loss and won't halt as long as there are
a majority of servers working.

You need to read papers (2 in particular) and follow their ideas to implement
your PAXOS in this one. The original paper for PAXOS, [The Part-Time
Parliament](https://courses.cs.washington.edu/courses/csep590/04wi/papers/lamport-part-time-parliament.pdf),
proposed by Leslie Lamport in the 1990s, is somehow hard to understand and there
is, in general, a huge gap between the implementation and illustration (while
there is little doubt around the efficiency of PAXOS, due to its proved least
rounds of messaging). This is also the reason that lots of industrial products
such as Kubernetes and Redis would turn their back to PAXOS and choose Raft.
However, some enhanced papers with clearer illustrations come out as time goes
by, and we find them much easier for students to follow. Once you understanding
these papers, you will find their ideas are the same as the original ones. As
for add-ons, we will ask you to implement more features than merely PAXOS, i.e.
stable leaders and garbage collection. We will discuss them in detail in the
following sections. There are also some optimizations you can perform, such as
request batching, in your PAXOS implementation.


Although PAXOS is a protocol for several servers, we will also test your implementation for the single server running. Your implementation should work under different total server numbers (1, 3, and 5 in specific).

As for our final project (Project 5), which is built upon your PAXOS, will share commons with the Google Spanner (a shard KV store with strong consistency), while there are extra features in Google Spanner, such as SQL supports, atomic clocks, and MVCC-like policy. We leave their details to Project 5.

We are refreshing you with some assumptions that apply to all projects, and you may find them helpful for your PAXOS implementation.
1. In-order request generation for a single client: a client will only move on if and only if the previous request is fulfilled and receive a proper response from servers.
2. Non-in-order delivery for message passage: not FIFO and you may not make any strong assumption about the delivery order.
3. No assumption about ultimate delivery: message might be lost due to network failures.

We have provided you a sketch of the overall design in source files. The tests for this project are even more demanding. You have to think hard to design and finish your implementation in a flawless way. Along with the implementation, we will guide you with some illustrations about several tests to help you understand the test logic and prepare you for the last great challenge in Project 4, where your implemented PAXOS will be used to provide fault-tolerance and request ordering service for a more complex application.

Before you start, please make sure your implementation for the exactly-one application and KV store is correct (`project2: dslabs.kvstore` and `project2: dslabs.atmostonce`). Your implementation for Project 4 will be built upon the Project 2 source. **You should copy your implementation of Project 2 and replace it with the corresponding position of Project 4 src.** Also, we suggest you read through the questions from previous projects, especially Project 1 and 2. There are some questions applied to all projects, and you might find them helpful.

1. [Project Setup](#project-setup)
2. [PAXOS Implementation](#paxos-implementation)
   1. [Stable Leaders](#stable-leaders)
   2. [Garbage Collection](#garbage-collection)
   3. [PAXOS Interface Methods](#paxos-interface-methods)
3. [Project 5 Preview](#project-5-preview)
4. [Hints](#hints)
4. [Submission](#submission)
5. [Project Question List](#project-question-list)
6. [Recommend Reading List](#recommend-reading-list)

## Project Setup
We assume you have already configured your development environment properly. If not, please refer to the `environment` repo in our organization.

You will first clone the project repo from our organization. Please use the following command for cloning from our organization repo.
You should replace `account` with your github.students.cs.ubc.ca name, `course-organization` to the
current term organization, and `project-repo` to the project repo name.

```shell script
git clone https://<account>@github.students.cs.ubc.ca/<course-organization>/<project-repo.git>
```

The project repos are private and we only grant access to enrolled students. If you have enrolled but cannot clone project repos (in general, if you can see this repo, you should be able to clone it), please contact the course TAs for addressing the issues. We only allow read and clone permissions for students without push. Please initial your repo inside your GitHub account and update this repo to it for further usage. We have provided detailed commands to clone and create copy inside your private GitHub in the `environment` repo under the same organization.

After cloning the project repo, open the project directory with IDE (IntelliJ). Also, open a terminal and enter the project directory. Enter ``make`` for the crash will automatically download all dependencies and you should no longer see import reference issues in IDE. In case of build failure shows as below, `sudo chmod +x gradlew` should fix your problem.

<div style="text-align:center;"><img src="/lab/pic/makeerror.png"/></div>

## PAXOS
We will follow [Paxos Made Moderately Complex](http://www.cs.cornell.edu/courses/cs7412/2011sp/paxos.pdf)
for the PAXOS implementation. Before starting, you should also read
[Paxos Made Simple](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf)
for the general ideas of PAXOS.

Your system will consist of `PaxosServer`s and `PaxosClient`s. The clients send
`PaxosRequest`s to the servers, and servers respond with `PaxosReply`s. Each
node has the list of servers in the system. You are only provided the
aforementioned messages as well as the `ClientTimer`; you will have to define
the rest of the messages and timers your implementation uses.

Your system should guarantee *linearizability* of clients' commands. That is,
from the perspective of the callers of clients' functions and the results they
see, your implementation should be indistinguishable from a single, correct
entity processing clients' commands in sequence (e.g., your `SimpleServer` from
lab 1). Furthermore, your implementation should be able to process incoming
commands and return results as long as a majority of servers can communicate
with each other with "reasonable" message delay (and as long as the client can
send a command to these servers). This means that it should be robust to dropped
messages, as long as network connectivity is eventually restored.

You should achieve this by implementing the multi-instance Paxos algorithm.
Multi-instance Paxos can be viewed as a way for servers to agree on a shared log
of clients' commands. Agreement for each "slot" in the log is reached separately
by a different instance of Paxos, and servers can then play the log forward in
order, executing commands on their own local copy of the application, as long as
each command they execute is "stable" (i.e., agreement has been reached for that
command and all preceding commands). As long as no two servers ever decide
different values for a log slot (and as long as the underlying application is
deterministic), this approach will guarantee linearizability.

You *do not* need to implement server recovery. You should assume that servers
only fail by permanently crashing (or by being temporarily unreachable over the
network). You will, however, implement log compaction, a garbage collection
mechanism commonly used in practice to prevent the shared log from growing
without end and exhausting the memory on servers.

Your Paxos-based replicated state machine will have some limitations that would
need to be fixed in order for it to be a serious system. It won't cope with
crashes, since it stores neither the key/value database nor the Paxos state on
disk. Also, It requires the set of servers to be fixed, so one cannot replace
old servers. These problems can be fixed.

You might find the Paxos lecture notes, [Paxos Made
Simple](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf), and [Paxos
Made Moderately
Complex](http://www.cs.cornell.edu/courses/cs7412/2011sp/paxos.pdf), and
[Viewstamped Replication](https://dl.acm.org/citation.cfm?id=62549) useful.

The Paxos Made Moderately Complex (PMMC) protocol divides the nodes into different
"roles" (e.g., replica, acceptor, leader), while your implementation only has one
role, `PaxosServer`. It will play all of the aforementioned roles simultaneously.
This could be done by keeping all of the states for each sub-role
entirely separate, but you'll find that there are opportunities for optimization
on the naive approach. You should not try to "spawn" scouts or commanders;
instead, simply keep the necessary state on your `PaxosServer`.


### Stable Leaders
In addition to the base PMMC protocol, as Google Spanner, we are expecting a mechanism
for checking and maintaining a stable leader (sometimes called the distinguished proposer).
When a leader is preempted (receives a ballot larger than its own), instead of immediately
starting phase 1 of PAXOS (spawning a scout) again, it should transition to
"follower mode" and stay inactive.

Every `PaxosServer` should have a `HeartbeatCheckTimer` which ticks, firing
periodically, exactly like the `PingCheckTimer` in lab 3. While in follower
mode, if a node sees two of these timers in a row without receiving a message
from the node it thinks is the active leader (the one with the largest ballot
it's seen), it should then attempt to become active and start phase 1 of Paxos.

To prevent itself from being preempted unnecessarily, while an active leader, a
`PaxosServer` should periodically broadcast `Heartbeat` messages to the other
nodes. Simply have another timer which fires periodically.

While you can use the AIMD (additive increase, multiplicative decrease) like the
one described in section 3 of the PMMC paper, we recommend keeping the timer
lengths fixed for simplicity. The timer lengths from lab 3 are a good starting
point, but you are free to tune them as you see fit.

One important note: this approach to leader election should only be seen as a
performance optimization. There could still be nodes which simultaneously
believe themselves to be an active leader – who believe their ballot is
acceptable to a majority. The Paxos protocol ensures safety in this case.


## Garbage Collection
A long-running Paxos deployment must forget about log slots that are no longer
needed and free the memory storing information about those slots. While PMMC
proposes one mechanism for garbage collection, for this lab we will do something
slightly simpler.

For our purposes, we will say that a command in a log slot is needed if it has
not been processed on all servers; it is not okay to delete commands as soon as
they are processed on the local server. In order to implement this log
compaction, each server will have to inform the other servers about the latest
point in its own log that is "stable." The easiest approach is to piggyback this
information on the periodic heartbeat protocol. Each follower server responds to
heartbeats with a message containing the latest slot they have executed
(`slot_out` in PMMC terms). Then, the active leader should be able to figure out
the latest log slot which has been executed on all nodes; it can then include
that information in subsequent heartbeats. Once a node learns that all nodes
have executed all slots up to some slot `i`, it can then safely discard all
information for slots less than or equal to `i`.

If one of your servers falls behind (i.e. does not receive the decision for some
instance), it will later need to find out what (if anything) was agreed to. A
simple way to bring a follower node up to date is by having the active leader
send it missing decisions when the follower sends its latest executed slot in
the above protocol.

Your garbage collection mechanism should be able to free memory from old log
slots when all Paxos servers can communicate with each other; it does not need
to make progress when only a majority can communicate. This is one weakness of
the simplified approach we describe. It is possible to do garbage collection of
slots that only a majority have executed and discovered the values for. In that
case, however, bringing lagging nodes up-to-date requires a complete *state
transfer*, which can get tricky. Additionally, doing state transfer is not as
modular; you will see what we mean by that when you use Paxos as a part of a
larger protocol in lab 5.


## Paxos Interface Methods
In order for the tests to more efficiently check your implementation, you'll
need to implement four methods in your `PaxosServer`: `status(logSlotNum)`,
`command(logSlotNum)`, `firstNonCleared()`, and `lastNonEmpty()`. These methods
simply return information about the *local state* of a server. Implementing
them should be straightforward, but be sure to pay attention their requirements
and implement them correctly.

We have provided you the implementation of ballot and pvalue. The `PaxosRequest`,
`PaxosReply`, and `PaxosDecision` are also ready to use. They are shortly discussed
in following section about future Project 5.

---


Our solution took approximately 400 lines of code.

You should pass the lab 4 tests; execute them with `./run-tests.py --lab 4`.


## Hints
* Your system should be able to reach agreement on different commands concurrently
  (not one-by-one as Project 3).
* Nodes should never send messages to themselves. You may wish to implement your own utility function on `PaxosServer` for sending a message to all servers
  while bypassing the network when sending to itself. **Also, your implementation
  should work correctly with the server size as 1.**
* Once a `PaxosServer` proposes a value in a slot, it should make sure that as
  long as it is not preempted by another server and can communicate with a
  quorum, that value is eventually decided in that slot.
* You may want to add some duplication-checking mechanisms to your PAXOS
  implementation, rather than using AMOApplication (there is no Application for
  Project 5).
* The easiest way for a client to send requests to the system is by broadcasting
  them to all servers.
* Your implementation needs to be able to handle "holes" in the PAXOS log. That is, when completing the first phase of PAXOS, a server might see previously
  accepted values for a slot but not previous slots. Your implementation should still make progress in this case.
* Figure out the minimum number of messages PAXOS should use when reaching
  agreement in non-failure cases and make your implementation use that minimum.
* You should always check whether a message is outdated before recording it to your
  PAXOS states. The Garbage Collection would fail if you accepting some outdated messages after the corresponding slot is garbage collected and forgetting their presence.
* The search tests in this project are rather minimal because the state space for most implementations of PAXOS explodes quickly. You should take their passing
  with a grain of salt.
* During phase 1 of PMMC, your leader should be able to collect pvalues (from all acceptors) for all non-garbage-collected slots, which is an invariant for your
  PAXOS implementation. You can use this fact for some correctness checks.
* One benefit of colocating all PMMC roles in the `PaxosServer` is that your
  "acceptor" knows both the accepted and decided values; you need not ever store
  more than one value for each slot on each node. You can then include both the
  accepted and decided values in P1B messages, speeding up the leader election
  process.
* You might find the `Multimap` classes from Guava useful for storing messages for each slot.

## Submission
Project 4 requires a fair amount of code along with huge efforts for a correct
implementation. It is a difficult one and we will use this service for
fault-tolerance and request ordering in the final sharded KV store. We have
provided the programming assignment in GradeScope. This assignment will run all
test cases for PAXOS. We use one zip file to test your implementation in
GradeScope. You should also write a simple report. The report rubrics are
already available in Piazza. We have provided you the general structure in
`REPORT.md`.

For submission, you should submit both your implementation and report. As for
report, fill the content in  `REPORT.md`. A `submit.sh` under `lab` is ready to
use. Run that script as follows and enter your gtAccount. A zip file with the
name `student.zip` is generated. You can override the name by providing an extra
argument, e.g., `submit.sh fsgeek` generates a file called `fsgeek.zip`. The zip file
should contain your implementation source code and `REPORT.md`. Submit the `zip`
file to the corresponding project in GradeScope. Then, you are done! We will use
your last submission for project grading, and we reserve the right to re-run the
autograder. GradeScope is set to use 4 CPUs with 6GB of memory.

You can submit your report to Gradescope as well, using the corresponding report
submission.  However, you do not need to zip it up - just submit a file ending
with `.pdf`. If it happens to _be_ a PDF file, we will grade it and give you
credit for it.```

***Note**: Make sure you do not include **print** or **log** statements in your implementation. Also, do not include or zip extra files for your submissions. We will check the completeness and validity of submission before grading.
If your submission fails to satisfy the submission requirement or could not compile, you will see feedback from GradeScope indicating that and receive 0 for that submission.*



## Project Question List

***

- When should we update the PAXOS log for the non-leader servers?
	
	There is little doubt for the leader server since it is proposing, i.e. the order in its PAXOS log should be determined already. Recall that we are combining three roles into one server where each role can directly see the states of others. While we are still recommending you to follow a modular design with some level of separations for the variables and methods used by different roles, the PAXOS log should be something you can share safely between roles. There are different phases in our recommended reading for implementation. As for acceptors in non-leader servers, once new proposals are reaching, you can update the PAXOS log with certain condition checking, rather than waiting until the decision phase.

***

- The search tests reach timeout, but the expected states are not showing up (probably test 22 and 23). What should I do?
	
	The first thing that we recommend you to do is to check the liveness property of your implementation using the visualization tools integrated in the project framework (make sure you have Chrome installed). As starting, you should first figure out an event path (messages and timers) to make the expected state happen and visualize that path using the tool to validate your implementation. The second thing would be to check the length of the path. If the length is too long (you may add too many messages and timers, which increases the state space), the test won't be able to find that state due to the time limit. You should then try to simplify your implementation by either combining some messages and timers or thinking of some invariants to speed up the progress in your design. As for invariants, we are referring to something that is definitely happening in the PAXOS phase without the need for any message and timer.
	
***

- How to decide if a node is a replica (or) a leader (or) a acceptor?
	
	It is all in one. The all-in-one design can use the servers in an efficient one, comparing with x3 illustrated in the paper. Also, they are just logical roles rather than really have some hard boundaries. There is one more important reason from my view is because of the testing ability.

***

- Why is the server list array being used in the `PaxosClient` AND in the `PaxosServer`?
	
	Clients broadcast requests to all servers using this list. `PaxosServers` need to know about each other so they can send messages to one another and implement Paxos.

***

- What is the purpose or usage of `Ballot` type?
	
	`Ballot` is sent when a node is trying to become the leader and be active. This is used for leader elections. The replica with the highest ballot will be elected as the active leader. You can try to understand this by combining it with the pseudocode for leader phase 1 (scout) in PMMC.
	
	Before understanding PMMC or PAXOS, you should first understand why PAXOS can reach consensus and how the two-phase commit with (>=f/2 + 1) constraint helps with the global consistency when there are server failures.

***

- Do we need to implement the "WINDOW" from PMMC such that `slot_in < slot_out + WINDOW` or is this something we can primarily ignore for our project?
	
	You need to first understand what is the window for. The window is to limit the gap between the `slot_in` and `slot_out`. It is not necessary to implement the window in our project.



## Recommend Reading List

- [Paxos](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf)
- [Paxos Implementation](https://www.cs.cornell.edu/courses/cs7412/2011sp/paxos.pdf)
- [Viewstamped Replication](https://dl.acm.org/doi/10.1145/62546.62549)
- [Raft](https://web.stanford.edu/~ouster/cgi-bin/papers/raft-atc14)
- [Zookeeper](https://www.usenix.org/legacy/events/atc10/tech/full_papers/Hunt.pdf)
- [Raft Lecture](https://www.youtube.com/watch?v=64Zp3tzNbpE)
- [Two Phase Commit](https://en.wikipedia.org/wiki/Two-phase_commit_protocol)
