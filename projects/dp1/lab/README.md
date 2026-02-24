# Lab 3: Primary-Backup Service
*Adapted from the [MIT 6.824
Labs](http://nil.csail.mit.edu/6.824/2015/labs/lab-2.html)*


## Introduction
In the previous lab, you guaranteed exactly-once execution of RPCs despite
unreliable network conditions. Your server maintained state, but it was not
fault-tolerant. Lab 3 is a first step towards fault tolerance for services with
state.


### Road Map for Labs 3-5
In the next 3 labs you will build several systems for replicating stateful
services. The labs differ in the degree of fault tolerance and performance they
provide:

* Lab 3 uses primary/backup replication, assisted by a view service that decides
  which machines are alive. The view service allows the primary/backup service
  to work correctly in the presence of network partitions. The view service
  itself is not replicated and is a single point of failure.
* Lab 4 uses the Paxos protocol for replication with no single point of failure
  and handles network partitions correctly. This service is slower than a
  non-replicated server would be, but is fault tolerant.
* Lab 5 is a sharded, transactional key/value database, where each shard
  replicates its state using Paxos. This key/value service can perform
  operations in parallel on different shards, allowing it to support
  applications that can put a high load on a storage system. Lab 4 has a
  replicated configuration service, which tells the shards for what key range
  they are responsible. It can change the assignment of keys to shards, for
  example, in response to changing load. It also supports transactions, allowing
  atomic update of keys held on different shards. Lab 4 has the core of a
  real-world design for thousands of servers.

In each lab you will have to do substantial design. We give you a sketch of the
overall design, but you will have to flesh it out and nail down a complete
protocol. The tests explore your protocol's handling of failure scenarios as
well as general correctness and basic performance. You may need to re-design
(and thus re-implement) in light of problems exposed by the tests, or even by
future labs; careful thought and planning may help you avoid too many re-design
cycles. We don't give you a description of the test cases (other than the code);
in the real world, you would have to come up with them yourself.

## Project Setup
We assume you have already configured your virtual machine with Ubuntu 22.04,
downloaded Java 17, installed git, and have an IDE for your implementation.
If not, please refer to the `environment` repo in our organization.

You will first clone the project repo from our organization. Please use the
following command for cloning from our organization repo. You should replace
`account` with your unique name or identifier, `course-organization` to the
current term organization, and `project-repo` to the project repo name. If you
have properly configured SSH, you won't need to authenticate.  Otherwise, you
will be asked to enter a password for access. The password is the same as your
UBC CWL account.

```shell script
git clone git@github.students.cs.ubc.ca:<course-organization>/project2-client-server.git
```

The project repos are private and we only grant access to enrolled students. If
you have enrolled but cannot clone project repos (in general, if you can see
this repo, you should be able to clone it), please contact the course TAs for
addressing the issues. We only allow read and clone permissions for students
without push. Please initial your repo inside your GitHub account and update
this repo to it for further usage. We have provided detailed commands to clone
and create copy inside your private GitHub in the `environment` repo
under the same organization.

After cloning the project repo, open the project directory with IDE (IntelliJ).
Also, open a terminal and enter the project directory. Enter ``make`` for the
crash will automatically download all dependencies and you should no longer see
import reference issues in IDE. In case of build failure shows as below, `sudo
chmod +x gradlew` should fix your problem.

<div style="text-align:center;"><img src="/lab/pic/makeerror.png"/></div>

## Primary-Backup Overview
As illustrated in the intro, we will implement a fault-tolerant service using a
form of primary/backup replication. We assume there are a group of servers
(>=2). The general illustration of primary/backup is simple. Among that group of
servers, one of them serves as the primary, and one of the serves as the backup.
The backup can be empty if there is no other server available. Both servers and
clients can get the knowledge about which server is the primary and which is the
backup, while the knowledge might be outdated for individuals. There are two
failure cases, i.e. primary failure and backup failure (our tests ensure at
least one server is available for client requests, while the message loss might
create a fake view that both the primary and backup have lost contacts
temporarily). The primary one is determined as 'dead' (lose connection,
encounter fault, etc.) for the first case. Then, the backup one will be prompted
to the primary and some other idle servers can start preparing to become the new
backup. For the second case, the backup is determined as 'dead'. The primary
will receive this update and a new backup will be selected from the available
servers. The primary will then transfer the up-to-date application to the new
backup.

It seems easy. Humm ... But problems come. First, who will determine the death
of a server? Also, what are the criteria for death in a distributed setting?
Recalls that each node runs independently and there is no way for the other
nodes to access the state of one node except messages. It is hard to tell the
differences between failures and running slow in a short period of time
[Distributed Snapshots](https://dl.acm.org/doi/pdf/10.1145/214451.214456). Since
there is no reliable failure detector that other servers can access, there is no
way to distinguish between a failed server and one which is temporarily
unavailable.

In order to ensure that all parties (clients and servers) agree on
which server is the primary, and which is the backup, a master server is added,
called the `ViewServer`. The `ViewServer` monitors whether each
available server is alive or dead. If the current primary or backup becomes
dead, the `ViewServer` selects a server to replace it. A client checks with the
`ViewServer` to find the current primary. The servers cooperate with the
`ViewServer` to ensure that at most one primary is active at a time.

Your service should allow the replacement of failed servers. If the primary
fails (the `ViewServer` thinks it fails), the `ViewServer` will take the
responsibility to promote the backup to be primary. If the backup is thought
failed or is promoted, the `ViewServer` will select an idle server to be the new
backup. The primary will send its complete application state to the new backup,
and then send subsequent operations to the backup to ensure that the backup's
application remains identical to the primary's.

To sync up with the backup, the primary must send read operations (`Get`)
as well as write operations (`Puts`/`Appends`) to the backup (if there is one),
and must wait for the backup to reply before responding to the client. This
mechanism prevent two servers from acting as primary (a "split brain"). An
example: S1 is the primary and S2 is the backup. The `ViewServer` decides that
S1 is dead (while S1 remains alive), and promotes S2 to be primary. If a client
still has the outdated knowledge with S1 as the primary and sends it an operation,
S1 will forward the operation to S2, and S2 will reply with an error indicating
that it is no longer the backup (assuming S2 obtained the new view from the
`ViewServer`). S1 can then return an error to the client indicating that S1
might no longer be the primary (reasoning that, since S2 rejected the operation,
a new view must have been formed); the client can then ask the `ViewServer` for
the correct primary (S2) and send it the operation.

Servers fail by crashing. That is, they can fail permanently but not restart.
However, other servers (including the `ViewServer`) do not have access to a
reliable failure detector. There is no way to distinguish between a failed
server and one which is temporarily unavailable.

The design outlined in the lab has some fault-tolerance and performance
limitations:

* The `ViewServer` is vulnerable to failures since it's not replicated, while we
  ignore this for the expected implementation and assume it as a reliable service.
* The primary and backup must process operations one at a time, limiting their
  performance.
* A recovering server must copy the complete application state from the primary,
  which will be slow, even if the recovering server has an almost-up-to-date
  copy of the data already (e.g. only missed a few minutes of updates while its
  the network connection was temporarily broken). **Recalls that we have implemented
  the deep copy constructor in the previous project. You will need those interface
  for replicating the up-to-date application.**
* The servers don't store the application data on disk, so they can't survive
  simultaneous crashes.
* If a temporary problem prevents primary to backup communication (inner network partition),
  the system has only two remedies: change the view to eliminate the backup (we assume the `ViewServer`
  can get the same knowledge that there is a communication gap), or keep
  trying; neither performs well if such problems are frequent.
* If a primary fails before acknowledging the view in which it is primary, the
  `ViewServer` cannot make progress---it will spin forever and not perform a
  view change. This will be explained in more detail in part 1.

We will address these limitations in later labs by using better designs and
protocols. This lab will make you understand what the tricky issues are so that
you can design better design/protocols. Also, parts of this lab's design (e.g.,
a separate `ViewServer`) are uncommon in practice.

We will address these limitations in later projects by using better designs and
protocols (PAXOS). This project will make you understand what the tricky issues in fault-tolerance with consistency requirements are so that you can design better design/protocols. Also, in practice, a separate `ViewServer` is uncommon and vulnerable to real-world failures.

The primary/backup scheme in this project is not based on any published protocol,
while it shares some common with published ones. In fact, we do not specify a
complete protocol and there are still lots of design decisions you would make
during your implementation. As for references, the protocol has similarities
with
[Fault Tolerance VM](https://www.vmware.com/techpapers/2010/the-design-and-evaluation-of-a-practical-system-fo-10134.html)
and [Viewstamped Replication](http://pmg.csail.mit.edu/papers/vr-revisited.pdf),
where a detailed treatment of high-performance primary/backup and reconstruction
of system state after various kinds of failures is available. You may also find
[Facebook Memecache](https://www.usenix.org/conference/nsdi13/technical-sessions/presentation/nishtala)
have some in common with the primary/backup service.

***

There are extra references listed in the original dslabs and we quote them as follows.
We leave them for your reference if you wish to learn more.

1. [Flat Datacenter Storage](https://www.usenix.org/system/files/conference/osdi12/osdi12-final-75.pdf).
The `ViewServer` is like FDS's metadata server and the primary/backup servers
are like FDS's tractservers), though FDS pays far more attention to performance.
It's also a bit like a MongoDB replica set (though MongoDB selects the leader
with a Paxos-like election).

2. A (different) primary-backup-like protocol is in [Chain Replication](http://www.cs.cornell.edu/home/rvr/papers/osdi04.pdf).
Chain Replication has higher performance than this lab's design, though it assumes
that the `ViewServer` never declares a server dead when it is merely partitioned.

## Part 1: View Server
Now we discuss the details about `ViewServer`. You will finish `ViewServer.java`
and pass part 1 tests. The `ViewServer` won't itself be replicated, so it will
be relatively straightforward. Part 2 is much harder than part 1, because the
primary-backup service is replicated, and you have to flesh out the replication
protocol.

The `ViewServer` goes through a sequence of numbered views, each with a primary
and (if possible) a backup. A view consists of a view number and the identity of
the view's primary and backup servers.

Valid views have a few properties that are enforced:
* The primary in a view must always be either the primary or the backup of the
  previous view, i.e. at most one server change between two consecutive views.
  This helps ensure that the key/value service's state is preserved. An exception:
  when the `ViewServer` first starts, it should accept any server at all as the
  first primary.
* The backup in a view can be any server (other than the primary) or can be
  altogether missing if no server is available (i.e., null).

These two properties -- a view can have no backup and the primary from a view
must be either the primary or backup of the previous view -- lead to the view
service being stuck if the primary fails in a view with no backup. This is a
flaw of the design of the `ViewServer` that we will fix in later labs.

Each key/value server should send a `Ping` message once per `PING_MILLIS` (use
`PingTimer` for this purpose on the `PBServer`s). The `ViewServer` replies to
the `Ping` with a `ViewReply`. A `Ping` lets the `ViewServer` know that the
server is alive; informs the server of the current view; and informs the
`ViewServer` of the most recent view that the server knows about. The ViewServer
should use the `PingCheckTimer` for deciding whether a server is alive or
(potentially) dead. If the ViewServer doesn't receive a Ping from a server
in-between two consecutive `PingCheckTimer`s, it should consider the server to
be dead. **Important:** to facilitate search tests, you should **not** store
timestamps in your `ViewServer`; your message and timer handlers should be
**deterministic**, i.e. there is no randomness involved and no behavior
divergence, given the same sequence of inputs at different times.

The `ViewServer` should return `STARTUP_VIEWNUM` with `null` primary and backup
when it has not yet started a view and use `INITIAL_VIEWNUM` for the first
started view. It then proceeds to later view numbers sequentially. The view
service can proceed to a new view in one of two cases:
1. It hasn't received a `Ping` from the primary or backup between two
   consecutive `PingCheckTimer`s.
2. There is no backup and there's an idle server (a server that's been pinging
   but is neither the primary nor the backup).

An important property of the `ViewServer` is that it will not change views
until the primary from the current view acknowledges that it is operating in
the current view (by sending a `Ping` with the current view number). **If the
`ViewServer` has not yet received an acknowledgment for the current view from
the primary of the current view, the `ViewServer` should not change views even
if it thinks that the primary has died.**

The acknowledgment rule prevents the `ViewServer` from getting more than one
view ahead of the servers. If the `ViewServer` could get arbitrarily far ahead,
then it would need a more complex design in which it kept a history of views,
allowed servers to ask about old views, and garbage-collected information about
old views when appropriate. The downside of the acknowledgment rule is that if
the primary fails before it acknowledges the view in which it is primary, then
the `ViewServer` cannot change views, spins forever, and cannot make forward
progress.

It is important to note that servers may not immediately switch to the new view
returned by the `ViewServer`. For example, S1 could continue sending `Ping(5)`
even if the `ViewServer` returns view 6. This indicates that the server is not
ready to move into the new view, which will be important for Part 1 of this lab.
The `ViewServer` should not consider the view to be acknowledged until the
primary sends a ping with the view number (i.e., S1 sends `Ping(6)`).

You should not need to implement any messages, timers, or other data structures
for this part of the lab. Your `ViewServer` should have handlers for `Ping` and
`GetView` messages (replying with a `ViewReply` for each, where `GetView` simply
returns the current view without the sender "pinging") and should handle and set
`PingCheckTimer`s.

Our solution took approximately 100 lines of code.

You should pass the part 1 tests before moving on to part 2; execute them with
`run-tests.py --lab 3 --part 1`.

### Hints
* There will be some states that your `ViewServer` cannot get out of because
  of the design of the view service. For example, if the primary fails before
  acknowledging the view in which it is the primary. This is expected. We will
  fix these flaws in the design in future projects.
* You'll want to add field(s) to `ViewServer` to keep track of which
  servers have pinged since the most recent `PingCheckTimer` and how many pings
  intervals each server has most recently missed. Also, you may want to track
  the most recent acked view for each server, thus the outdated `Ping`s can be
  filtered.
* Add field(s) to `ViewServer` to keep track of the current view.
* There may be more than two servers sending `Ping`s. The extra ones (beyond
  primary and backup) are volunteering to be backup if needed. You'll want to
  track these extra servers as well in case one of them needs to be promoted to
  be the backup.
* You might need to think whether and when the `ViewServer` should update the view
  if the current view is not yet acked by the primary.
* When the primary fails and there is no backup, your `ViewServer`
  implementation should _not_ switch back to the uninitialized state.  While
  this is a design decision, the tests will verify _this_ particular
  implementation.

## Part 2: The Primary/Backup Key/Value Service
Next, you will implement the client and primary-backup servers (`PBClient` and
`PBServer`).

We expect your service would continue operating correctly as long as there has
never been a time at which no server was alive. Also, it should operate
correctly with network partitions: a server that suffers temporary network
failure without crashing, or can talk to some computers but not others, e.g. a
client can communicate with backup but not primary while the communication
between servers is normal. If your service is operating with just one server, it
should be able to incorporate an idle server (as backup), so that it can then
tolerate another server failure.

Correct operation means that operations are executed linearizably. All
operations should provide exactly-once semantics as in lab 1.

You should assume that the `ViewServer` never halts or crashes.

It's crucial that only one primary be active at any given time. You should have
a clear story worked out for why that's the case for your design. A danger:
suppose in some view S1 is the primary; the `ViewServer` changes views so that
S2 is the primary; but S1 hasn't yet heard about the new view and thinks it is
still primary. Then some clients might talk to S1, and others talk to S2, and
not see each other's operations.

Normally, a server that isn't the active primary should either not respond to clients, or
respond with an error. However, this won't work with network partitions. You may need to
adjust your implementation or add extra mechanisms to deal with network partitions, e.g.
using backup as a proxy for clients.

A server should not talk to the `ViewServer` for every operation it receives,
since that would put the `ViewServer` on the critical path for performance and
fault-tolerance (we will check this in tests). Instead, servers should `Ping`
the `ViewServer` periodically (once every `PING_MILLIS`) to learn about new views.
Similarly, the client should not talk to the `ViewServer` for every operation
it sends; instead, it should cache the current view and only talk to the `ViewServer`
(by sending a `GetView` message) on initial startup, when the current primary seems
to be dead (i.e., on `ClientTimer`), or when it receives an error indicating outdated views.

When servers startup initially, they should `Ping` the `ViewServer` with
`ViewServer.STARTUP_VIEWNUM`. After that, they should `Ping` with the latest
view number they've seen, unless they're the primary for a view that has not yet
started.

Part of your one-primary-at-a-time strategy should rely on the `ViewServer` only
promoting the backup from view `i` to be primary in view `i+1`. If the old
primary from view `i` tries to handle a client request, it will forward it to
its backup. If that backup hasn't heard about view `i+1`, then it's not acting
as primary yet, so no harm done. If the backup has heard about view `i+1` and is
acting as primary, it knows enough to reject the old primary's forwarded client
requests.

You'll need to ensure that the backup sees every update to the application in
the same order as the primary, by a combination of the primary initializing it
with the complete application state and forwarding subsequent client operations.
The at-most-once semantics of `AMOApplication` (which you should once again wrap
your application in) should handle the backup receiving duplicate operations
from the primary. However, you will need to keep some state on the primary to
ensure that the backup processes operations in the correct order.

Our solution took approximately 200 lines of code for `PBServer`, and we have added
2 extra messages and 3 extra timers.

You should pass the part 2 tests; execute them with `run-tests.py --lab 3 --part
2`.

### Implementation
We have added some fields and inner classes for you to use. We have provided you
three `Result`s (`PBResult`) for the inner operations between servers. The `Backup` refers to the
request forwarding between the primary and backup. The `Sync` refers to the application
state sync when a new backup is promoted. The `InvalidView` can be used for indicating
the outdated views, either sending to servers or clients.

### Hints
* You'll probably need to create new messages and timers to forward client
  requests from primary to backup, since the backup should reject a direct
  client request but should accept a forwarded request.
* You'll need to add special mechanisms for handling the network partition issue.
* You'll probably need to create new messages and timers to handle the transfer of
  the complete application state from the primary to a new backup. You can send
  the whole application in one message. Remember to use the deep copy interface that
  we implement in the previous project.
* The timer check length matters. You might not want the check intervals for `Backup`
  and `Sync` to be too short. This would make timers fire too frequently and might
  exhaust your running servers. This also applies to client timers.
* Even if your `ViewServer` passed all the tests in Part 1, it may still have
  bugs that cause failures in Part 2.
* Your `PBClient` should be very similar to the `SimpleClient` from Project 2 but with `View`.
* You may find the visualization debugger (`--start-viz`) useful for the last several
  search tests in Part 2.

### Extra About Search Tests
Now that the solutions and tests are more complex, you may find your code
failing either the correctness or the liveness search tests.

If your code fails a correctness test, the next step is relatively easy: the
checker provides you the counterexample, and you can use the debugger to
visualize it. However, note that our model checking is incomplete – your code
might have an error but we don't find it. For example, the sequence of messages
that triggers the problem may be longer than our search depth. This is a
fundamental problem for these types of tests.

The liveness tests have the opposite problem. In the liveness tests, certain events
are expected to happen as the system goes. They pass as your code produce the
right result, while they will flag an error if they can't find a valid event
sequence that allow certain events to happen in the time allowed. This is also
a fundamental problem for these types of tests. It is the reason why we
include both correctness and liveness tests – you have more assurance if you pass both
than if you pass only one. For example, the null solution that does nothing can sometimes
pass a correctness test – it doesn't do anything wrong! – but it will not pass the liveness test.
Similarly, passing a liveness test alone doesn't imply that your solution is bug free.

If your code does flag a liveness error, here are some steps to take:
1) `python3 run-tests.py --checks` will check that your handlers are deterministic, etc.
   If not, try fixing those issues first and rerunning the tests. By allowing
   faster search, this may also allow the model checker to find a liveness
   example, but it may also allow it to find additional correctness
   counterexamples. That's a good thing, in our view.
2) If you believe your solution is live, you can use the visual debugger to
   create by hand a sequence of messages that achieves the stated goal. You may
   find doing that that your code doesn't behave as you expected, ie., that
   there is a bug you need to fix. **This would require you to understand the
   expected event in tests and figure out a sequence of events to make that
   event happen. If the length of that sequence is too long (dfs), you might
   need to simplify some msg or timer and adjust your implementation to speed
   up the progress.**

## Submission
Project 3 requires a fair amount of code and you can sense the increasing
difficulty as the semester goes. We have provided 2 programming assignments in
GradeScope. The first assignment is for the part 1 view server, and the second
is for the final submission for overall tests. The GradeScope auto grader will
only test for part 1 for the first assignment. You do not have to submit for the
first, while its due is just as our recommend implementation schedule and
reminder for you. **You can just submit for the second (overall) assignment in
GradeScope, and we will take the result of that submission as your grades for
Project 3.** You should also write a simple report. The report rubrics are
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

```shell script
$ submit.sh
```

***Note**: Make sure you do not include **print** or **log** statements in your implementation. Also, do not include or zip extra files for your submissions. We will check the completeness and validity of submission before grading.
If your submission fails to satisfy the submission requirement or could not compile, you will see feedback from GradeScope indicating that and receive 0 for that submission.*

***



## Project Question List

***

- Can both primary and backup send the response to a client?
	
	The answer depends on your treatment for the network partition issue. If there is a trap between the primary and the client, you might want the backup ack as proxy and send a response to the client. There might be some consistency issue if you let backup sending a response to clients and you will need an extra mechanism to ensure strong consistency. However, there are other solutions, e.g. adding an extra command in the client to ask the `ViewServer` to invalidate the current view.

***

- There are so many cases. I don't have a clue to start. What should I do?
	
	The first piece of advice would be cutting down the number of field variables in your implementation. Since we implement all parts as state machines, you should narrow down the input scope that would make the nodes taking action and updating states. For example, only dealing with client requests when the client view and server view are equal while ignoring or sending back an invalid response for other cases. We would also recommend you to think carefully about the procedures to perform view update in `PBServer`. There might be some ongoing backup requests as the new view coming, indicating server failures.


## Recommend Reading List

- [MIT 6.824 Lab 2](http://nil.csail.mit.edu/6.824/2015/labs/lab-2.html)
- [Primary Backup](https://www.youtube.com/watch?v=M_teob23ZzY)
- [Fault Tolerance VM](https://www.vmware.com/techpapers/2010/the-design-and-evaluation-of-a-practical-system-fo-10134.html)
- [Viewstamped Replication](http://pmg.csail.mit.edu/papers/vr-revisited.pdf)
- [Facebook Memecache](https://www.usenix.org/conference/nsdi13/technical-sessions/presentation/nishtala)
- [PB Design](https://courses.cs.washington.edu/courses/cse452/18sp/PrimaryBackup.pdf)
- [State Machine PB](https://www.cs.princeton.edu/courses/archive/fall16/cos418/docs/L8-consensus-2.pdf)
