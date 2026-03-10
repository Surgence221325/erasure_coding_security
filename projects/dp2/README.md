# CPSC 416 Project: DSLabs

**DO NOT DISTRIBUTE OR PUBLICLY POST SOLUTIONS TO THESE LABS. MAKE ALL FORKS OF
THIS REPOSITORY WITH SOLUTION CODE PRIVATE.**

***Instructor: Tony Mason***

**<ins>TA Contacts</ins>**

| TA | E-mail |
| --- | --- |
| Andy Hsu | andy.hsu@alumni.ubc.ca |
| Hamidreza Ramezanikebrya | hamid@ece.ubc.ca |
| Jonas Tai | jonastai@student.ubc.ca |
| Cathy Yang | kaiqiany@student.ubc.ca |



***

Welcome to Winter Term 2 2022!

This repo hosts the course project for CPSC 416 Distributed Computing at UBC.
The course project is designed to guide students in gaining hands-on
experience with multiple approaches that are at the core of modern
distributed computing systems. During the semester, you will work to construct
a sharded Key-Value (KV) store resembling [Google
Spanner](https://dl.acm.org/doi/pdf/10.1145/2491245),
which provides the tools necessary for building distributed transactions.
This system will handle user R/W requests and internal shard consistency.
Using a step-by-step approach, students will implement multiple fault-tolerant
protocols for maintaining replicas and ensuring distributed consensus built over
the provided communication infrastructures. After finishing this project,
students are expected to have a clear understanding of the communication model
in distributed and asynchronous manners and gain experience in system
programming.

DSLabs was developed as the educational framework for  **CSE452 Distributed
Systems** from the *University of Washington* and it was written by Ellis
Michael. It is a framework for creating, testing, model checking, visualizing, and debugging
distributed systems. While all information required for projects
is included, students can still refer to the original GitHub links to see the
general ideas in [dslabs repo](https://github.com/emichael/dslabs). **Note:**
the version that we use in this course differs from the original.

We modified parts of the testing modules of the original DSLabs to fit
our course requirements, including the use of GradeScope for
automatically grading and testing. We have also added several limits on the
programming flexibility, i.e. exposing certain code blocks for students, to
support separated grading. For different tasks in the published lab assignments,
students are asked to implement subparts of code, giving restrictions on code
scope, code behaviors, etc. We then integrate student code with our
half-complete codebase to perform tests and generate test
reports for feedbacks on GradeScope.

Detailed guides for each project are placed under ``lab/README.md``. Moreover,
we have included some hints and code snippets. Please read them before you start
your implementation. All implementation will go to ``lab/src``. All tests are
available under ``lab/tst``. Also, a general illustration about the programming
model and dependency libraries for the framework is under ``handout`` for reference.

## CPSC 416 Project

We will use **Java** for course projects, **8** in specific. There are 5 programming assignments for the whole semester. We summarize the general purposes and rough durations for each assignment as below. Students should remember these purposes as guidelines when implementing the solutions.

- **Project 1 intro**: students are guided to get familiar with project framework and programming routines - *roughly 2 weeks starting from the first day of semester*
- **Project 2 clientserver**: an exactly-once RPC protocol is implemented and tested on top of an asynchronous network - *2 weeks estimated*
- **Project 3 primary-backup**: a classical primary-backup protocol for fault-tolerance is implemented - *2 weeks estimated*
- **Project 4 paxos**: the *PAXOS* protocol for system consensus is implemented. Students will follow the lecture contents and some published papers - *3 weeks estimated*
- **Project 5 sharded KV Store**: a sharded key/value store out of multiple replica groups, with each of them using PAXOS for requests ordering internally, is built. Besides, a two-phase commit protocol is introduced for transactions - *4 weeks estimated*

## Programming Model

The DSLabs framework is built around message-passing state machines (also known as I/O automata or distributed actors), which we call nodes. These basic units of a distributed system consist of a set of message and timer handlers; these handlers define how the node updates its internal state, sends messages, and sets timers in response to an incoming message or timer. These nodes are run in single-threaded event loops, which take messages from network and timers from the node's timer queue and call the node's handlers for those events.

## Testing and Model Checking

Following doc is adapted from the [dslabs
repo](https://github.com/emichael/dslabs) and the [dslabs
paper](https://ellismichael.com/papers/dslabs-eurosys19.pdf). Students can refer
to them for more extensive reading.

The infrastructure has a suite of tools for creating automated test cases for
distributed systems. These tools make it easy to express the scenarios the
system should be tested against (e.g., varying client workloads, network
conditions, failure patterns, etc.) and then run students' implementations on an
emulated network (it is also possible to replace the emulated network interface
with an interface to the actual network).

As much as possible, the tests try to identify bugs in student implementations.
Note that it is actually a harder problem to try and build a grader to figure
out the numerous ways that highly creative students can implement the code wrong
than to solve the lab correctly.  We have tried to ensure that the feedback
given is useful, but keep in mind that the feedback may not be very useful.  In
some ways this is like working with a customer: sometimes bug reports are
useful, often they are not.  One benefit of the node-centric perspective of the
DSLabs is that it permits the use of a formal verification technique known as
**model checking**.

Model checking a distributed system is conceptually simple. First, the initial
state of the system is configured. Then, saying one state of the system, s₂,
(consisting of the internal state of all nodes, the state of their timer queues,
and the state of the network) is the successor of another state s₁ if it can be
obtained from s₁ by delivering a single message or timer that is pending in s₁.
A state might have multiple successor states. Model checking is the systematic
exploration of this state graph, the simplest approach being breadth-first
search. Some invariants, e.g. state correctness, that should be preserved (e.g.
linearizability) are defined for tests. By searching though all possible
ordering of events, tests will try to make sure those invariants are preserved
in students' implementations. When an invariant violation is found, the
model-checker can produce a minimal trace which leads to the invariant
violation.

While model checking distributed systems is useful and has been used extensively in industry and academia to find bugs in distributed systems, exploration of the state graph is still a fundamentally hard problem – the size of the graph is typically exponential as a function of depth. To extend the usefulness of model checking even further, the test infrastructure lets us prune the portion of the state graph we explore for an individual test, guiding the search towards common problems while still exploring all possible executions in the remaining portion of the state space.

## General Suggestion for Students

The projects for CPSC 416 are challenging, particularly the last two labs. You
_will_ encounter difficulties debugging your implementation. Even though we have
provided you some simplifying assumptions, e.g. method invokes, there is still a
great degree of freedom and flexibility for your implementation.
There is no single correct solution. You can find some hints and sample snippets under
each lab assignment. They are assumed to be useful. Although there is a
visualization tool for labs, our experiences and feedbacks from previous
semesters indicates this tool is not generally useful though it does help for
certain test cases.

Debugging your implementation will demand much of your effort. Here are a few
strategies for making this easier:

- Logging.  The challenge here is to figure out where and how to log this
  information; this skill is likely to be more valuable than solving the actual
  problem!
- Understand the test cases; duplicate them yourself locally if you can.
- When your results diverge from the expected results, try to figure out where
  they diverge.  Then look through your code to see if you can reason about
  _why_ they diverge.  This sort of debugging is common for distributed systems
  and building skills in this area is likely to be more valuable to you after
  this course than simply solving the actual problem.
- If you are using an IDE, make sure you learn how to use the debugger with it.
  This will improve your productivity: stepping through code can often provide
  insight that you will miss when looking at extensive debug output.

***Start early! Happy coding!* :)**

