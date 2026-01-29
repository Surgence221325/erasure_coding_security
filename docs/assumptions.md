Ideas about Failure Model/Assumptions of System:

Current failure model decided is the crash model, current project is a distributed key value store.

Assumptions of system

To provide some ground work, we must define distributed key value store as a system with two or more partitioned nodes responsible for managing a store of data accessed via a simple key (no strong query processing).

for this system I am assuming:
- Our messages are reliable, that is the messages are sent over TCP and will eventually be delivered.
- Our message sending time is bounded, that is our messages will eventually be sent (current failure model does not account for time outs).
- Our system can process the request in bounded time. That is we do not account for server overloading/processing delays.
- 