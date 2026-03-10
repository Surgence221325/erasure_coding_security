#!/usr/bin/env bash

function verify() {
	arr=("$@")
	for i in "${arr[@]}";
		do
				if [ ! -f $i ]; then

					echo "Missing ${i}"
					exit 1
				fi
		done
}

req_files=("src/dslabs.atmostonce/AMOCommand.java" "src/dslabs.atmostonce/AMOResult.java" "src/dslabs.atmostonce/AMOApplication.java" "src/dslabs.paxos/PaxosReply.java" "src/dslabs.paxos/Timers.java" "src/dslabs.paxos/PaxosDecision.java" "src/dslabs.paxos/PaxosLogSlotStatus.java" "src/dslabs.paxos/Messages.java" "src/dslabs.paxos/Ballot.java" "src/dslabs.paxos/PaxosServer.java" "src/dslabs.paxos/PaxosClient.java" "src/dslabs.paxos/PaxosRequest.java" "src/dslabs.paxos/Pvalue.java" "src/dslabs.kvstore/KVStore.java")
verify "${req_files[@]}"
if [[ $? -ne 0 ]]; then
    exit 1
fi

if [ $# -eq 1 ]
then
	zip "${1}.zip" src/dslabs.atmostonce/AMOCommand.java src/dslabs.atmostonce/AMOResult.java src/dslabs.atmostonce/AMOApplication.java src/dslabs.paxos/PaxosReply.java src/dslabs.paxos/Timers.java src/dslabs.paxos/PaxosDecision.java src/dslabs.paxos/PaxosLogSlotStatus.java src/dslabs.paxos/Messages.java src/dslabs.paxos/Ballot.java src/dslabs.paxos/PaxosServer.java src/dslabs.paxos/PaxosClient.java src/dslabs.paxos/PaxosRequest.java src/dslabs.paxos/Pvalue.java src/dslabs.kvstore/KVStore.java
else
	zip "submission.zip" src/dslabs.atmostonce/AMOCommand.java src/dslabs.atmostonce/AMOResult.java src/dslabs.atmostonce/AMOApplication.java src/dslabs.paxos/PaxosReply.java src/dslabs.paxos/Timers.java src/dslabs.paxos/PaxosDecision.java src/dslabs.paxos/PaxosLogSlotStatus.java src/dslabs.paxos/Messages.java src/dslabs.paxos/Ballot.java src/dslabs.paxos/PaxosServer.java src/dslabs.paxos/PaxosClient.java src/dslabs.paxos/PaxosRequest.java src/dslabs.paxos/Pvalue.java src/dslabs.kvstore/KVStore.java
fi
