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

req_files=("src/dslabs.atmostonce/AMOCommand.java" "src/dslabs.atmostonce/AMOResult.java" "src/dslabs.atmostonce/AMOApplication.java" "src/dslabs.primarybackup/Timers.java" "src/dslabs.primarybackup/PBClient.java" "src/dslabs.primarybackup/PBServer.java" "src/dslabs.primarybackup/Messages.java" "src/dslabs.primarybackup/View.java" "src/dslabs.primarybackup/ViewServer.java" "src/dslabs.kvstore/KVStore.java")
verify "${req_files[@]}"
if [[ $? -ne 0 ]]; then
    exit 1
fi

if [ $# -eq 1 ]
then
	zip "${1}.zip" src/dslabs.atmostonce/AMOCommand.java src/dslabs.atmostonce/AMOResult.java src/dslabs.atmostonce/AMOApplication.java src/dslabs.primarybackup/Timers.java src/dslabs.primarybackup/PBClient.java src/dslabs.primarybackup/PBServer.java src/dslabs.primarybackup/Messages.java src/dslabs.primarybackup/View.java src/dslabs.primarybackup/ViewServer.java src/dslabs.kvstore/KVStore.java
else
	zip "submission.zip" src/dslabs.atmostonce/AMOCommand.java src/dslabs.atmostonce/AMOResult.java src/dslabs.atmostonce/AMOApplication.java src/dslabs.primarybackup/Timers.java src/dslabs.primarybackup/PBClient.java src/dslabs.primarybackup/PBServer.java src/dslabs.primarybackup/Messages.java src/dslabs.primarybackup/View.java src/dslabs.primarybackup/ViewServer.java src/dslabs.kvstore/KVStore.java
fi
