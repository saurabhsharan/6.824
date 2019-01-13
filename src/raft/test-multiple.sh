#!/bin/bash

for i in {1..10}; do
	go test -race -run 2B > /dev/null
	if [[ $? != 0 ]]; then
		echo "ERROR"
	fi
	echo "$i passed"
done
