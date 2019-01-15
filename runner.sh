#!/usr/local/bin/bash

docker build -t raft .
for i in {1..12}; do
	docker run --rm -t --env NUM=$i --mount "type=bind,source=$(pwd)/logs,target=/logs" raft &
done
while true; do
	wait -n || {
		code="$?"
		([[ $code = "127" ]] && exit 0 || exit "$code")
		break
	}
done;
