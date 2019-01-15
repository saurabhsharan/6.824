FROM golang:1.10.7

COPY src /mit/src

RUN mkdir /logs

ENV GOPATH /mit

WORKDIR /mit/src/raft

ENTRYPOINT go test > /logs/$NUM.log
