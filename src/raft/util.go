package raft

import "log"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 2 {
		log.Printf(format, a...)
	}
	return
}

func IPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 1 {
		log.Printf(format, a...)
	}
	return
}

func EPrintf(format string, a ...interface{}) (n int, err error) {
	log.Printf(format, a...)
	return
}
