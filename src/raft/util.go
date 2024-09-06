package raft

import "log"

// false  true
// Debugging
const Debug = false // 打印goroutine退出情况
const Cflag = false
const Tflag = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func TPrintf(format string, a ...interface{}) (n int, err error) {
	if Tflag {
		log.Printf(format, a...)
	}
	return
}

func CPrintf(format string, a ...interface{}) (n int, err error) {
	if Cflag {
		log.Printf(format, a...)
	}
	return
}
