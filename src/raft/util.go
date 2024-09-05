package raft

import "log"

// false  true
// Debugging
const Debug = false // 打印goroutine退出情况
const Cflag = true
const Tflag = true
const Kflag = true // 专门针对某个bug的猜想

const Fflag = false

const all = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func CPrintf(format string, a ...interface{}) (n int, err error) {
	if Cflag && all {
		log.Printf(format, a...)
	}
	return
}

func TPrintf(format string, a ...interface{}) (n int, err error) {
	if Tflag && all {
		log.Printf(format, a...)
	}
	return
}

func KPrintf(format string, a ...interface{}) (n int, err error) {
	if Kflag && all {
		log.Printf(format, a...)
	}
	return
}

func FPrintf(format string, a ...interface{}) (n int, err error) {
	if Fflag {
		log.Printf(format, a...)
	}
	return
}
