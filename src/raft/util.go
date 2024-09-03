package raft

import "log"

// false  true
// Debugging
const Debug = true // 打印goroutine退出情况
const Cflag = true
const Tflag = true
const Kflag = true // 专门针对某个bug的猜想

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
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

func TPrintf(format string, a ...interface{}) (n int, err error) {
	if Tflag {
		log.Printf(format, a...)
	}
	return
}

func KPrintf(format string, a ...interface{}) (n int, err error) {
	if Kflag {
		log.Printf(format, a...)
	}
	return
}
