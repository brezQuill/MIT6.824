package raft

import "log"

// false  true
// Debugging
const Debug = false // 打印goroutine退出情况
const Tflag = false // 打印时间信息

const Cflag = false
const Sflag = false // 当前debug日志

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

func SPrintf(format string, a ...interface{}) (n int, err error) {
	if Sflag {
		log.Printf(format, a...)
	}
	return
}
