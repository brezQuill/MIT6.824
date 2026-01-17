package main

import (
	"fmt"
	"math/rand"
	"time"
)

func RandomizedElectionTimeout(me int) time.Duration {
	return time.Duration(time.Duration(float64(120+me*20)+10*rand.Float64()) * time.Millisecond)
}

var peers []int = make([]int, 5)
var me int = 1

func main() {
	for server := range peers {
		if server == me {
			continue
		}
		fmt.Println(server)
	}
}
