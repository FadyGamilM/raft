package main

import "log"

func main() {
	log.Println("hello raft ..")
	log.Println([]int{0, 1, 2, 3, 4, 5}[1+1 : 3+1])
}
