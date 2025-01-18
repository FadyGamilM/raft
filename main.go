package main

import (
	"log"
	"math/rand"
	"time"
)

func main() {
	log.Println("hello raft ..")
	log.Println([]int{0, 1, 2, 3, 4, 5}[1+1 : 3+1])
	log.Println("testing new setup")

	min := int64(150)
	max := int64(300)
	rand.Seed(time.Now().UnixMilli())

	log.Println(min + rand.Int63n(max-min))

}
