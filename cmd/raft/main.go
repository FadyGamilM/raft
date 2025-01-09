package main

import (
	"log"

	"github.com/FadyGamilM/raft/raft"
)

func main() {
	followerNode := raft.Follower
	log.Println(followerNode.String())
}
