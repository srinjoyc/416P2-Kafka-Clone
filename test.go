package main

import "github.com/serialx/hashring"

func main() {
	memcacheServers := []string{"192.168.0.246:11212",
		"192.168.0.247:11212",
		"192.168.0.249:11212"}

	ring := hashring.New(memcacheServers)
	server, _ := ring.GetNode("my_key")
	println(server)
}
