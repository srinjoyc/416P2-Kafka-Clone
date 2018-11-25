package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"./follower"
	"./leader"
)

/*
* @desc:
* Handles TCP connections aysnc as they come in to the listening port (:8080)
* @param:
* TCP net connection object
* @return:
* Exits the connection if the string "STOP" is sent by the client. or replies with 1.
 */
func handleConnection(c net.Conn) {
	fmt.Printf("Serving %s\n", c.RemoteAddr().String())
	for {
		netData, err := bufio.NewReader(c).ReadString('\n')
		if err != nil {
			fmt.Println(err)
			return
		}

		msg := strings.TrimSpace(string(netData))
		fmt.Println(msg)
		if msg == "STOP" {
			break
		}

		result := strconv.Itoa(1) + "\n"
		c.Write([]byte(string(result)))
	}
	c.Close()
}

/*
* @desc
* Starts listening on the port passed in.
* @param:
* ARG from cmd.
* @return:
* Infinite loop listening for new TCP connections
* and spawning a new thread to serve each TCP client.
 */
func main() {
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please provide a port number!")
		return
	}
	PORT := ":" + arguments[1]
	leader.Initialize()
	follower.Initialize()
	l, err := net.Listen("tcp4", PORT)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer l.Close()
	fmt.Println("Listening on: " + PORT + "!")

	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		go handleConnection(c)
	}
}
