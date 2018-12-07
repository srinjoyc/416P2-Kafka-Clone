package main

import (
	"bufio"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	"../lib/IOlib"
	"../lib/message"
)

// Error types

// ErrInsufficientFreeNodes denotes that someone requested more free nodes
// than are currently available
var ErrInsufficientFreeNodes = errors.New("insufficient free nodes")

type configSetting struct {
	ManagerNodeID     string
	ProviderIPPort    string
	BrokerIPPort      string
	ManagerIP         string
	PeerManagerNodeIP []string
}

var config configSetting

var brokersList struct {
	list  []string
	mutex sync.Mutex
}

// manager keeps track of all free nodes (i.e. nodes that are ready to be 'provisioned'.
// If this manager goes down, it will need to ensure that the other managers
// have the correct copy of this set.
// Initialize this set to a bunch of free nodes on manager startup, probably via a json config.
var freeNodes freeNodesSet

// freeNodesSet contains a set of nodes for the entire topology.
// if set[nodeIP] == true,  node is free
// if set[nodeIP] == false, node is busy
//
// Operations on freeNodesSet are atomic.
type freeNodesSet struct {
	set   map[string]bool
	mutex sync.Mutex
}

// getFreeNodes returns a slice of free nodes with length num
// If the amount of available free nodes is < num,
// return an InsufficientFreeNodes error.
// Once these nodes are returned to caller, there are marked as busy to avoid concurrency issues.
// If the caller gets free nodes using this function, and then decides not to use them, it must
// manually de-allocate each node by calling setNodeAsFree.
// TODO: In the future, this may be implemented as some other complicated algorithm
// for allocating free nodes. For now just getting the first free nodes we see is ok.
func (s *freeNodesSet) getFreeNodes(num int) ([]string, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	var nodes []string
	var full bool
	for ip, free := range s.set {
		if free {
			nodes = append(nodes, ip)
			s.set[ip] = false
		}

		if len(nodes) == num {
			full = true
			break
		}
	}

	if full {
		return nodes, nil
	} else {
		return nil, ErrInsufficientFreeNodes
	}
}

func (s *freeNodesSet) isFree(ip string) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if free, ok := s.set[ip]; ok {
		return free
	}
	// If we asked for an ip thats not even in the registered set of nodes,
	// just return false as if node is busy.
	return false
}

func (s *freeNodesSet) setNodeAsFree(ip string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, ok := s.set[ip]; ok {
		s.set[ip] = true
	}
	// No-op if ip is not in registered set.
}

func (s *freeNodesSet) setNodeAsBusy(ip string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, ok := s.set[ip]; ok {
		s.set[ip] = false
	}
	// No-op if ip is not in registered set.
}

func (s *freeNodesSet) addFreeNode(ip string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, ok := s.set[ip]; !ok {
		// Only add this free node if its not already in the set of free nodes.
		s.set[ip] = true
	}
	// If ip is already in the set, no-op.
}

/* readConfigJSON
 * Desc:
 *		read the configration from file into struct config
 *
 * @para configFile: relative url of configuration file
 * @retrun: None
 */
func readConfigJSON(configFile string) {
	jsonFile, err := os.Open(configFile)
	defer jsonFile.Close()
	if err != nil {
		fmt.Println(err) // if we os.Open returns an error then handle it
	}
	json.Unmarshal([]byte(IOlib.ReadFileByte(configFile)), &config)
}

/* listenProvider
 * Desc:
 * 		this is a goroutine dealing with requests from provider routine
 *
 * @para IPPort string:
 *		the ip and port opened for messages from provider routine
 */
func listenProvider() {
	listener, err := net.Listen("tcp4", config.ProviderIPPort)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer listener.Close()

	fmt.Println("Listening provider at :" + config.ProviderIPPort)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go handleProviderMessage(conn)
	}
}

/* listenManagers()
 * Desc:
 * 		this is a goroutine dealing with other manger nodes
 *
 * @para IPPort [string]:
 *		The list of neighbouring managers
 */
func listenManagers() {
	listener, err := net.Listen("tcp4", config.ManagerIP)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer listener.Close()

	fmt.Println("Listening to other managers at :" + config.ManagerIP)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		go dealManager(conn)
	}
}

/* dealManager
 * @para conn:
 *		the ip and port opened for messages from provider routine
 *
 * Desc: Handles all messages from other managers
 *
 */
func dealManager(conn net.Conn) {
	// decode the serialized message from the connection
	dec := gob.NewDecoder(conn)
	message := &Message{}
	dec.Decode(message) // decode the infomation into initialized message

	// if-else branch to deal with different types of messages
	if message.Type == "Text" {
		fmt.Printf("Receive Manager Msg: {pID:%s, type:%s, partition:%s, text:%s}\n", message.ID, message.Type, message.Partition, message.Text)

		// code about append text

	} else if message.Type == "CreateTopic" {
		fmt.Printf("Receive Manager Msg: {pID:%s, type:%s, topic:%s}\n", message.ID, message.Type, message.Topic)

		// code about topic

	}

	// write the success response
	enc := gob.NewEncoder(conn)
	err := enc.Encode(Message{config.ManagerNodeID, "response", "succeed", "", ""})
	if err != nil {
		log.Fatal("encode error:", err)
	}
	conn.Close()
}

/* dealProvider
 * @para conn:
 *		the ip and port opened for messages from provider routine
 *
 * Desc:
 *
 */
func handleProviderMessage(conn net.Conn) {
	// decode the serialized message from the connection
	defer conn.Close()
	dec := gob.NewDecoder(conn)

	incoming := &message.Message{}
	dec.Decode(incoming) // decode the infomation into initialized message

	// if-else branch to deal with different types of messages
	if incoming.Type == "Text" {
		fmt.Printf("Receive Provider Msg: {pID:%s, type:%s, partition:%s, text:%s}\n", incoming.ID, incoming.Type, incoming.Partition, incoming.Text)

		// code about append text

	} else if incoming.Type == "CreateTopic" {
		fmt.Printf("Receive Provider Msg: {pID:%s, type:%s, topic:%s}\n", incoming.ID, incoming.Type, incoming.Topic)

		// code about topic

	}

	// write the success response
	enc := gob.NewEncoder(conn)
	err := enc.Encode(message.Message{config.ManagerNodeID, "response", "succeed", "", ""})
	if err != nil {
		log.Fatal("encode error:", err)
	}
}

func listenBroker() {
	listener, err := net.Listen("tcp4", config.BrokerIPPort)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer listener.Close()

	fmt.Println("Listening broker at :" + config.BrokerIPPort)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go handleBrokerMessage(conn)
	}
}

func handleBrokerMessage(conn net.Conn) {
	defer conn.Close()
	// decode the serialized message from the connection
	dec := gob.NewDecoder(conn)
	incoming := &message.Message{}
	dec.Decode(incoming) // decode the infomation into initialized message

	// if-else branch to deal with different types of messages
	if incoming.Type == "New Broker" {
		fmt.Printf("Receive Broker Msg: {pID:%s, type:%s, partition:%s, text:%s}\n", incoming.ID, incoming.Type, incoming.Partition, incoming.Text)

		// code about append the broker
		brokersList.mutex.Lock()
		flag := false
		for _, ip := range brokersList.list {
			if ip == incoming.Text {
				flag = true
				break
			}
		}
		if flag == false {
			brokersList.list = append(brokersList.list, incoming.Text)
		}
		brokersList.mutex.Unlock()
	}

	// write the success response
	enc := gob.NewEncoder(conn)
	err := enc.Encode(message.Message{config.ManagerNodeID, "response", "succeed", "", ""})
	if err != nil {
		log.Fatal("encode error:", err)
	}
}

// Initialize starts the node as a Manager node in the network
func Initialize() bool {
	configFilename := os.Args[1]
	readConfigJSON(configFilename)

	println("Manager", config.ManagerNodeID, "starts")

	go listenProvider()
	go listenBroker()
	go listenManagers()

	return true
}

func main() {
	Initialize()

	// terminal controller like shell
	reader := bufio.NewReader(os.Stdin)
	for {
		cmd, _ := reader.ReadString('\n')
		if cmd == "broker\n" {
			fmt.Println(brokersList.list)
		}
	}
}
