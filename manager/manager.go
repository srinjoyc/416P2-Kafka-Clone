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
	"strings"
	"sync"
	"time"

	"../lib/IOlib"
	message "../lib/message"
)

// Error types

// ErrInsufficientFreeNodes denotes that someone requested more free nodes
// than are currently available
var ErrInsufficientFreeNodes = errors.New("insufficient free nodes")

type configSetting struct {
	ManagerNodeID     string
	ManagerIP         string
	PeerManagerNodeIP []string
}

var config configSetting

// manager keeps track of all free nodes (i.e. nodes that are ready to be 'provisioned'.
// If this manager goes down, it will need to ensure that the other managers
// have the correct copy of this set.
// Initialize this set to a bunch of free nodes on manager startup, probably via a json config.
var freeNodes = freeNodesSet{
	set: make(map[string]bool),
}

var channelMap = make(map[string]map[uint8]channel)

type channel struct {
	topicName   string
	partition   uint8
	leaderIP    string
	followerIPs string
	available   bool
}

//var topicMap =
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

// TODO: Make this DHT instead of first seen first provisioned.
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

/* listenManagers()
 * Desc:
 * 		this is a goroutine dealing with other manger nodes
 *
 * @para IPPort [string]:
 *		The list of neighbouring managers
 */
func listenForMessages() {
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
		go processMessage(conn)
	}
}

/* dealManager
 * @para conn:
 *		the ip and port opened for messages from provider routine
 *
 * Desc: Handles all messages from other managers
 *
 */
func processMessage(conn net.Conn) {
	// decode the serialized message from the connection
	dec := gob.NewDecoder(conn)
	msg := &message.Message{}
	dec.Decode(msg) // decode the infomation into initialized message
	senderIP := conn.RemoteAddr().String()
	// if-else branch to deal with different types of messages

	// NEW BROKER ON NETWORK, LETTING US KNOW.
	if msg.Type == message.NEW_BROKER {
		fmt.Println(conn.RemoteAddr().String())
		freeNodes.addFreeNode(senderIP)
		enc := gob.NewEncoder(conn)
		managerIPs := ""
		for _, ip := range config.PeerManagerNodeIP {
			managerIPs += ip + ";"
		}
		err := enc.Encode(message.Message{Type: message.INFO, Text: managerIPs, Timestamp: time.Now()})
		if err != nil {
			fmt.Println(err)
		}
	}

	// NETWORK INFO MSG (RANDOM/TESTING)
	if msg.Type == message.INFO {
		fmt.Printf("Receive info Msg: {pID:%s, type:%s, partition:%s, text:%s}\n", msg.ID, msg.Type, msg.Partition, msg.Text)
		// code about append text

		// NEW TOPIC BEING CREATED
	} else if msg.Type == message.NEW_TOPIC {
		fmt.Printf("Receive Provider Msg: {pID:%s, type:%s, topic:%s}\n", msg.ID, msg.Type, msg.Topic)
		// get free nodes (chose based on algo)
		clusterNodes, err := freeNodes.getFreeNodes(1)
		if err != nil {
			fmt.Println(err)
		}
		followerIPs := ""
		// contact each node to set their role
		for idx, ip := range clusterNodes {
			fmt.Println(ip)
			if idx == 0 {
				continue
			} else {
				followerIPs += ip
			}
		}
		startBrokerMsg := message.Message{ID: config.ManagerIP, Type: message.START_LEADER, Text: followerIPs, Topic: msg.Topic, Role: message.LEADER, Partition: msg.Partition, Timestamp: time.Now()}
		fmt.Println(startBrokerMsg)
		enc := gob.NewEncoder(conn)
		err = enc.Encode(startBrokerMsg)
		if err != nil {
			fmt.Println(err)
		}
		if channelMap[msg.Topic] == nil {
			channelMap[msg.Topic] = make(map[uint8]channel)
		}
		channelMap[msg.Topic][msg.Partition] = channel{msg.Topic, msg.Partition, clusterNodes[0], followerIPs, true}

		// REQUEST TO FIND THE LEADER OF A TOPIC/PARTITION COMBO
	} else if msg.Type == message.GET_LEADER {
		leaderIP := channelMap[msg.Topic][msg.Partition].leaderIP
		providerMsg := message.Message{ID: config.ManagerIP, Type: message.GET_LEADER, Text: leaderIP, Topic: msg.Topic, Role: message.LEADER, Partition: msg.Partition, Timestamp: time.Now()}
		enc := gob.NewEncoder(conn)
		err := enc.Encode(providerMsg)
		if err != nil {
			fmt.Println(err)
		}

		// GETS A FAILURE FROM ANOTHER NODE
	} else if msg.Type == message.FOLLOWER_NODE_DOWN {
		followerIP := msg.Text
		affectedChannel := channelMap[msg.Topic][msg.Partition]
		affectedChannel.followerIPs = removeIPFromStringList(followerIP, affectedChannel.followerIPs)
		newFollower, err := freeNodes.getFreeNodes(1)
		affectedChannel.followerIPs += newFollower[0] + ";"
		if err != nil {
			fmt.Println(err)
		}
		leaderMsg := message.Message{ID: config.ManagerIP, Type: message.FOLLOWER_NODE_DOWN, Text: newFollower[0], Topic: msg.Topic, Role: message.LEADER, Partition: msg.Partition, Timestamp: time.Now()}
		enc := gob.NewEncoder(conn)
		err = enc.Encode(leaderMsg)
		if err != nil {
			fmt.Println(err)
		}
		// LEADER HAS FAILED
	} else if msg.Type == message.LEADER_NODE_DOWN {
		leaderIP := msg.Text
		promotedNodeIP := ""
		affectedChannel := channel{}
		for _, partition := range channelMap {
			for _, channel := range partition {
				if channel.leaderIP == leaderIP {
					affectedChannel = channel
					promotedNodeIP = strings.Split(channel.followerIPs, ";")[0]
					channel.leaderIP = promotedNodeIP
					channel.followerIPs = removeIPFromStringList(promotedNodeIP, channel.followerIPs)
				}
			}
		}
		newFollower, err := freeNodes.getFreeNodes(1)
		affectedChannel.followerIPs += newFollower[0] + ";"
		if err != nil {
			fmt.Println(err)
		}
		promoteMsg := message.Message{ID: config.ManagerIP, Type: message.PROMOTE, Text: newFollower[0], Topic: msg.Topic, Role: message.LEADER, Partition: msg.Partition, Timestamp: time.Now()}
		enc := gob.NewEncoder(conn)
		err = enc.Encode(promoteMsg)
		if err != nil {
			fmt.Println(err)
		}
	}
	conn.Close()
}

// removes an ip from an ip list seperated by ';'
func removeIPFromStringList(removeIP string, ipList string) (newIPList string) {
	s := strings.Split(ipList, ";")
	for _, ip := range s {
		if ip == removeIP {
			continue
		} else if ip != "" {
			newIPList += ip + ";"
		}
	}
	return newIPList
}

/* provideMsg
 * para message string
 *
 * Desc:
 * 		send the message to kafka node by remoteIPPort
 */
func provideMsg(remoteIPPort string, outgoing message.Message) error {
	conn, err := net.Dial("tcp", remoteIPPort)
	if err != nil {
		println("Fail to connect kafka manager" + remoteIPPort)
		return err
	}
	defer conn.Close()

	// send message
	enc := gob.NewEncoder(conn)
	err = enc.Encode(outgoing)
	if err != nil {
		log.Fatal("encode error:", err)
	}

	// response
	dec := gob.NewDecoder(conn)
	response := &message.Message{}
	dec.Decode(response)
	fmt.Printf("Response : {senderID:%s, response:%s}\n", response.ID, response.Text)

	return nil
}

// Initialize starts the node as a Manager node in the network
func Initialize() bool {
	configFilename := os.Args[1]
	readConfigJSON(configFilename)

	println("Manager", config.ManagerNodeID, "starts")
	go listenForMessages()

	return true
}

func main() {
	Initialize()

	// terminal controller like shell
	reader := bufio.NewReader(os.Stdin)
	for {
		cmd, _ := reader.ReadString('\n')
		if cmd == "broker\n" {
			fmt.Println(channelMap)
		}
	}
}
