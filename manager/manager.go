package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/rpc"
	"os"
	"strings"
	"sync"

	message "../lib/message"

	"github.com/DistributedClocks/GoVector/govec"
	"github.com/DistributedClocks/GoVector/govec/vrpc"
)

// Error types

// ErrInsufficientFreeNodes denotes that someone requested more free nodes
// than are currently available
var ErrInsufficientFreeNodes = errors.New("insufficient free nodes")

type ManagerRPCServer int

type ManagerNode struct {
	ManagerNodeID ManagerNodeID
	ManagerIP     net.Addr
	TopicMap      map[TopicID]Topic
	ManagerPeers  map[ManagerNodeID]net.Addr
	BrokerNodes   map[BrokerID]net.Addr
	TopicMutex    *sync.Mutex
	ManagerMutex  *sync.Mutex
	BrokerMutex   *sync.Mutex
	BACKUP        bool
}

type TopicID string

type BrokerID string

type ManagerNodeID string

type Topic []*Partition

type Partition struct {
	LeaderIP    net.Addr
	FollowerIPs []net.Addr
}

type configSetting struct {
	ManagerNodeID     string
	ManagerIP         string
	PeerManagerNodeIP []string
	BACKUP            bool
}

var config configSetting

var manager *ManagerNode

var logger *govec.GoLog
var loggerOptions govec.GoLogOptions

//----------------------------------------------------------------------------------------------------------------------------------------------

// manager keeps track of all free nodes (i.e. nodes that are ready to be 'provisioned'.
// If this manager goes down, it will need to ensure that the other managers
// have the correct copy of this set.
// Initialize this set to a bunch of free nodes on manager startup, probably via a json config.
// var freeNodes = freeNodesSet{
// 	set: make(map[string]bool),
// }

// var channelMap = make(map[string]map[uint8]channel)

// // just to help unpack maps from other nodes
// type backupMap struct {
// 	backup map[string]map[uint8]channel
// 	msg    string
// }
// type channel struct {
// 	topicName   string
// 	partition   uint8
// 	leaderIP    string
// 	followerIPs string
// 	available   bool
// }

//var topicMap =
// freeNodesSet contains a set of nodes for the entire topology.
// if set[nodeIP] == true,  node is free
// if set[nodeIP] == false, node is busy
//
// Operations on freeNodesSet are atomic.
// type freeNodesSet struct {
// 	set   map[string]bool
// 	mutex sync.Mutex
// }

// getFreeNodes returns a slice of free nodes with length num
// If the amount of available free nodes is < num,
// return an InsufficientFreeNodes error.
// Once these nodes are returned to caller, there are marked as busy to avoid concurrency issues.
// If the caller gets free nodes using this function, and then decides not to use them, it must
// manually de-allocate each node by calling setNodeAsFree.

// TODO: Make this DHT instead of first seen first provisioned.
// func (s *freeNodesSet) getFreeNodes(num int) ([]string, error) {
// 	s.mutex.Lock()
// 	defer s.mutex.Unlock()

// 	var nodes []string
// 	var full bool
// 	for ip, free := range s.set {
// 		if free {
// 			nodes = append(nodes, ip)
// 			s.set[ip] = false
// 		}

// 		if len(nodes) == num {
// 			full = true
// 			break
// 		}
// 	}

// 	if full {
// 		return nodes, nil
// 	} else {
// 		return nil, ErrInsufficientFreeNodes
// 	}
// }

// func (s *freeNodesSet) isFree(ip string) bool {
// 	s.mutex.Lock()
// 	defer s.mutex.Unlock()
// 	if free, ok := s.set[ip]; ok {
// 		return free
// 	}
// 	// If we asked for an ip thats not even in the registered set of nodes,
// 	// just return false as if node is busy.
// 	return false
// }

// func (s *freeNodesSet) setNodeAsFree(ip string) {
// 	s.mutex.Lock()
// 	defer s.mutex.Unlock()
// 	if _, ok := s.set[ip]; ok {
// 		s.set[ip] = true
// 	}
// 	// No-op if ip is not in registered set.
// }

// func (s *freeNodesSet) setNodeAsBusy(ip string) {
// 	s.mutex.Lock()
// 	defer s.mutex.Unlock()
// 	if _, ok := s.set[ip]; ok {
// 		s.set[ip] = false
// 	}
// 	// No-op if ip is not in registered set.
// }

// func (s *freeNodesSet) addFreeNode(ip string) {
// 	s.mutex.Lock()
// 	defer s.mutex.Unlock()
// 	if _, ok := s.set[ip]; !ok {
// 		// Only add this free node if its not already in the set of free nodes.
// 		s.set[ip] = true
// 	}
// 	// If ip is already in the set, no-op.
// }

/* listenManagers()
 * Desc:
 * 		this is a goroutine dealing with other manger nodes
 *
 * @para IPPort [string]:
 *		The list of neighbouring managers
 */
// func listenForMessages() error {
// 	listener, err := net.Listen("tcp", config.ManagerIP)
// 	if err != nil {
// 		fmt.Println(err)
// 		return err
// 	}
// 	defer listener.Close()

// 	fmt.Println("Listening to other managers at :" + config.ManagerIP)

// 	for {
// 		conn, err := listener.Accept()
// 		if err != nil {
// 			fmt.Println(err)
// 			return err
// 		}

// 		go processMessage(conn)
// 	}
// }

/* dealManager
 * @para conn:
 *		the ip and port opened for messages from provider routine
 *
 * Desc: Handles all messages from other managers
 *
 */

// func processMessage(conn net.Conn) {
// 	// check if msg is from other manager
// 	senderIP := conn.RemoteAddr().String()
// 	// first handle cases if this is the manager backup
// 	// THIS variable can be turned off through a special manager msg
// 	// decode the serialized message from the connection

// 	// decode the infomation into initialized message
// 	// if-else branch to deal with different types of messages

// 	// NETWORK INFO MSG (RANDOM/TESTING)
// 	} else if msg.Type == message.NEW_TOPIC {
// 		fmt.Printf("Receive Provider Msg: {pID:%s, type:%s, topic:%s}\n", msg.ID, msg.Type, msg.Topic)
// 		// get free nodes (chose based on algo)
// 		clusterNodes, err := freeNodes.getFreeNodes(1)
// 		if err != nil {
// 			fmt.Println(err)
// 		}
// 		followerIPs := ""
// 		// contact each node to set their role
// 		for idx, ip := range clusterNodes {
// 			fmt.Println(ip)
// 			if idx == 0 {
// 				continue
// 			} else {
// 				followerIPs += ip
// 			}
// 		}
// 		startBrokerMsg := message.Message{ID: config.ManagerIP, Type: message.START_LEADER, Text: followerIPs, Topic: msg.Topic, Role: message.LEADER, Partition: msg.Partition, Timestamp: time.Now()}
// 		fmt.Println(startBrokerMsg)
// 		enc := gob.NewEncoder(conn)
// 		err = enc.Encode(startBrokerMsg)
// 		if err != nil {
// 			fmt.Println(err)
// 		}
// 		if channelMap[msg.Topic] == nil {
// 			channelMap[msg.Topic] = make(map[uint8]channel)
// 		}
// 		channelMap[msg.Topic][msg.Partition] = channel{msg.Topic, msg.Partition, clusterNodes[0], followerIPs, true}

// 		// REQUEST TO FIND THE LEADER OF A TOPIC/PARTITION COMBO
// 	} else if msg.Type == message.GET_LEADER {
// 		leaderIP := channelMap[msg.Topic][msg.Partition].leaderIP
// 		providerMsg := message.Message{ID: config.ManagerIP, Type: message.GET_LEADER, Text: leaderIP, Topic: msg.Topic, Role: message.LEADER, Partition: msg.Partition, Timestamp: time.Now()}
// 		enc := gob.NewEncoder(conn)
// 		err := enc.Encode(providerMsg)
// 		if err != nil {
// 			fmt.Println(err)
// 		}

// 		// GETS A FAILURE FROM ANOTHER NODE
// 	} else if msg.Type == message.FOLLOWER_NODE_DOWN {
// 		followerIP := msg.Text
// 		affectedChannel := channelMap[msg.Topic][msg.Partition]
// 		affectedChannel.followerIPs = removeIPFromStringList(followerIP, affectedChannel.followerIPs)
// 		newFollower, err := freeNodes.getFreeNodes(1)
// 		affectedChannel.followerIPs += newFollower[0] + ";"
// 		if err != nil {
// 			fmt.Println(err)
// 		}
// 		leaderMsg := message.Message{ID: config.ManagerIP, Type: message.FOLLOWER_NODE_DOWN, Text: newFollower[0], Topic: msg.Topic, Role: message.LEADER, Partition: msg.Partition, Timestamp: time.Now()}
// 		enc := gob.NewEncoder(conn)
// 		err = enc.Encode(leaderMsg)
// 		if err != nil {
// 			fmt.Println(err)
// 		}
// 		// LEADER HAS FAILED
// 	} else if msg.Type == message.LEADER_NODE_DOWN {
// 		leaderIP := msg.Text
// 		promotedNodeIP := ""
// 		affectedChannel := channel{}
// 		for _, partition := range channelMap {
// 			for _, channel := range partition {
// 				if channel.leaderIP == leaderIP {
// 					affectedChannel = channel
// 					promotedNodeIP = strings.Split(channel.followerIPs, ";")[0]
// 					channel.leaderIP = promotedNodeIP
// 					channel.followerIPs = removeIPFromStringList(promotedNodeIP, channel.followerIPs)
// 				}
// 			}
// 		}
// 		newFollower, err := freeNodes.getFreeNodes(1)
// 		affectedChannel.followerIPs += newFollower[0] + ";"
// 		if err != nil {
// 			fmt.Println(err)
// 		}
// 		promoteMsg := message.Message{ID: config.ManagerIP, Type: message.PROMOTE, Text: newFollower[0], Topic: msg.Topic, Role: message.LEADER, Partition: msg.Partition, Timestamp: time.Now()}
// 		enc := gob.NewEncoder(conn)
// 		err = enc.Encode(promoteMsg)
// 		if err != nil {
// 			fmt.Println(err)
// 		}
// 	} else if msg.Type == message.MANAGER_SYNC {
// 		enc := gob.NewEncoder(conn)
// 		sendBackup := &backupMap{backup: channelMap, msg: "normal"}
// 		err := enc.Encode(sendBackup)
// 		if err != nil {
// 			fmt.Println(err)
// 		}
// 	}

// 	conn.Close()
// }

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

//-------------------------------------------------------------------------------------------------------------------------------

var tempManagerID ManagerNodeID

/* readConfigJSON
 * Desc:
 *		read the configration from file into struct config
 *
 * @para configFile: relative url of configuration file
 * @retrun: None
 */
func readConfigJSON(configFile string) error {
	configByte, err := ioutil.ReadFile(configFile)

	if err != nil {
		return err
	}

	if err := json.Unmarshal(configByte, &config); err != nil {
		return err
	}
	return nil
}

// Initialize starts the node as a Manager node in the network
func Initialize(configFileName string) error {

	if err := readConfigJSON(configFileName); err != nil {
		return fmt.Errorf("initialize error: %v", err)
	}

	println("Manager", config.ManagerIP, "starts")

	managerIP, err := net.ResolveTCPAddr("tcp", config.ManagerIP)

	if err != nil {
		return err
	}

	manager = &ManagerNode{
		ManagerNodeID: ManagerNodeID(config.ManagerNodeID),
		ManagerIP:     managerIP,
		TopicMap:      make(map[TopicID]Topic),
		ManagerPeers:  make(map[ManagerNodeID]net.Addr),
		BrokerNodes:   make(map[BrokerID]net.Addr),
		TopicMutex:    &sync.Mutex{},
		ManagerMutex:  &sync.Mutex{},
		BrokerMutex:   &sync.Mutex{},
		BACKUP:        config.BACKUP,
	}

	logger = govec.InitGoVector(string(manager.ManagerNodeID), fmt.Sprintf("%v-logfile", manager.ManagerNodeID), govec.GetDefaultConfig())
	loggerOptions = govec.GetDefaultLogOptions()

	for i, v := range config.PeerManagerNodeIP {
		fmt.Println(len(config.PeerManagerNodeIP))
		fmt.Println("idx", i, v)
	}

	for _, mPeer := range config.PeerManagerNodeIP {
		if err := manager.addManagerPeerRPC(mPeer); err != nil {
			//TODO ignore error for now
			fmt.Printf("Failed to connect to Manager Peer - %v: %v\n", mPeer, err)
			continue
		}
	}

	spawnRPCServer()

	return nil

	// listenForMessages()
}

func (m *ManagerNode) addManagerPeerRPC(managerPeerAddr string) (err error) {

	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("failed to make rpc connection: %v", p)
		}
	}()

	rAddr, err := net.ResolveTCPAddr("tcp", managerPeerAddr)
	if err != nil {
		return err
	}

	for _, v := range m.ManagerPeers {
		if v == rAddr {
			return fmt.Errorf("Peer address already exists in Manager Peer Mapping")
		}
	}

	rpcClient, err := vrpc.RPCDial("tcp", rAddr.String(), logger, loggerOptions)
	defer rpcClient.Close()
	if err != nil {
		return err
	}

	var res ManagerNodeID

	fmt.Println("Ready to call rpc")

	managerTCPAddr, ok := manager.ManagerIP.(*net.TCPAddr)

	if !ok {
		return fmt.Errorf("Cannot type-assert net.Conn interface to TCPAddr Pointer")
	}

	// 2-phase Commit

	if err := rpcClient.Call("ManagerRPCServer.AddPeerRequest", manager.ManagerNodeID, &res); err != nil {
		return err
	}

	if err := rpcClient.Call("ManagerRPCServer.AddPeerCommit", managerTCPAddr, &res); err != nil {
		return err
	}

	fmt.Println("Manager Node ID", res)

	manager.ManagerMutex.Lock()
	manager.ManagerPeers[res] = rAddr
	manager.ManagerMutex.Unlock()

	fmt.Printf("Added Peer request: %v - %v\n", res, rAddr.String())

	// if err:= manager.addBroker(rAddr); err!=nil{
	// 	return err
	// }

	return nil
}

func (mrpc *ManagerRPCServer) AddPeerRequest(id ManagerNodeID, res *ManagerNodeID) error {
	tempManagerID = id
	*res = manager.ManagerNodeID
	return nil
}

func (mrpc *ManagerRPCServer) AddPeerCommit(addr net.TCPAddr, res *ManagerNodeID) error {
	fmt.Println("incoming Addr", addr.String())
	fmt.Println("Start add peer RPC", addr.String())

	manager.ManagerPeers[tempManagerID] = &addr

	fmt.Printf("Completed Add Peer request: %v - %v\n", tempManagerID, addr.String())

	tempManagerID = ""

	*res = manager.ManagerNodeID
	return nil
}

func (mrpc *ManagerRPCServer) AddPeerReady(addr net.TCPAddr, res *ManagerNodeID) error {
	fmt.Println("Peer is ready")
	*res = manager.ManagerNodeID
	return nil
}

func spawnRPCServer() error {
	mRPC := new(ManagerRPCServer)
	server := rpc.NewServer()
	server.Register(mRPC)

	tcpAddr, err := net.ResolveTCPAddr("tcp", config.ManagerIP)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
	}

	listener, err := net.ListenTCP("tcp", tcpAddr)
	defer listener.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
	}

	fmt.Printf("Serving RPC Server at: %v\n", tcpAddr.String())

	vrpc.ServeRPCConn(server, listener, logger, loggerOptions)

	return nil
}

func (mrpc *ManagerRPCServer) Ping(addr string, ack *bool) error {
	fmt.Println("incoming Addr", addr)
	*ack = true
	return nil
}

func (mrpc *ManagerRPCServer) RegisterBroker(m *message.Message, ack *bool) error {
	*ack = false

	fmt.Println(m.Text)
	fmt.Println(m.ID)

	rAddr, err := net.ResolveTCPAddr("tcp", m.Text)

	rpcClient, err := vrpc.RPCDial("tcp", rAddr.String(), logger, loggerOptions)

	defer rpcClient.Close()
	if err != nil {
		return err
	}

	if err := rpcClient.Call("BrokerRPCServer.Ping", config.ManagerIP, &ack); err != nil {
		return nil
	}

	if err := manager.addBroker(BrokerID(m.ID), rAddr); err != nil {
		return err
	}
	return nil
}

func (m *ManagerNode) addBroker(nodeID BrokerID, brokerAddr net.Addr) error {
	manager.BrokerMutex.Lock()
	defer manager.BrokerMutex.Unlock()

	/*
	* 	No exist check due to Broker Rejoin
	 */

	// if _, exist := manager.BrokerNodes[nodeID]; exist{
	// 	return fmt.Errorf("Broker has already been registered")
	// }

	manager.BrokerNodes[nodeID] = brokerAddr
	fmt.Printf("sucessfully added broker: %v - %v\n", nodeID, brokerAddr)

	return nil
}

// func (mrpc *ManagerRPCServer) RegisterBroker(m *message.Message, ack *bool) error {

// }

func main() {

	if len(os.Args) != 2 {
		fmt.Println("Please provide config filename. e.g. m1.json, m2.json")
		return
	}

	configFileName := os.Args[1]

	Initialize(configFileName)
}
