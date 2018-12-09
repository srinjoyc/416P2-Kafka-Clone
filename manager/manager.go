package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/rpc"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru"

	m "../lib/message"
	"github.com/serialx/hashring"

	"github.com/DistributedClocks/GoVector/govec"
	"github.com/DistributedClocks/GoVector/govec/vrpc"
)

// Error types

// ErrInsufficientFreeNodes denotes that someone requested more free nodes
// than are currently available
var ErrInsufficientFreeNodes = errors.New("insufficient free nodes")

type ManagerRPCServer int

type ManagerNode struct {
	ManagerNodeID    ManagerNodeID
	ManagerPeers     map[ManagerNodeID]net.Addr
	ManagerIP        net.Addr
	TopicMap         map[TopicID]Topic
	BrokerNodes      map[BrokerID]net.Addr
	BrokerNodesIP    []string
	TopicMutex       *sync.Mutex
	ManagerMutex     *sync.Mutex
	BrokerMutex      *sync.Mutex
	TransactionCache *lru.Cache // A Transaction to State Mapping, key is sha1 hash of the Message object, and value would be the state
}

type State uint

const (
	READY State = iota
	WAIT
	PREPARE
	COMMIT
	ABORT
)

const cacheSize = 10

type TopicID string

type BrokerID string

type ManagerNodeID string

type Topic []*Partition

type Partition struct {
	LeaderIP    string
	FollowerIPs []string
}

type configSetting struct {
	ManagerNodeID     string
	ManagerIP         string
	PeerManagerNodeIP string
	BACKUP            bool
}

var config configSetting
var manager *ManagerNode
var logger *govec.GoLog
var loggerOptions govec.GoLogOptions
var ring *hashring.HashRing

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
	}

	cache, err := lru.New(cacheSize)

	if err != nil {
		return err
	}

	manager.TransactionCache = cache

	logger = govec.InitGoVector(string(manager.ManagerNodeID), fmt.Sprintf("%v-logfile", manager.ManagerNodeID), govec.GetDefaultConfig())
	loggerOptions = govec.GetDefaultLogOptions()

	if len(config.PeerManagerNodeIP) != 0 {
		if err := manager.registerPeerRequest(config.PeerManagerNodeIP); err != nil {
			return err
		}
	}

	// for i, v := range config.PeerManagerNodeIP {
	// 	fmt.Println(len(config.PeerManagerNodeIP))
	// 	fmt.Println("idx", i, v)
	// }

	// for _, mPeer := range config.PeerManagerNodeIP {
	// 	if err := manager.addManagerPeerRPC(mPeer); err != nil {
	// 		//TODO ignore error for now
	// 		fmt.Printf("Failed to connect to Manager Peer - %v: %v\n", mPeer, err)
	// 		continue
	// 	}
	// }

	var emptylist []string
	ring = hashring.New(emptylist)

	spawnRPCServer()
	return nil

	// listenForMessages()
}

func (mn *ManagerNode) registerPeerRequest(managerPeerAddr string) (err error) {
	fmt.Println("registerPeerRequest")
	rAddr, err := net.ResolveTCPAddr("tcp", managerPeerAddr)
	if err != nil {
		return err
	}
	rpcClient, err := vrpc.RPCDial("tcp", rAddr.String(), logger, loggerOptions)
	defer rpcClient.Close()
	if err != nil {
		return err
	}
	var peerList map[string]string

	newMsg := m.Message{
		ID:        string(mn.ManagerNodeID),
		Text:      mn.ManagerIP.String(),
		Proposer:  string(mn.ManagerNodeID),
		Timestamp: time.Now(),
	}

	fmt.Println("Ready to invoke service")

	if err := rpcClient.Call("ManagerRPCServer.RegisterPeer", newMsg, &peerList); err != nil {
		return err
	}
	fmt.Println("Done Invoking")

	manager.ManagerMutex.Lock()
	for k, v := range peerList {
		// Skip registering own IP to the Peer Map
		if ManagerNodeID(k) == manager.ManagerNodeID {
			continue
		}

		tcpAddr, err := net.ResolveTCPAddr("tcp", v)
		if err != nil {
			continue
		}
		manager.ManagerPeers[ManagerNodeID(k)] = tcpAddr
	}
	manager.ManagerMutex.Unlock()

	fmt.Println("Done RegisterPeerRequest")

	fmt.Println("PrintPeerMap")
	fmt.Println(manager.ManagerPeers)

	return nil
}



func (mrpc *ManagerRPCServer) RegisterPeer(msg *m.Message, peerList *map[string]string) error {
	if err := mrpc.threePC("RegisterPeer", msg); err != nil {
		return err
	}
	manager.ManagerMutex.Lock()
	for k, v := range manager.ManagerPeers {
		(*peerList)[string(k)] = v.String()
	}
	manager.ManagerMutex.Unlock()
	(*peerList)[string(manager.ManagerNodeID)] = manager.ManagerIP.String()
	return nil
}


func (mrpc *ManagerRPCServer) threePC(serviceMethod string, msg *m.Message) error {
	// canCommitPhase
	if err := mrpc.canCommit("RegisterPeer", msg); err != nil {
		return err
	}
	// preCommitPhase
	if err := mrpc.preCommit("RegisterPeer", msg); err != nil {
		return err
	}
	// commitPhase
	if err := mrpc.commit("RegisterPeer", msg); err != nil {
		return err
	}
	return nil
}

func (mrpc *ManagerRPCServer) canCommit(serviceMethod string, msg *m.Message) error {
	// canCommitPhase
	fmt.Println("CanCommitPhase")
	var wg sync.WaitGroup
	errorCh := make(chan error, 1)
	manager.ManagerMutex.Lock()
	fmt.Println("Break1")
	for _, managerPeer := range manager.ManagerPeers {
		wg.Add(1)
		go func() {
			// Prevent Closure
			managerPeerAddr := managerPeer
			defer func() {
				if p := recover(); p != nil {
					errorCh <- fmt.Errorf("bad connection - %v: %v", managerPeerAddr, p)
				}
			}()
			defer wg.Done()
			rpcClient, err := vrpc.RPCDial("tcp", managerPeerAddr.String(), logger, loggerOptions)
			defer rpcClient.Close()
			if err != nil {
				errorCh <- fmt.Errorf("manager peer - %v: %v", managerPeerAddr, err)
				return
			}
			var ack bool
			if err := RpcCallTimeOut(rpcClient, fmt.Sprintf("ManagerRPCServer.CanCommit%vRPC", serviceMethod), msg, &ack); err != nil {
				errorCh <- fmt.Errorf("manager peer - %v: %v", managerPeerAddr, err)
				return
			}
			if !ack {
				errorCh <- fmt.Errorf("peer - %v disagrees", managerPeerAddr)
			}
		}()
	}
	fmt.Println("Break2")
	manager.TransactionCache.Add(msg.Hash(), WAIT)
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	fmt.Println("Break3")
	select {
	case err := <-errorCh:
		manager.TransactionCache.Add(msg.Hash(), ABORT)
		manager.ManagerMutex.Unlock()
		fmt.Println("Abort Transaction")
		return fmt.Errorf("transaction aborted: %v", err)
	case <-c:
		fmt.Println("CanCommitPhase Done")
	}
	fmt.Println("Break4")
	manager.ManagerMutex.Unlock()

	// Local canCommit
	var localAck = false
	method := reflect.ValueOf(mrpc).MethodByName(fmt.Sprintf("CanCommit%vRPC", serviceMethod))
	if err := method.Call([]reflect.Value{reflect.ValueOf(msg), reflect.ValueOf(&localAck)})[0].Interface(); err != nil {
		manager.TransactionCache.Add(msg.Hash(), ABORT)
		return fmt.Errorf("coordinator failed: transaction aborted: %v", err)
	}
	if !localAck {
		manager.TransactionCache.Add(msg.Hash(), ABORT)
		return fmt.Errorf("transaction aborted: peerID exists in coordinator")
	}

	return nil
}

func (mrpc *ManagerRPCServer) preCommit(serviceMethod string, msg *m.Message) error {
	// preCommitPhase
	fmt.Println("PreCommit Phase")
	errorCh := make(chan error, 1)
	wg := sync.WaitGroup{}

	for _, managerPeer := range manager.ManagerPeers {
		wg.Add(1)
		go func() {
			managerPeerAddr := managerPeer
			defer wg.Done()
			rpcClient, err := vrpc.RPCDial("tcp", managerPeerAddr.String(), logger, loggerOptions)
			defer rpcClient.Close()
			if err != nil {
				errorCh <- err
				return
			}
			var ack bool
			if err := RpcCallTimeOut(rpcClient, fmt.Sprintf("ManagerRPCServer.PreCommit%vRPC", serviceMethod), msg, &ack); err != nil {
				errorCh <- err
				return
			}
			if !ack {
				errorCh <- fmt.Errorf("peer disagrees")
			}
		}()
	}

	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()

	select {
	case err := <-errorCh:
		manager.TransactionCache.Add(msg.Hash(), ABORT)
		return fmt.Errorf("transaction aborted: %v", err)
	case <-c:
		fmt.Println("PreCommit Phase Done")
	}
	return nil
}

func (mrpc *ManagerRPCServer) commit(serviceMethod string, msg *m.Message) error {
	errorCh := make(chan error, 1)
	wg := sync.WaitGroup{}
	for _, managerPeer := range manager.ManagerPeers {
		wg.Add(1)
		go func() {
			managerPeerAddr:=managerPeer
			defer wg.Done()
			rpcClient, err := vrpc.RPCDial("tcp", managerPeerAddr.String(), logger, loggerOptions)
			defer rpcClient.Close()
			if err != nil {
				errorCh <- err
				return
			}
			var ack bool
			if err := RpcCallTimeOut(rpcClient, fmt.Sprintf("ManagerRPCServer.Commit%vRPC", serviceMethod), msg, &ack); err != nil {
				errorCh <- err
				return
			}
			if !ack {
				errorCh <- fmt.Errorf("peer disagrees")
			}
		}()

	}

	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()


	select {
	case err := <-errorCh:
		manager.TransactionCache.Add(msg.Hash(), ABORT)
		return fmt.Errorf("transaction aborted: %v", err)
	case <-c:
		fmt.Println("Commit Phase Done")
	}


	// Local Commit
	var ack bool

	method := reflect.ValueOf(mrpc).MethodByName(fmt.Sprintf("Commit%vRPC", serviceMethod))
	if err := method.Call([]reflect.Value{reflect.ValueOf(msg), reflect.ValueOf(&ack)})[0].Interface(); err != nil {
		manager.TransactionCache.Add(msg.Hash(), ABORT)
		return fmt.Errorf("coordinator failed: transaction aborted: %v", err)
	}
	if !ack {
		manager.TransactionCache.Add(msg.Hash(), ABORT)
		return fmt.Errorf("transaction aborted: peerID exists in coordinator")
	}
	return nil
}

//-----------------------------------------------------------------------------------------------------------------------------

func (mrpc *ManagerRPCServer) CanCommitRegisterPeerRPC(msg *m.Message, ack *bool) error {
	fmt.Println("CanCommitRegisterPeerRPC")
	*ack = false
	peerManagerID := ManagerNodeID(msg.ID)

	manager.ManagerMutex.Lock()
	defer manager.ManagerMutex.Unlock()

	if _, exist := manager.ManagerPeers[peerManagerID]; exist {
		fmt.Println("Manager", peerManagerID)
		manager.TransactionCache.Add(msg.Hash(), ABORT)
		return nil
	}

	*ack = true
	manager.TransactionCache.Add(msg.Hash(), READY)
	return nil
}

func (mrpc *ManagerRPCServer) PreCommitRegisterPeerRPC(msg *m.Message, ack *bool) error {
	*ack = false
	manager.TransactionCache.Add(msg.Hash(), PREPARE)
	*ack = true
	return nil
}

func (mrpc *ManagerRPCServer) CommitRegisterPeerRPC(msg *m.Message, ack *bool) error {
	*ack = false
	peerManagerID := ManagerNodeID(msg.ID)

	peerManagerAddr, err := net.ResolveTCPAddr("tcp", msg.Text)

	fmt.Println(peerManagerAddr, peerManagerID)

	if err != nil {
		return err
	}

	manager.ManagerMutex.Lock()
	manager.ManagerPeers[peerManagerID] = peerManagerAddr
	manager.ManagerMutex.Unlock()

	ring = ring.AddNode(peerManagerAddr.String())

	fmt.Printf("added peer - %v - %v\n", peerManagerID, peerManagerAddr)
	fmt.Println("Peer Map: ", manager.ManagerPeers)

	manager.TransactionCache.Add(msg.Hash(), COMMIT)
	*ack = true
	return nil
}

//----------------------------------------------------------------------------------------------------------------------

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

//------------------------------------------------------------------------------------------------------------------

func (mrpc *ManagerRPCServer) Ack(addr string, ack *bool) error {
	fmt.Println("incoming Addr", addr)
	*ack = true
	return nil
}

func (mrpc *ManagerRPCServer) RegisterBroker(msg *m.Message, ack *bool) error {
	*ack = false

	fmt.Println(msg.Text)
	fmt.Println(msg.ID)

	rAddr, err := net.ResolveTCPAddr("tcp", msg.Text)
	if err != nil {
		return err
	}
	newMsg := m.Message{
		Text: manager.ManagerIP.String(),
	}
	rpcClient, err := vrpc.RPCDial("tcp", rAddr.String(), logger, loggerOptions)

	// defer rpcClient.Close()
	// if err != nil {
	// 	return err
	// }
	fmt.Println("Ready to Ping")

	c := make(chan error, 1)
	go func() { c <- rpcClient.Call("BrokerRPCServer.Ping", newMsg, &ack) }()

	select {
	case err := <-c:

		if err != nil {
			return nil
		}
		// use err and reply
	case <-time.After(time.Duration(3) * time.Second):
		// call timed out
	}

	fmt.Println("Done Ping")

	// add the broker informtaion into list
	// manager.addBroker(BrokerID(m.ID), rAddr)

	if err := manager.addBroker(BrokerID(msg.ID), rAddr); err != nil {
		return err
	}

	return nil
}

func (mn *ManagerNode) addBroker(nodeID BrokerID, brokerAddr net.Addr) error {
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

func (mrpc *ManagerRPCServer) CreateNewTopic(request *m.Message, response *m.Message) error {
	println(request.Topic)
	response.ID = config.ManagerNodeID
	response.Role = m.MANAGER
	response.Timestamp = time.Now()
	response.Type = m.MANAGER_RESPONSE_TO_PROVIDER

	if int(request.Partition) > len(manager.BrokerNodes) {
		response.Text = "At most " + strconv.Itoa(len(manager.BrokerNodes)) + " partitions"
		return ErrInsufficientFreeNodes
		// response.Ack = false
	} else {
		response.IPs = getHashingNodes(request.Topic, int(request.Partition))
		// response.Ack = true

		topicGroup := Partition{LeaderIP: response.IPs[0], FollowerIPs: response.IPs[1:]}
		manager.TopicMutex.Lock()
		manager.TopicMap[TopicID(request.Topic)] = append(manager.TopicMap[TopicID(request.Topic)], &topicGroup)
		manager.TopicMutex.Unlock()
	}

	printTopicMap()

	return nil
}

func getHashingNodes(key string, replicaCount int) []string {
	var list []string
	for _, v := range manager.BrokerNodes {
		list = append(list, v.String())
	}

	ring := hashring.New(list)
	server, _ := ring.GetNodes(key, replicaCount)

	return server
}

func printTopicMap() {
	println("------------")
	for key, value := range manager.TopicMap {
		println("topic:", key)
		for _, v := range value {
			println("LeaderIP:", v.LeaderIP)
			fmt.Printf("%v\n", v.FollowerIPs)
		}
	}
	println("------------")
}

func shell() {
	reader := bufio.NewReader(os.Stdin)
	for {
		cmd, _ := reader.ReadString('\n')
		if cmd == "broker\n" {
			fmt.Printf("%v\n", manager.BrokerNodes)
		} else if cmd == "ring\n" {

			server, _ := ring.GetNode("my_key")
			println(server)

		} else if cmd == "ring2\n" {
			var v string
			fmt.Scanf("%s", &v)

			var n int
			fmt.Scanf("%d", &n)
			server := getHashingNodes(v, n)
			fmt.Printf("%v\n", server)
		} else if cmd == "topicmap\n" {

		}
	}
}

func main() {

	if len(os.Args) != 2 {
		fmt.Println("Please provide config filename. e.g. m1.json, m2.json")
		return
	}
	configFileName := os.Args[1]

	go shell()

	if err := Initialize(configFileName); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func RpcCallTimeOut(rpcClient *rpc.Client, serviceMethod string, args interface{}, reply interface{}) error {
	rpcCall := rpcClient.Go(serviceMethod, args, reply, nil)

	select {
	case doneCall := <-rpcCall.Done:
		if doneCall.Error != nil {
			return doneCall.Error
		}
	case <-time.After(time.Duration(3) * time.Second):
		return fmt.Errorf("rpc call: %v has timed out", rpcCall.ServiceMethod)
	}
	return nil
}
