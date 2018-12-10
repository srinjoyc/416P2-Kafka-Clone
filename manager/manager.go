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

	lru "github.com/hashicorp/golang-lru"

	m "../lib/message"
	"github.com/serialx/hashring"

	"github.com/DistributedClocks/GoVector/govec"
	"github.com/DistributedClocks/GoVector/govec/vrpc"
)

// Error types

// ErrInsufficientFreeNodes denotes that someone requested more free nodes
// than are currently available
var ErrInsufficientFreeNodes = errors.New("insufficient free nodes")

type ConnectionErr struct {
	Addr   net.Addr
	NodeID ManagerNodeID
	Err    error
}

type TransactionErr struct {
	Err error
}

type RPCTimedout struct {
	ServiceMethod string
}

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

type Topic struct {
	LeaderIP     string
	FollowerIPs  []string
	PartitionNum uint8
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
	defer func() {
		if p := recover(); p != nil {
			err = NewConnectionErr(ManagerNodeID("PrimaryManger"), rAddr, fmt.Errorf("%v", p))
		}
	}()
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
	if err := mrpc.threePC("RegisterPeer", msg, manager.ManagerPeers); err != nil {
		switch err.(type) {
		case *TransactionErr:
			e := err.(*TransactionErr)
			switch (e.Err).(type) {
			case *ConnectionErr:
				fmt.Println("attempt to retry by delteig peer")
				ce := (e.Err).(*ConnectionErr)

				deleteMsg := m.Message{
					ID:        string(ce.NodeID),
					Text:      ce.Addr.String(),
					Proposer:  string(manager.ManagerNodeID),
					Timestamp: time.Now(),
				}
				var ack bool
				if err := mrpc.DeletePeer(&deleteMsg, &ack); err != nil {
					return err
				}
				fmt.Println("Failed: ", ack)
				fmt.Println("Addr Failed: ", ce.NodeID, ce.Addr)
			}
			return err
		default:
			fmt.Println("default Error")
			return err
		}
	}
	manager.ManagerMutex.Lock()
	for k, v := range manager.ManagerPeers {
		(*peerList)[string(k)] = v.String()
	}
	manager.ManagerMutex.Unlock()
	(*peerList)[string(manager.ManagerNodeID)] = manager.ManagerIP.String()
	return nil
}

func (mrpc *ManagerRPCServer) DeletePeer(msg *m.Message, ack *bool) error {
	*ack = false

	delPeerID := ManagerNodeID(msg.ID)

	var newPeerAddr map[ManagerNodeID]net.Addr

	manager.ManagerMutex.Lock()
	for k, v := range manager.ManagerPeers {
		if k == delPeerID {
			continue
		}
		newPeerAddr[k] = v
	}
	manager.ManagerMutex.Unlock()

	if err := mrpc.threePC("DeletePeer", msg, newPeerAddr); err != nil {
		return err
	}
	*ack = true
	return nil
}

func (mrpc *ManagerRPCServer) threePC(serviceMethod string, msg *m.Message, peerAddrs map[ManagerNodeID]net.Addr) error {
	// canCommitPhase
	peerTransactionState, err := mrpc.canCommit(serviceMethod, msg, peerAddrs)
	if err != nil {
		return err
	}
	// recovery if needed
	if peerTransactionState != nil {
		var recoverPeerAddr map[ManagerNodeID]net.Addr

		for k, v := range *peerTransactionState {
			if v == COMMIT {
				recoverPeerAddr[k] = manager.ManagerPeers[k]
			}
		}

		if err := mrpc.recoverPhase(serviceMethod, msg, recoverPeerAddr); err != nil {
			return err
		}
		return nil
	}
	// preCommitPhase
	if err := mrpc.preCommit(msg, peerAddrs); err != nil {
		return err
	}
	// commitPhase
	if err := mrpc.commit(serviceMethod, msg, peerAddrs); err != nil {
		return err
	}
	return nil
}

func (mrpc *ManagerRPCServer) recoverPhase(serviceMethod string, msg *m.Message, peerAddrs map[ManagerNodeID]net.Addr) (err error) {
	fmt.Println("Begin Recover")
	errorCh := make(chan error, 1)
	wg := sync.WaitGroup{}
	manager.ManagerMutex.Lock()
	for k, v := range peerAddrs {
		if k == manager.ManagerNodeID {
			continue
		}
		wg.Add(1)
		go func(managerID ManagerNodeID, managerPeerAddr net.Addr) {
			defer func() {
				if p := recover(); p != nil {
					err = NewConnectionErr(managerID, managerPeerAddr, fmt.Errorf("%v", p))
				}
			}()
			defer wg.Done()
			rpcClient, err := vrpc.RPCDial("tcp", managerPeerAddr.String(), logger, loggerOptions)
			defer rpcClient.Close()
			if err != nil {
				errorCh <- NewConnectionErr(managerID, managerPeerAddr, err)
				return
			}
			var ack bool
			if err := RpcCallTimeOut(rpcClient, fmt.Sprintf("ManagerRPCServer.Commit%vRPC", serviceMethod), msg, &ack); err != nil {
				errorCh <- NewConnectionErr(managerID, managerPeerAddr, err)
				return
			}
		}(k, v)
	}
	manager.ManagerMutex.Unlock()

	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case err := <-errorCh:
		manager.TransactionCache.Add(msg.Hash(), ABORT)
		return fmt.Errorf("recover aborted: %v", err)
	case <-c:
		fmt.Println("Commit Phase Done")
	}

	if _, exists := peerAddrs[manager.ManagerNodeID]; exists {
		// Local Commit
		var ack bool

		method := reflect.ValueOf(mrpc).MethodByName(fmt.Sprintf("Commit%vRPC", serviceMethod))
		if err := method.Call([]reflect.Value{reflect.ValueOf(msg), reflect.ValueOf(&ack)})[0].Interface(); err != nil {
			manager.TransactionCache.Add(msg.Hash(), ABORT)
			return fmt.Errorf("coordinator failed: transaction aborted: %v", err)
		}
	}
	fmt.Println("Done Recover")
	return nil
}

func (mrpc *ManagerRPCServer) canCommit(serviceMethod string, msg *m.Message, peerAddrs map[ManagerNodeID]net.Addr) (*map[ManagerNodeID]State, error) {
	// canCommitPhase
	fmt.Println("CanCommitPhase")
	var wg sync.WaitGroup
	errorCh := make(chan error, 1)
	manager.ManagerMutex.Lock()
	fmt.Println("Break1")
	peerTransactionState := make(map[ManagerNodeID]State)

	for managerID, managerPeer := range peerAddrs {
		fmt.Println("Peer addr: ", managerPeer.String())
		wg.Add(1)
		go func(managerID ManagerNodeID, managerPeerAddr net.Addr) {
			// Prevent Closure
			defer func() {
				if p := recover(); p != nil {
					errorCh <- NewConnectionErr(managerID, managerPeerAddr, fmt.Errorf("%v", p))
				}
			}()
			defer wg.Done()
			rpcClient, err := vrpc.RPCDial("tcp", managerPeerAddr.String(), logger, loggerOptions)
			defer rpcClient.Close()
			if err != nil {
				errorCh <- NewConnectionErr(managerID, managerPeerAddr, err)
				return
			}
			var s State
			if err := RpcCallTimeOut(rpcClient, fmt.Sprintf("ManagerRPCServer.CanCommitRPC"), msg, &s); err != nil {
				errorCh <- NewConnectionErr(managerID, managerPeerAddr, err)
				return
			}
			peerTransactionState[managerID] = s
		}(managerID, managerPeer)
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
		return nil, NewTransactionErr(err)
	case <-c:
		fmt.Println("CanCommitPhase Done")
	}

	fmt.Println("Break4")
	manager.ManagerMutex.Unlock()

	// Local canCommit
	var s State
	method := reflect.ValueOf(mrpc).MethodByName(fmt.Sprintf("CanCommitRPC"))
	if err := method.Call([]reflect.Value{reflect.ValueOf(msg), reflect.ValueOf(&s)})[0].Interface(); err != nil {
		manager.TransactionCache.Add(msg.Hash(), ABORT)
		return nil, fmt.Errorf("coordinator failed: transaction aborted: %v", err)
	}
	peerTransactionState[manager.ManagerNodeID] = s
	for _, v := range peerTransactionState {
		if v == COMMIT {
			return &peerTransactionState, nil
		}
	}
	fmt.Println("Peer Transaction Map", peerTransactionState)
	return nil, nil
}

func (mrpc *ManagerRPCServer) preCommit(msg *m.Message, peerAddrs map[ManagerNodeID]net.Addr) (err error) {
	// preCommitPhase
	fmt.Println("PreCommit Phase")
	errorCh := make(chan error, 1)
	wg := sync.WaitGroup{}
	for managerID, managerPeer := range peerAddrs {
		wg.Add(1)
		go func(managerID ManagerNodeID, managerPeerAddr net.Addr) {
			defer func() {
				if p := recover(); p != nil {
					err = NewConnectionErr(managerID, managerPeerAddr, fmt.Errorf("%v", p))
				}
			}()
			defer wg.Done()
			rpcClient, err := vrpc.RPCDial("tcp", managerPeerAddr.String(), logger, loggerOptions)
			defer rpcClient.Close()
			if err != nil {
				errorCh <- NewConnectionErr(managerID, managerPeerAddr, err)
				return
			}
			var ack bool
			if err := RpcCallTimeOut(rpcClient, fmt.Sprintf("ManagerRPCServer.PreCommitRPC"), msg, &ack); err != nil {
				errorCh <- NewConnectionErr(managerID, managerPeerAddr, err)
				return
			}
			if !ack {
				errorCh <- fmt.Errorf("peer disagrees")
			}
		}(managerID, managerPeer)
	}
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case err := <-errorCh:
		manager.TransactionCache.Add(msg.Hash(), ABORT)
		return NewTransactionErr(err)
	case <-c:
		fmt.Println("PreCommit Phase Done")
	}
	return nil
}

func (mrpc *ManagerRPCServer) commit(serviceMethod string, msg *m.Message, peerAddrs map[ManagerNodeID]net.Addr) (err error) {
	errorCh := make(chan error, 1)
	wg := sync.WaitGroup{}
	for managerID, managerPeer := range peerAddrs {
		wg.Add(1)
		go func(managerID ManagerNodeID, managerPeerAddr net.Addr) {
			defer func() {
				if p := recover(); p != nil {
					err = NewConnectionErr(managerID, managerPeerAddr, fmt.Errorf("%v", p))
				}
			}()
			defer wg.Done()
			rpcClient, err := vrpc.RPCDial("tcp", managerPeerAddr.String(), logger, loggerOptions)
			defer rpcClient.Close()
			if err != nil {
				errorCh <- NewConnectionErr(managerID, managerPeerAddr, err)
				return
			}
			var ack bool
			if err := RpcCallTimeOut(rpcClient, fmt.Sprintf("ManagerRPCServer.Commit%vRPC", serviceMethod), msg, &ack); err != nil {
				errorCh <- NewConnectionErr(managerID, managerPeerAddr, err)
				return
			}

			if !ack {
				errorCh <- fmt.Errorf("peer disagrees")
			}
		}(managerID, managerPeer)

	}

	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()

	select {
	case err := <-errorCh:
		manager.TransactionCache.Add(msg.Hash(), ABORT)
		return NewTransactionErr(err)
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
		return fmt.Errorf("transaction aborted: transaction not commited")
	}

	return nil
}

//-----------------------------------------------------------------------------------------------------------------------------

func (mrpc *ManagerRPCServer) CanCommitRPC(msg *m.Message, state *State) error {
	fmt.Println("CanCommitRPC")
	v, exist := manager.TransactionCache.Get(msg.Hash())
	if exist {
		s, ok := v.(State)
		if !ok {
			return fmt.Errorf("Couldn't typecast interface value: %v to State", s)
		}

		if s == COMMIT {
			fmt.Println("transaction has been committed, return existing state: ", s)
			*state = s
			return nil
		}
	}

	// return err and set Transaction Cache as Abort on unwanted cases

	// peerManagerID := ManagerNodeID(msg.ID)
	// manager.ManagerMutex.Lock()
	// defer manager.ManagerMutex.Unlock()
	// if _, exist := manager.ManagerPeers[peerManagerID]; exist {
	// *state = ABORT
	// 	manager.TransactionCache.Add(msg.Hash(), ABORT)
	// 	return nil
	// }

	manager.TransactionCache.Add(msg.Hash(), PREPARE)
	println("CanCommitRPC prepare")
	*state = PREPARE
	return nil
}

func (mrpc *ManagerRPCServer) PreCommitRPC(msg *m.Message, ack *bool) error {
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

func (mrpc *ManagerRPCServer) CommitDeletePeerRPC(msg *m.Message, ack *bool) error {
	*ack = false
	peerManagerID := ManagerNodeID(msg.ID)
	peerManagerAddr, err := net.ResolveTCPAddr("tcp", msg.Text)

	fmt.Println(peerManagerAddr, peerManagerID)

	if err != nil {
		return err
	}

	manager.ManagerMutex.Lock()
	delete(manager.ManagerPeers, peerManagerID)
	manager.ManagerMutex.Unlock()

	// ring = ring.AddNode(peerManagerAddr.String())

	fmt.Printf("Delete Peer - %v - %v\n", peerManagerID, peerManagerAddr)
	fmt.Println("Peer Map: ", manager.ManagerPeers)

	manager.TransactionCache.Add(msg.Hash(), COMMIT)
	*ack = true
	return nil
}

//----------------------------------------------------------------------------------------------------------------------

func (mrpc *ManagerRPCServer) CommitRegistBrokerRPC(msg *m.Message, ack *bool) error {
	*ack = false
	BrokerNodeID := BrokerID(msg.ID)
	BrokerAddr, err := net.ResolveTCPAddr("tcp", msg.Text)

	fmt.Println(BrokerAddr, BrokerNodeID)

	if err != nil {
		return err
	}

	manager.BrokerMutex.Lock()
	manager.BrokerNodes[BrokerNodeID] = BrokerAddr
	manager.BrokerMutex.Unlock()

	fmt.Printf("added peer - %v - %v\n", BrokerNodeID, BrokerAddr)
	fmt.Println("Broker Map: ", manager.BrokerNodes)

	manager.TransactionCache.Add(msg.Hash(), COMMIT)
	*ack = true
	return nil
}

//----------------------------------------------------------------------------------------------------------------------

func (mrpc *ManagerRPCServer) CommitNewTopicRPC(msg *m.Message, ack *bool) error {
	fmt.Println("CommitNewTopicRPC")
	*ack = false
	ProviderID := msg.ID
	ProviderAddr, err := net.ResolveTCPAddr("tcp", msg.Text)

	println("------------------------------")
	fmt.Printf("%+v\n", msg)

	if err != nil {
		return err
	}
	var topicInfo Topic
	if len(msg.IPs) > 1 {
		topicInfo = Topic{LeaderIP: msg.IPs[0], FollowerIPs: msg.IPs[1:], PartitionNum: msg.Partition}
	} else {
		topicInfo = Topic{LeaderIP: msg.IPs[0], PartitionNum: msg.Partition}
	}
	println("------------------------------")
	manager.TopicMutex.Lock()
	manager.TopicMap[TopicID(msg.Topic)] = topicInfo
	manager.TopicMutex.Unlock()

	fmt.Printf("added topic - from provider %v - %v\n", ProviderID, ProviderAddr)
	printTopicMap()

	manager.TransactionCache.Add(msg.Hash(), COMMIT)
	*ack = true
	return nil
}

/*----------------------------------------------------------------------------------------------------------------*/

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

func (mrpc *ManagerRPCServer) RegisterBroker(msg *m.Message, ack *bool) (err error) {
	*ack = false

	fmt.Println(msg.Text)
	fmt.Println(msg.ID)

	rAddr, err := net.ResolveTCPAddr("tcp", msg.Text)
	defer func() {
		if p := recover(); p != nil {
			err = NewConnectionErr(ManagerNodeID("unkown"), rAddr, fmt.Errorf("%v", p))
		}
	}()
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

	fmt.Println("Calling three pc to add broker")
	fmt.Println("Current Peers: ", manager.ManagerPeers)
	if err := mrpc.threePC("RegistBroker", msg, manager.ManagerPeers); err != nil {
		return err
	}

	return nil
}

func (mrpc *ManagerRPCServer) CreateNewTopic(request *m.Message, response *m.Message) error {
	println(request.Topic)

	if int(request.Partition) > len(manager.BrokerNodes) {
		response.Text = "At most " + strconv.Itoa(len(manager.BrokerNodes)) + " partitions"
		return ErrInsufficientFreeNodes
		// response.Ack = false
	} else if _, v := manager.TopicMap[TopicID(request.Topic)]; v {
		response.Text = "The topic " + request.Topic + "has been created"
		return errors.New("More than one topic")
	} else {

		// response.Ack = true
		IPs := getHashingNodes(request.Topic, int(request.Partition))
		msg := &m.Message{ID: request.ID, IPs: IPs, Topic: request.Topic, Partition: request.Partition}
		mrpc.threePC("NewTopic", msg, manager.ManagerPeers)

		response.IPs = IPs
		response.ID = config.ManagerNodeID
		response.Role = m.MANAGER
		response.Timestamp = time.Now()
		response.Type = m.MANAGER_RESPONSE_TO_PROVIDER
	}

	// printTopicMap()

	return nil
}

func getHashingNodes(key string, replicaCount int) []string {
	var list []string

	manager.BrokerMutex.Lock()
	defer manager.BrokerMutex.Unlock()

	for _, v := range manager.BrokerNodes {
		list = append(list, v.String())
	}

	ring := hashring.New(list)
	server, _ := ring.GetNodes(key, replicaCount)

	return server
}

func printTopicMap() {
	println("------------")
	for key, v := range manager.TopicMap {
		println("topic:", key)
		println("LeaderIP:", v.LeaderIP)
		fmt.Printf("FollowerIP: %v\n", v.FollowerIPs)
		println("PartitionNum:", v.PartitionNum)
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

		} else if cmd == "peer\n" {
			fmt.Println(manager.ManagerPeers)
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
	defer rpcClient.Close()

	select {
	case doneCall := <-rpcCall.Done:
		if doneCall.Error != nil {
			return doneCall.Error
		}
	case <-time.After(time.Duration(3) * time.Second):
		return NewRPCTimedout(rpcCall.ServiceMethod)
	}
	return nil
}

func NewConnectionErr(nodeID ManagerNodeID, addr net.Addr, err error) *ConnectionErr {
	return &ConnectionErr{
		Addr:   addr,
		NodeID: nodeID,
		Err:    err,
	}
}

func NewTransactionErr(err error) *TransactionErr {
	return &TransactionErr{
		Err: err,
	}
}

func (e *TransactionErr) Error() string {
	return fmt.Sprintf("transaction aborted: %v", e.Err)
}

func (e *ConnectionErr) Error() string {
	return fmt.Sprintf("bad connection - %v - %v: %v", e.NodeID, e.Addr.String(), e.Err)
}

func NewRPCTimedout(serviceMethod string) *RPCTimedout {
	return &RPCTimedout{
		ServiceMethod: serviceMethod,
	}
}

func (e *RPCTimedout) Error() string {
	return fmt.Sprintf("rpc call: %v has timed out", e.ServiceMethod)
}
