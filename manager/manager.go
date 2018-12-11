package main

import (
	"bufio"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/rpc"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"
	"strings"

	m "../lib/message"
	lru "github.com/hashicorp/golang-lru"
	"github.com/serialx/hashring"

	"github.com/DistributedClocks/GoVector/govec"
	"github.com/DistributedClocks/GoVector/govec/vrpc"
)

// Error types

// ErrInsufficientFreeNodes denotes that someone requested more free nodes
// than are currently available
var ErrInsufficientFreeNodes = errors.New("insufficient free nodes")

type ConnectionErr struct {
	Addr   string
	NodeID ManagerNodeID
	Err    error
}

type TransactionErr struct {
	Err error
	Msg string
}

type RPCTimedout struct {
	ServiceMethod string
}

type AgreementErr struct {
	msg string
}

type RecoveryErr struct {
	Err error
}

type TimeoutErr struct {
	Addr   string
	NodeID ManagerNodeID
	Err    error
}

//Error during Abort
type AbortErr struct {
	Err error
}

type ManagerRPCServer int

type ManagerNode struct {
	ManagerNodeID    ManagerNodeID
	ManagerPeers     map[ManagerNodeID]string
	ManagerIP        string
	TopicMap         map[string]Topic
	BrokerNodes      map[BrokerNodeID]string
	TopicMutex       *sync.Mutex
	ManagerMutex     *sync.Mutex
	BrokerMutex      *sync.Mutex
	MU               *sync.Mutex
	TransactionCache *lru.Cache // A Transaction to State Mapping, key is sha1 hash of the Message object, and value would be the state
}

type State uint

const (
	READY State = iota
	WAIT
	APPROVE
	PREPARE
	COMMIT
	ABORT
)

const cacheSize = 10

type BrokerNodeID string

type ManagerNodeID string

type Partition struct {
	TopicName    string
	PartitionIdx uint8
	LeaderNodeID BrokerNodeID
	FollowerIPs  map[BrokerNodeID]string
}

type Topic struct {
	TopicName  string
	Partitions []*Partition
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

//-------------------------------------------------------------------------------------------------------------------------------

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

	manager = &ManagerNode{
		ManagerNodeID: ManagerNodeID(config.ManagerNodeID),
		TopicMap:      make(map[string]Topic),
		ManagerPeers:  make(map[ManagerNodeID]string),
		BrokerNodes:   make(map[BrokerNodeID]string),
		TopicMutex:    &sync.Mutex{},
		ManagerMutex:  &sync.Mutex{},
		BrokerMutex:   &sync.Mutex{},
		MU:            &sync.Mutex{},
	}

	cache, err := lru.New(cacheSize)

	if err != nil {
		return err
	}

	manager.TransactionCache = cache

	logger = govec.InitGoVector(string(manager.ManagerNodeID), fmt.Sprintf("%v-logfile", manager.ManagerNodeID), govec.GetDefaultConfig())
	loggerOptions = govec.GetDefaultLogOptions()

	go func() {
		time.Sleep(time.Duration(100) * time.Millisecond)
		if len(config.PeerManagerNodeIP) != 0 {
			if err := manager.registerPeerRequest(config.PeerManagerNodeIP); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		}
	}()

	var emptylist []string
	ring = hashring.New(emptylist)
	spawnRPCServer()
	return nil
}

func (mn *ManagerNode) registerPeerRequest(managerPeerAddr string) (err error) {
	fmt.Println("registerPeerRequest")

	defer func() {
		if p := recover(); p != nil {
			err = NewConnectionErr(ManagerNodeID("PrimaryManger"), managerPeerAddr, fmt.Errorf("%v", p))
		}
	}()

	rpcClient, err := vrpc.RPCDial("tcp", managerPeerAddr, logger, loggerOptions)
	defer rpcClient.Close()
	if err != nil {
		return err
	}

	newMsg := m.Message{
		ID:        string(mn.ManagerNodeID),
		Text:      mn.ManagerIP,
		Proposer:  string(mn.ManagerNodeID),
		Timestamp: time.Now(),
	}

	var response m.Message

	if err := rpcClient.Call("ManagerRPCServer.RegisterPeer", newMsg, &response); err != nil {
		return err
	}

	var tmpManager ManagerNode

	if err := json.Unmarshal(response.Payload, &tmpManager); err != nil {
		fmt.Println(err)
	}

	// Get Manager Peer IP
	manager.ManagerMutex.Lock()
	for k, v := range tmpManager.ManagerPeers {
		// Skip registering own IP to the Peer Map
		if ManagerNodeID(k) == manager.ManagerNodeID {
			continue
		}
		manager.ManagerPeers[ManagerNodeID(k)] = v
	}
	manager.ManagerPeers[ManagerNodeID(tmpManager.ManagerNodeID)] = tmpManager.ManagerIP
	manager.ManagerMutex.Unlock()

	// Sync Brokers
	manager.BrokerMutex.Lock()
	manager.BrokerNodes = tmpManager.BrokerNodes
	manager.BrokerMutex.Unlock()

	// Sync Topics
	manager.TopicMutex.Lock()
	manager.TopicMap = tmpManager.TopicMap
	manager.TopicMutex.Unlock()

	return nil
}

// -------------------------------------------------------------------------------------------------------------------------------------------------------

func (mrpc *ManagerRPCServer) RegisterPeer(msg *m.Message, response *m.Message) error {

	if err := mrpc.threePC("RegisterPeer", msg, manager.ManagerPeers); err != nil {
		// Handle Error and make decision for next procedure
		switch err.(type) {
		case *TimeoutErr:

		case *ConnectionErr:
			connErr := err.(*ConnectionErr)
			deleteMsg := m.Message{
				ID:        string(connErr.NodeID),
				Text:      connErr.Addr,
				Proposer:  string(manager.ManagerNodeID),
				Timestamp: time.Now(),
			}
			var ack bool
			if err := mrpc.DeletePeer(&deleteMsg, &ack); err != nil {
				return fmt.Errorf("delete node failed: %v", err)
			}

			if ack {
				fmt.Println(manager.ManagerPeers)

				if err := mrpc.threePC("RegisterPeer", msg, manager.ManagerPeers); err != nil {
					return fmt.Errorf("retry failed: %v", err)
				}
			}
		default:
			fmt.Println("What the h* just happened?")
			return err
		}
	}

	manager.ManagerMutex.Lock()
	manager.BrokerMutex.Lock()
	manager.TopicMutex.Lock()
	data, err := json.Marshal(manager)
	if err != nil {
		return err
	}
	manager.TopicMutex.Unlock()
	manager.BrokerMutex.Unlock()
	manager.ManagerMutex.Unlock()

	response.ID = string(manager.ManagerNodeID)
	response.Timestamp = time.Now()
	response.Proposer = string(manager.ManagerNodeID)
	response.Payload = data

	return nil
}

func (mrpc *ManagerRPCServer) DeletePeer(msg *m.Message, ack *bool) error {
	*ack = false
	delPeerID := ManagerNodeID(msg.ID)


	var newPeerAddr = map[ManagerNodeID]string{}



	manager.ManagerMutex.Lock()
	for k, v := range manager.ManagerPeers {
		if k == delPeerID {
			continue
		}
		newPeerAddr[k] = v
	}
	manager.ManagerMutex.Unlock()

	
	for _, addr := range manager.BrokerNodes {
		rpcClient, err := vrpc.RPCDial("tcp", addr, logger, loggerOptions)
		defer rpcClient.Close()
		if err != nil {
			return err
		}

		var response m.Message

		if err := rpcClient.Call("BrokerRPCServer.DeleteManager", msg, &response); err != nil {
			return err
		}
	}



	if err := mrpc.threePC("DeletePeer", msg, newPeerAddr); err != nil {
		switch err.(type) {
		case *TimeoutErr:
			// connErr := err.(*ConnectionErr)

		case *ConnectionErr:
			connErr := err.(*ConnectionErr)
			deleteMsg := &m.Message{
				ID:        string(connErr.NodeID),
				Text:      connErr.Addr,
				Proposer:  string(manager.ManagerNodeID),
				Timestamp: time.Now(),
			}
			var ack bool

			if err := mrpc.DeletePeer(deleteMsg, &ack); err != nil {
				return err
			}

		default:
			fmt.Println(err)
			return err
		}
	}

	*ack = true
	return nil
}

func (mrpc *ManagerRPCServer) AddBroker(msg *m.Message, response *m.Message) error {

	if err := mrpc.threePC("AddBroker", msg, manager.ManagerPeers); err != nil {
		return err
	}

	response.IPs = map[string]string{}

	manager.BrokerMutex.Lock()
	for k, v := range manager.BrokerNodes {
		response.IPs[string(k)] = v
	}
	manager.BrokerMutex.Unlock()

	var ipList = []string{manager.ManagerIP}
	manager.ManagerMutex.Lock()
	for _, v := range manager.ManagerPeers {
		ipList = append(ipList, v)
	}
	manager.ManagerMutex.Unlock()
	
	response.Text = strings.Join(ipList, ",")

	return nil
}

func (mrpc *ManagerRPCServer) CreateNewTopic(request *m.Message, response *m.Message) error {
	fmt.Println("RecivedCreatedTopic")

	if int(request.ReplicaNum) > len(manager.BrokerNodes) {
		response.Text = "At most " + strconv.Itoa(len(manager.BrokerNodes)) + " partitions"
		return ErrInsufficientFreeNodes
		// response.Ack = false
	} else if _, v := manager.TopicMap[request.Topic]; v {
		response.Text = "The topic " + request.Topic + "has been created"
		return errors.New("More than one topic")
	} else {
		// response.Ack = true
		topic := &Topic{
			TopicName:  request.Topic,
			Partitions: []*Partition{},
		}

		errorCh := make(chan error, 1)
		wg := sync.WaitGroup{}

		var i uint8

		for i = 0; i < request.Partitions; i++ {
			wg.Add(1)
			go func(j uint8) {
				k := j
				fmt.Println(k)
				partition := &Partition{
					TopicName:    request.Topic,
					PartitionIdx: uint8(j),
				}

				nodeIDs := getHashingNodes(partition.HashString(), int(request.ReplicaNum))

				if len(nodeIDs) == 0 {
					errorCh <- fmt.Errorf("Cannot assign any broker nodes")
				}
				var followerAddrMap = map[string]string{}

				for _, v := range nodeIDs[1:] {
					followerAddrMap[v] = manager.BrokerNodes[BrokerNodeID(v)]
				}
				request.PartitionIdx = j
				request.IPs = followerAddrMap
				leaderNodeID := BrokerNodeID(nodeIDs[0])
				leaderAddr := manager.BrokerNodes[leaderNodeID]

				println("-------------")
				println("topic:", request.Topic)
				println("LeaderIP:", nodeIDs[0])
				fmt.Printf("FollowerIP: %v\n", nodeIDs[1:])
				println("PartitionNum:", request.Partitions)
				println("-------------")

				defer func() {
					if p := recover(); p != nil {
						errorCh <- NewConnectionErr(ManagerNodeID(leaderNodeID), leaderAddr, fmt.Errorf("%v", p))
					}
				}()
				defer wg.Done()

				rpcClient, err := vrpc.RPCDial("tcp", leaderAddr, logger, loggerOptions)
				if err != nil {
					errorCh <- NewConnectionErr(ManagerNodeID(leaderNodeID), leaderAddr, err)
				}
				var ack bool
				if err := RpcCallTimeOut(rpcClient, "BrokerRPCServer.CreateNewPartition", request, &ack); err != nil {
					errorCh <- NewTimeoutErr(ManagerNodeID(leaderNodeID), leaderAddr, err)
				}

				partition.LeaderNodeID = leaderNodeID
				partition.FollowerIPs = map[BrokerNodeID]string{}

				for k, v := range followerAddrMap {
					partition.FollowerIPs[BrokerNodeID(k)] = v
				}

				topic.Partitions = append(topic.Partitions, partition)

				manager.TopicMap[request.Topic] = *topic

			}(i)
		}

		c := make(chan struct{})
		go func() {
			defer close(c)
			wg.Wait()
		}()

		select {
		case err := <-errorCh:
			//TODOs:Handle Broker Node connection failure
			fmt.Println(err)
			switch err.(type) {
			case *TimeoutErr:
			case *ConnectionErr:
			default:
			}
			return err
		case <-c:
			fmt.Println("Done")
		}

		data, err := json.Marshal(topic)

		if err != nil {
			return err
		}

		payloadMsg := &m.Message{
			Payload:   data,
			Proposer:  string(manager.ManagerNodeID),
			Timestamp: time.Now(),
		}

		if err := mrpc.threePC("AddTopic", payloadMsg, manager.ManagerPeers); err != nil {
			fmt.Println(err)
			// TODOs: Handle Error
			return err
		}
	}

	// printTopicMap()

	return nil
}

// --------------------------------------------------------------------------------------------------------------------------------------------------------------

func (mrpc *ManagerRPCServer) threePC(serviceMethod string, msg *m.Message, peerAddrs map[ManagerNodeID]string) error {
	// canCommitPhase
	peerTransactionState, err := mrpc.canCommit(serviceMethod, msg, peerAddrs)

	if err != nil {
				switch err.(type) {
		case *TimeoutErr:
			// connErr := err.(*ConnectionErr)

		case *ConnectionErr:
			connErr := err.(*ConnectionErr)
			deleteMsg := &m.Message{
				ID:        string(connErr.NodeID),
				Text:      connErr.Addr,
				Proposer:  string(manager.ManagerNodeID),
				Timestamp: time.Now(),
			}
			var ack bool

			if err := mrpc.DeletePeer(deleteMsg, &ack); err != nil {
				return err
			}

		default:
			return err
		}
	}
	// recovery if needed
	if peerTransactionState != nil {
		var recoverPeerAddr = map[ManagerNodeID]string{}
		for k, v := range peerTransactionState {
			if v != COMMIT {
				recoverPeerAddr[k] = manager.ManagerPeers[k]
			}
		}

		if err := mrpc.recoverPhase(serviceMethod, msg, recoverPeerAddr); err != nil {
			return err
		}
		return nil
	}
	// preCommitPhase
	if err := mrpc.preCommit(serviceMethod, msg, peerAddrs); err != nil {
		return err
	}

	// commitPhase
	if err := mrpc.commit(serviceMethod, msg, peerAddrs); err != nil {
		return err
	}
	return nil
}

func (mrpc *ManagerRPCServer) recoverPhase(serviceMethod string, msg *m.Message, peerAddrs map[ManagerNodeID]string) (err error) {
	fmt.Println("Begin Recover")
	errorCh := make(chan error, 1)
	wg := sync.WaitGroup{}
	manager.ManagerMutex.Lock()
	for k, v := range peerAddrs {
		if k == manager.ManagerNodeID {
			continue
		}
		wg.Add(1)
		go func(managerID ManagerNodeID, managerPeerAddr string) {
			defer func() {
				if p := recover(); p != nil {
					err = NewConnectionErr(managerID, managerPeerAddr, fmt.Errorf("%v", p))
				}
			}()
			defer wg.Done()
			rpcClient, err := vrpc.RPCDial("tcp", managerPeerAddr, logger, loggerOptions)
			defer rpcClient.Close()
			if err != nil {
				errorCh <- NewConnectionErr(managerID, managerPeerAddr, err)
				return
			}
			var ack bool
			if err := RpcCallTimeOut(rpcClient, fmt.Sprintf("ManagerRPCServer.Commit%vRPC", serviceMethod), msg, &ack); err != nil {
				switch err.(type) {
				case *RPCTimedout:
					errorCh <- NewTimeoutErr(managerID, managerPeerAddr, err)
				default:
					errorCh <- NewConnectionErr(managerID, managerPeerAddr, err)
				}
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
		return NewRecoveryErr(err)
	case <-c:
		fmt.Println("Commit Phase Done")
	}

	if _, exists := peerAddrs[manager.ManagerNodeID]; exists {
		// Local Commit
		var ack bool

		method := reflect.ValueOf(mrpc).MethodByName(fmt.Sprintf("Commit%vRPC", serviceMethod))
		if err := method.Call([]reflect.Value{reflect.ValueOf(msg), reflect.ValueOf(&ack)})[0].Interface(); err != nil {
			manager.TransactionCache.Add(msg.Hash(), ABORT)
			newErr := err.(error)
			return NewTransactionErr(newErr, "local canCommit")
		}
	}
	fmt.Println("Done Recover")
	return nil
}

func (mrpc *ManagerRPCServer) canCommit(serviceMethod string, msg *m.Message, peerAddrs map[ManagerNodeID]string) (map[ManagerNodeID]State, error) {
	// canCommitPhase
	fmt.Println("CanCommitPhase")
	v, exist := manager.TransactionCache.Get(msg.Hash())
	var s State
	if exist {
		s, ok := v.(State)
		if !ok {
			return nil, fmt.Errorf("Couldn't typecast interface value: %v to State", s)
		}
	} else {
		s = READY
	}
	peerTransactionState := make(map[ManagerNodeID]State)
	peerTransactionState[manager.ManagerNodeID] = s
	var wg sync.WaitGroup
	errorCh := make(chan error, 1)
	manager.ManagerMutex.Lock()

	fmt.Println("Break 1")
	
	fmt.Println("Break 2")

	for managerID, managerPeer := range peerAddrs {
		wg.Add(1)
		go func(managerID ManagerNodeID, managerPeerAddr string) {
			// Prevent Closure
			defer func() {
				if p := recover(); p != nil {
					errorCh <- NewConnectionErr(managerID, managerPeerAddr, fmt.Errorf("%v", p))
				}
			}()
			defer wg.Done()
			rpcClient, err := vrpc.RPCDial("tcp", managerPeerAddr, logger, loggerOptions)
			defer rpcClient.Close()
			if err != nil {
				errorCh <- NewConnectionErr(managerID, managerPeerAddr, err)
				return
			}
			var s State
			if err := RpcCallTimeOut(rpcClient, fmt.Sprintf("ManagerRPCServer.CanCommitRPC"), msg, &s); err != nil {
				switch err.(type) {
				case *RPCTimedout:
					errorCh <- NewTimeoutErr(managerID, managerPeerAddr, err)
				default:
					errorCh <- NewConnectionErr(managerID, managerPeerAddr, err)
				}
				return
			}
			peerTransactionState[managerID] = s
		}(managerID, managerPeer)
	}

	manager.TransactionCache.Add(msg.Hash(), WAIT)

	fmt.Println("Break 3")

	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()

	select {
	case err := <-errorCh:
		manager.TransactionCache.Add(msg.Hash(), ABORT)
		manager.ManagerMutex.Unlock()
		fmt.Println("Abort Transaction")
		mrpc.abort(msg, peerAddrs)
		return nil, err
	case <-c:
		fmt.Println("CanCommitPhase Done")
	}

	manager.ManagerMutex.Unlock()

	fmt.Println("Break 4")

	// Local canCommit
	method := reflect.ValueOf(mrpc).MethodByName(fmt.Sprintf("CanCommitRPC"))
	if err := method.Call([]reflect.Value{reflect.ValueOf(msg), reflect.ValueOf(&s)})[0].Interface(); err != nil {
		manager.TransactionCache.Add(msg.Hash(), ABORT)
		mrpc.abort(msg, peerAddrs)
		errTmp := err.(error)
		return nil, errTmp

	}

	for _, v := range peerTransactionState {
		if v == COMMIT || v == PREPARE {
			return peerTransactionState, nil
		}
	}

	return nil, nil
}

func (mrpc *ManagerRPCServer) preCommit(serviceMethod string, msg *m.Message, peerAddrs map[ManagerNodeID]string) (err error) {
	// preCommitPhase
	fmt.Println("PreCommit Phase")
	errorCh := make(chan error, 1)
	wg := sync.WaitGroup{}
	for managerID, managerPeer := range peerAddrs {
		wg.Add(1)
		go func(managerID ManagerNodeID, managerPeerAddr string) {
			defer func() {
				if p := recover(); p != nil {
					err = NewConnectionErr(managerID, managerPeerAddr, fmt.Errorf("%v", p))
				}
			}()
			defer wg.Done()
			rpcClient, err := vrpc.RPCDial("tcp", managerPeerAddr, logger, loggerOptions)
			defer rpcClient.Close()
			if err != nil {
				errorCh <- NewConnectionErr(managerID, managerPeerAddr, err)
				return
			}
			var ack bool
			if err := RpcCallTimeOut(rpcClient, fmt.Sprintf("ManagerRPCServer.PreCommitRPC"), msg, &ack); err != nil {
				switch err.(type) {
				case *RPCTimedout:
					errorCh <- NewTimeoutErr(managerID, managerPeer, err)
				default:
					errorCh <- NewConnectionErr(managerID, managerPeer, err)
				}
				return
			}
			if !ack {
				errorCh <- fmt.Errorf("peer disagrees")
			}
		}(managerID, managerPeer)
	}

	manager.TransactionCache.Add(msg.Hash(), PREPARE)

	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case err := <-errorCh:
		fmt.Println(err)
		switch err.(type) {
		case *TimeoutErr:
			manager.TransactionCache.Add(msg.Hash(), ABORT)
			mrpc.abort(msg, peerAddrs)
		default:
			var ack bool
			method := reflect.ValueOf(mrpc).MethodByName(fmt.Sprintf("Commit%vRPC", serviceMethod))
			if err := method.Call([]reflect.Value{reflect.ValueOf(msg), reflect.ValueOf(&ack)})[0].Interface(); err != nil {
				mrpc.abort(msg, peerAddrs)
				manager.TransactionCache.Add(msg.Hash(), ABORT)
				return fmt.Errorf("coordinator failed: transaction aborted: %v", err)
			}
			manager.TransactionCache.Add(msg.Hash(), COMMIT)
		}
		return err
	case <-c:
		fmt.Println("PreCommit Phase Done")
	}
	return nil
}

func (mrpc *ManagerRPCServer) commit(serviceMethod string, msg *m.Message, peerAddrs map[ManagerNodeID]string) (err error) {
	errorCh := make(chan error, 1)
	wg := sync.WaitGroup{}
	for managerID, managerPeer := range peerAddrs {
		wg.Add(1)
		go func(managerID ManagerNodeID, managerPeerAddr string) {
			defer func() {
				if p := recover(); p != nil {
					err = NewConnectionErr(managerID, managerPeerAddr, fmt.Errorf("%v", p))
				}
			}()
			defer wg.Done()
			rpcClient, err := vrpc.RPCDial("tcp", managerPeerAddr, logger, loggerOptions)
			defer rpcClient.Close()
			if err != nil {
				errorCh <- NewConnectionErr(managerID, managerPeerAddr, err)
				return
			}
			var ack bool
			if err := RpcCallTimeOut(rpcClient, fmt.Sprintf("ManagerRPCServer.Commit%vRPC", serviceMethod), msg, &ack); err != nil {
				switch err.(type) {
				case *RPCTimedout:
					errorCh <- NewTimeoutErr(managerID, managerPeerAddr, err)
				default:
					errorCh <- NewConnectionErr(managerID, managerPeerAddr, err)
				}
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
		switch err.(type) {
		case *TimeoutErr:
			manager.TransactionCache.Add(msg.Hash(), ABORT)
			mrpc.abort(msg, peerAddrs)
		default:
			var ack bool
			method := reflect.ValueOf(mrpc).MethodByName(fmt.Sprintf("Commit%vRPC", serviceMethod))

			if err := method.Call([]reflect.Value{reflect.ValueOf(msg), reflect.ValueOf(&ack)})[0].Interface(); err != nil {
				mrpc.abort(msg, peerAddrs)
				manager.TransactionCache.Add(msg.Hash(), ABORT)
				return fmt.Errorf("coordinator failed: transaction aborted: %v", err)
			}
			manager.TransactionCache.Add(msg.Hash(), COMMIT)
		}
		return err
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

	return nil
}

func (mrpc *ManagerRPCServer) abort(msg *m.Message, peerAddrs map[ManagerNodeID]string) {
	errorCh := make(chan error, 1)
	wg := sync.WaitGroup{}

	for managerID, managerAddr := range peerAddrs {
		wg.Add(1)
		go func(managerID ManagerNodeID, managerAddr string) {
			defer func() {
				if p := recover(); p != nil {
					fmt.Println(NewAbortErr(fmt.Errorf("%v", p)))
				}
			}()
			defer wg.Done()
			rpcClient, err := vrpc.RPCDial("tcp", managerAddr, logger, loggerOptions)
			defer rpcClient.Close()
			if err != nil {
				fmt.Println(NewAbortErr(NewConnectionErr(managerID, managerAddr, err)))
				return
			}
			var ack bool
			if err := RpcCallTimeOut(rpcClient, fmt.Sprintf("ManagerRPCServer.AbortRPC"), msg, &ack); err != nil {
				fmt.Println(NewAbortErr(NewConnectionErr(managerID, managerAddr, err)))
				return
			}
			if !ack {
				errorCh <- fmt.Errorf("peer disagrees")
			}
		}(managerID, managerAddr)
	}

	wg.Wait()

	fmt.Println("Abort Done")
}

//-----------------------------------------------------------------------------------------------------------------------------

func (mrpc *ManagerRPCServer) CanCommitRPC(msg *m.Message, state *State) error {
	v, exist := manager.TransactionCache.Get(msg.Hash())
	if exist {
		s, ok := v.(State)
		if !ok {
			return fmt.Errorf("Couldn't typecast interface value: %v to State", s)
		}
		if s == COMMIT || s == PREPARE {
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

	manager.TransactionCache.Add(msg.Hash(), APPROVE)
	println("CanCommitRPC prepare")
	*state = APPROVE
	return nil
}

func (mrpc *ManagerRPCServer) PreCommitRPC(msg *m.Message, ack *bool) error {
	*ack = false
	manager.TransactionCache.Add(msg.Hash(), PREPARE)
	*ack = true
	return nil
}

func (mrpc *ManagerRPCServer) AbortRPC(msg *m.Message, ack *bool) error {
	*ack = true
	manager.TransactionCache.Add(msg.Hash(), ABORT)
	return nil
}

func (mrpc *ManagerRPCServer) CommitRegisterPeerRPC(msg *m.Message, ack *bool) error {
	*ack = false
	peerManagerID := ManagerNodeID(msg.ID)
	peerManagerAddr := msg.Text

	if peerManagerID == manager.ManagerNodeID {
		*ack = true
		manager.TransactionCache.Add(msg.Hash(), COMMIT)
		return nil
	}

	manager.ManagerMutex.Lock()
	manager.ManagerPeers[peerManagerID] = peerManagerAddr
	manager.ManagerMutex.Unlock()

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

	fmt.Printf("Delete Peer - %v - %v\n", peerManagerID, peerManagerAddr)
	fmt.Println("Peer Map: ", manager.ManagerPeers)

	manager.TransactionCache.Add(msg.Hash(), COMMIT)
	*ack = true
	return nil
}

func (mrpc *ManagerRPCServer) CommitAddBrokerRPC(msg *m.Message, ack *bool) error {
	*ack = false
	brokerID := BrokerNodeID(msg.ID)
	brokerAddr := msg.Text

	manager.BrokerMutex.Lock()
	manager.BrokerNodes[brokerID] = brokerAddr
	manager.BrokerMutex.Unlock()

	fmt.Printf("Added Broker - %v - %v\n", brokerID, brokerAddr)
	fmt.Println("Broker Map: ", manager.BrokerNodes)

	manager.TransactionCache.Add(msg.Hash(), COMMIT)
	*ack = true
	return nil
}

func (mrpc *ManagerRPCServer) CommitAddTopicRPC(msg *m.Message, ack *bool) error {
	*ack = false

	var topic Topic

	if err := json.Unmarshal(msg.Payload, &topic); err != nil {
		fmt.Println(err)
		return err
	}

	manager.TopicMutex.Lock()
	manager.TopicMap[msg.Topic] = topic
	manager.TopicMutex.Unlock()

	*ack = true
	return nil
}

//TODOs
func (mrpc *ManagerRPCServer) CommitSubscribeClientRPC(msg *m.Message, ack *bool) error {

	return nil
}

//----------------------------------------------------------------------------------------------------------------------

func (mrpc *ManagerRPCServer) CommitRegistBrokerRPC(msg *m.Message, ack *bool) error {
	*ack = false
	BrokerNodeID := BrokerNodeID(msg.ID)
	BrokerAddr := msg.Text

	fmt.Println(BrokerAddr, BrokerNodeID)
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

func (mrpc *ManagerRPCServer) GetManagerRPC(nodeID ManagerNodeID, node *ManagerNode) error {
	fmt.Println("Recived Get Manager Request")
	manager.MU.Lock()
	*node = *manager
	manager.MU.Unlock()
	return nil
}

// 	if err := json.Unmarshal(msg.Payload, &manager); err!= nil{
// 		return err
// 	}

// 	return nil
// }

/*----------------------------------------------------------------------------------------------------------------*/

func spawnRPCServer() error {
	mRPC := new(ManagerRPCServer)
	server := rpc.NewServer()
	server.Register(mRPC)

	tcpAddr, err := net.ResolveTCPAddr("tcp", "0.0.0.0:9080")
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
	}

	listener, err := net.ListenTCP("tcp", tcpAddr)
	defer listener.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
	}

	manager.ManagerIP = config.ManagerIP

	fmt.Printf("Serving RPC Server at: %v\n", manager.ManagerIP)
	vrpc.ServeRPCConn(server, listener, logger, loggerOptions)

	return nil
}

//------------------------------------------------------------------------------------------------------------------

func (mrpc *ManagerRPCServer) Ack(addr string, ack *bool) error {
	fmt.Println("incoming Addr", addr)
	*ack = true
	return nil
}

//TODO:
func (mrpc *ManagerRPCServer) GetLeader(request *m.Message, response *string) error {
	println("Getting leader...")
	if request.Type == m.GET_LEADER {
		requestedTopic, ok := manager.TopicMap[request.Topic]
		// not found topic name
		if !ok || int(request.PartitionIdx) >= len(requestedTopic.Partitions)-1 {
			*response = "No leader for that topic/partition."
			return nil
		}
		partition := requestedTopic.Partitions[request.PartitionIdx]
		leaderIP := manager.BrokerNodes[partition.LeaderNodeID]
		*response = leaderIP
	} else {
		*response = "No leader for that topic/partition."
	}
	return nil
}

//TODO:
func (mrpc *ManagerRPCServer) GetTopicList(request *m.Message, response *map[string]int) error {
	if request.Type == m.TOPIC_LIST {
		allTopics := make(map[string]int)
		for topicName, topic := range manager.TopicMap {
			if topicName != "" {
				allTopics[topicName] = len(topic.Partitions)
			}
		}
		*response = allTopics
	}
	return nil
}

func getHashingNodes(key string, replicaCount int) []string {
	var list []string

	manager.BrokerMutex.Lock()
	defer manager.BrokerMutex.Unlock()

	for k := range manager.BrokerNodes {
		list = append(list, string(k))
	}

	ring := hashring.New(list)
	nodeIDs, _ := ring.GetNodes(key, replicaCount)

	return nodeIDs
}

// func printTopicMap() {
// 	println("------------")
// 	for key, v := range manager.TopicMap {
// 		println("topic:", key)
// 		println("LeaderIP:", v.LeaderIP)
// 		fmt.Printf("FollowerIP: %v\n", v.FollowerIPs)
// 		println("PartitionNum:", v.PartitionNum)
// 	}
// 	println("------------")
// }

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
	case <-time.After(time.Duration(100) * time.Second):
		return NewRPCTimedout(rpcCall.ServiceMethod)
	}
	return nil
}

func NewConnectionErr(nodeID ManagerNodeID, addr string, err error) *ConnectionErr {
	return &ConnectionErr{
		Addr:   addr,
		NodeID: nodeID,
		Err:    err,
	}
}

func (e *ConnectionErr) Error() string {
	return fmt.Sprintf("bad connection - %v - %v: %v", e.NodeID, e.Addr, e.Err)
}

func NewTransactionErr(err error, msg string) *TransactionErr {
	return &TransactionErr{
		Err: err,
		Msg: msg,
	}
}

func (e *TransactionErr) Error() string {
	return fmt.Sprintf("%v: transaction aborted: %v", e.Msg, e.Err)
}

func NewRPCTimedout(serviceMethod string) *RPCTimedout {
	return &RPCTimedout{
		ServiceMethod: serviceMethod,
	}
}

func (e *RPCTimedout) Error() string {
	return fmt.Sprintf("rpc call: %v has timed out", e.ServiceMethod)
}

func NewAgreementErr(msg string) *AgreementErr {
	return &AgreementErr{
		msg: msg,
	}
}

func (e *AgreementErr) Error() string {
	return fmt.Sprintf("node disagress - %v", e.msg)
}

func NewRecoveryErr(err error) *RecoveryErr {
	return &RecoveryErr{
		Err: err,
	}
}

func (e *RecoveryErr) Error() string {
	return fmt.Sprintf("reovery error - %v", e.Err)
}

func NewAbortErr(err error) *AbortErr {
	return &AbortErr{
		Err: err,
	}
}

func (e *AbortErr) Error() string {
	return fmt.Sprintf("abort error - %v", e.Err)
}

func NewTimeoutErr(nodeID ManagerNodeID, addr string, err error) *TimeoutErr {
	return &TimeoutErr{
		Addr:   addr,
		NodeID: nodeID,
		Err:    err,
	}
}

func (e *TimeoutErr) Error() string {
	return fmt.Sprintf("connection timed out - %v - %v: %v", e.NodeID, e.Addr, e.Err)
}

// func (t *Topic) Hash() [sha1.Size]byte {
// 	var buf = []byte{}
// 	for _, partition := range t {
// 		buf = append(buf, []byte(strconv.FormatUint(uint64(partition.PartitionIdx), 10))...)
// 		buf = append(buf, []byte(Partition.LeaderIP)...)

// 		for _, ip := range partition.FollowerIPs {
// 			buf = append(buf, []byte(ip)...)
// 		}
// 	}
// 	return sha1.Sum(buf)
// }

func (p *Partition) HashString() string {
	var buf = []byte{}
	buf = append(buf, []byte(p.TopicName)...)
	buf = append(buf, []byte(strconv.FormatUint(uint64(p.PartitionIdx), 10))...)

	hash := sha1.Sum(buf)
	return string(hash[:])
}
