package main

import (
	"crypto/sha1"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"

	basicIO "../lib/IOlib"
	m "../lib/message"
	"github.com/DistributedClocks/GoVector/govec/vrpc"
	lru "github.com/hashicorp/golang-lru"
)

type BrokerRPCServer int

type Status int

type consumerId string

type record struct {
	data      [512]byte
	timestamp time.Time
}

type partition []*record

type BrokerNodeID string

type PartitionID string

type ROLE uint

const (
	LEADER ROLE = iota
	FOLLOWER
)

type State uint

type ClientID string

const (
	READY State = iota
	WAIT
	APPROVE
	PREPARE
	COMMIT
	ABORT
)

type Partition struct {
	TopicName        string
	PartitionIdx     uint8
	ReplicationNum   uint8
	Partitions       uint8
	Role             ROLE
	contentMu        *sync.Mutex
	Contents         []string
	LastContentIndex int
	LeaderIP         string
	Followers        map[BrokerNodeID]string
	ClientOffsetMap  map[ClientID]uint
}

type BrokerNode struct {
	brokerNodeID     BrokerNodeID
	brokerAddr       string
	brokerPeers      map[BrokerNodeID]string
	partitionMap     map[PartitionID]*Partition
	partitionMu      *sync.Mutex
	brokerPeerMu     *sync.Mutex
	transactionCache *lru.Cache
	ManagerIPs       []string
}

type ConnectionErr struct {
	Addr   string
	NodeID BrokerNodeID
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
	NodeID BrokerNodeID
	Err    error
}

//Error during Abort
type AbortErr struct {
	Err error
}

const cacheSize = 10

var broker *BrokerNode

// Initialize starts the node as a Broker node in the network
func InitBroker(addr string) error {
	broker = &BrokerNode{
		brokerNodeID: BrokerNodeID(config.BrokerNodeID),
		partitionMap: make(map[PartitionID]*Partition),
		partitionMu:  &sync.Mutex{},
		brokerPeerMu: &sync.Mutex{},
		brokerPeers:  map[BrokerNodeID]string{},
		ManagerIPs:   []string{},
	}

	broker.brokerAddr = config.BrokerIP

	cache, err := lru.New(cacheSize)

	if err != nil {
		return err
	}

	broker.transactionCache = cache

	go registerBrokerWithManager()

	spawnListener(addr)

	return nil
}

// Spawn a rpc listen client
func spawnListener(addr string) {
	fmt.Println(addr)

	bRPC := new(BrokerRPCServer)
	server := rpc.NewServer()
	server.Register(bRPC)

	tcpAddr, err := net.ResolveTCPAddr("tcp", "0.0.0.0:8000")
	// tcpAddr, err := net.ResolveTCPAddr("tcp", config.BrokerIP)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
	}

	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
	}

	fmt.Printf("Serving Server at: %v\n", config.BrokerIP)

	vrpc.ServeRPCConn(server, listener, logger, loggerOptions)
}

func (brpc *BrokerRPCServer) CreateNewPartition(message *m.Message, ack *bool) error {
	println("CreateNewPartition...", message.Topic, message.PartitionIdx)

	*ack = false

	partition := &Partition{
		TopicName:       message.Topic,
		PartitionIdx:    message.PartitionIdx,
		ReplicationNum:  uint8(message.ReplicaNum),
		Partitions:      message.Partitions,
		Role:            ROLE(message.Role),
		LeaderIP:        broker.brokerAddr,
		Followers:       make(map[BrokerNodeID]string),
		ClientOffsetMap: make(map[ClientID]uint),
	}

	fmt.Println("IPs", message.IPs)

	for k, v := range message.IPs {
		partition.Followers[BrokerNodeID(k)] = v
	}

	message.Role = m.ROLE(FOLLOWER)

	fmt.Println("Ready for 3-phase commit")

	if err := brpc.threePC("CreateNewPartition", message, partition.Followers); err != nil {
		// TODO handle retry on connection error
		switch err.(type) {
		case *TimeoutErr:
			// connErr := err.(*ConnectionErr)

		case *ConnectionErr:
			connErr := err.(*ConnectionErr)

			deleteBrokerMsg := &m.Message{
				ID:           string(connErr.NodeID),
				Text:         connErr.Addr,
				Topic:        message.Topic,
				PartitionIdx: message.PartitionIdx,
				Proposer:     string(broker.brokerNodeID),
				Timestamp:    time.Now(),
			}

			var ack bool

			if err := brpc.DeleteBroker(deleteBrokerMsg, &ack); err != nil {

				return err
			}

			// if err := brpc.RequestNewPeer(newPeerRequest, &ack); err != nil {
			// 	return fmt.Errorf("delete node failed: %v", err)
			// }

			// if ack {
			// 	fmt.Println(manager.ManagerPeers)

			// 	if err := mrpc.threePC("RegisterPeer", msg, manager.ManagerPeers); err != nil {
			// 		return fmt.Errorf("retry failed: %v", err)
			// 	}
			// }
		default:
			fmt.Println("What the h* just happened?")
			return err
		}

	}

	*ack = true

	return nil
}

func (brpc *BrokerRPCServer) RequestNewPeer(msg *m.Message, ack *bool) error {
	return nil

}

func (brpc *BrokerRPCServer) DeleteBroker(msg *m.Message, ack *bool) error {

	return nil
}

// func (brpc *BrokerRPCServer) SetNewLeader(msg *m.Message, ack *bool) error {
// 	*ack = false
// 	p := broker.partitionMap[PartitionID(fmt.Sprintf("%v_%v", msg.Topic, msg.PartitionIdx))]

// 	if err := brpc.threePC("SetNewLeader", msg, p.Followers); err != nil {
// 		return err
// 	}

// 	*ack = true
// 	return nil
// }

// func (brpc *BrokerRPCServer) CommitSetNewLeaderRPC(msg *m.Message, ack *bool) error {
// 	*ack = false
// 	broker.partitionMu.Lock()
// 	p := broker.partitionMap[PartitionID(fmt.Sprintf("%v_%v", msg.Topic, msg.PartitionIdx))]
// 	p.LeaderIP = msg.Text

// 	broker.partitionMu.Unlock()

// }

func (brpc *BrokerRPCServer) AddBrokerPeer(msg *m.Message, ack *bool) error {
	if err := brpc.threePC("AddBrokerPeer", msg, broker.brokerPeers); err != nil {
		//TODOs, handle Error
		return err
	}
	return nil
}

func (brpc *BrokerRPCServer) CommitAddBrokerPeerRPC(msg *m.Message, ack *bool) error {
	*ack = false

	brokerPeerID := BrokerNodeID(msg.ID)
	brokerPeerAddr := msg.Text

	broker.brokerPeerMu.Lock()
	broker.brokerPeers[brokerPeerID] = brokerPeerAddr
	broker.brokerPeerMu.Unlock()

	*ack = true
	return nil
}

func (brpc *BrokerRPCServer) DeleteManagerAddr(msg *m.Message, ack *bool) error {

	if err := brpc.threePC("DeleteManagerAddr", msg, broker.brokerPeers); err != nil {
		return err
	}
	return nil
}

func (brpc *BrokerRPCServer) CommitDeleteManagerAddr(msg *m.Message, ack *bool) error {
	*ack = false

	*ack = true
	return nil
}

// func (b *BrokerServer) AddClient(m *Message, res *bool) error {
// 	topicId := m.Topic
// 	var rec record
// 	copy(rec[:], m.Payload.Marshall())
// 	Broker.topicList[topicId].partition = append(Broker.topicList[topicId].partition, &rec)
// 	*res = true
// 	return nil
// }

// func (b *BrokerServer) DispatchData(m *Message, res *bool) error {
// 	topicID := m.Topic
// 	clientId := m.Payload.Marshall()

// 	Broker.topicList[topicID].consumerOffset[consumerId(clientId)] = 0
// 	return nil
// }

// func (b *BrokerServer) AddFollowerToTopic(m *Message, res *bool) error {
// 	topicID := m.Topic
// 	followerAddr := m.Payload.Marshall()

// 	tcpAddr, err := net.ResolveTCPAddr("tcp", string(followerAddr))
// 	if err != nil {
// 		fmt.Fprintf(os.Stderr, err.Error())
// 	}

// 	Broker.topicList[topicID].FollowerList = append(Broker.topicList[topicID].FollowerList, tcpAddr)
// 	return nil
// }

// func broadcastToFollowers(stub interface{}) error {
// 	return nil
// }

func (brpc *BrokerRPCServer) threePC(serviceMethod string, msg *m.Message, peerAddrs map[BrokerNodeID]string) error {
	// canCommitPhase
	peerTransactionState, err := brpc.canCommit(serviceMethod, msg, peerAddrs)

	if err != nil {
		fmt.Println("Can commit Err: ", err)
		switch err.(type) {
		case *ConnectionErr:


			//
			// deleteMsg := m.Message{
			// 		ID:        string(ce.NodeID),
			// 		Text:      ce.Addr.String(),
			// 		Proposer:  string(manager.ManagerNodeID),
			// 		Timestamp: time.Now(),
			// }
			// var ack bool

			// if err := mrpc.DeletePeer(&deleteMsg, &ack); err != nil {
			// 		return fmt.Errorf("delete node failed: %v", err)
			// 	}

			// TODO: Needs to retry transaction by asking manager for a new peer

			// 	if ack {
			// 		if err := mrpc.threePC(serviceMethod, msg, manager.ManagerPeers); err != nil {
			// 			return fmt.Errorf("retry failed: %v", err)
			// 		}
			// 	}

			// }
		default:
			fmt.Println("default Error")
			return err
		}
		return nil
	}

	// recovery if needed
	if peerTransactionState != nil {
		var recoverPeerAddr = map[BrokerNodeID]string{}
		for k, v := range peerTransactionState {
			if v != COMMIT {
				recoverPeerAddr[k] = peerAddrs[k]
			}
		}
		if err := brpc.recoverPhase(serviceMethod, msg, recoverPeerAddr); err != nil {
					fmt.Println("Can commit Err: ", err)
		switch err.(type) {
		case *ConnectionErr:


			//
			// deleteMsg := m.Message{
			// 		ID:        string(ce.NodeID),
			// 		Text:      ce.Addr.String(),
			// 		Proposer:  string(manager.ManagerNodeID),
			// 		Timestamp: time.Now(),
			// }
			// var ack bool

			// if err := mrpc.DeletePeer(&deleteMsg, &ack); err != nil {
			// 		return fmt.Errorf("delete node failed: %v", err)
			// 	}

			// TODO: Needs to retry transaction by asking manager for a new peer

			// 	if ack {
			// 		if err := mrpc.threePC(serviceMethod, msg, manager.ManagerPeers); err != nil {
			// 			return fmt.Errorf("retry failed: %v", err)
			// 		}
			// 	}

			// }
		default:
			fmt.Println("default Error")
			return err
		}
		return nil
			return err
		}
		return nil
	}

	// preCommitPhase
	if err := brpc.preCommit(serviceMethod, msg, peerAddrs); err != nil {
				fmt.Println("Can commit Err: ", err)
		switch err.(type) {
		case *ConnectionErr:


			//
			// deleteMsg := m.Message{
			// 		ID:        string(ce.NodeID),
			// 		Text:      ce.Addr.String(),
			// 		Proposer:  string(manager.ManagerNodeID),
			// 		Timestamp: time.Now(),
			// }
			// var ack bool

			// if err := mrpc.DeletePeer(&deleteMsg, &ack); err != nil {
			// 		return fmt.Errorf("delete node failed: %v", err)
			// 	}

			// TODO: Needs to retry transaction by asking manager for a new peer

			// 	if ack {
			// 		if err := mrpc.threePC(serviceMethod, msg, manager.ManagerPeers); err != nil {
			// 			return fmt.Errorf("retry failed: %v", err)
			// 		}
			// 	}

			// }
		default:
			fmt.Println("default Error")
			return err
		}
		return nil
		return err
	}

	// commitPhase
	if err := brpc.commit(serviceMethod, msg, peerAddrs); err != nil {
				fmt.Println("Can commit Err: ", err)
		switch err.(type) {
		case *ConnectionErr:


			//
			// deleteMsg := m.Message{
			// 		ID:        string(ce.NodeID),
			// 		Text:      ce.Addr.String(),
			// 		Proposer:  string(manager.ManagerNodeID),
			// 		Timestamp: time.Now(),
			// }
			// var ack bool

			// if err := mrpc.DeletePeer(&deleteMsg, &ack); err != nil {
			// 		return fmt.Errorf("delete node failed: %v", err)
			// 	}

			// TODO: Needs to retry transaction by asking manager for a new peer

			// 	if ack {
			// 		if err := mrpc.threePC(serviceMethod, msg, manager.ManagerPeers); err != nil {
			// 			return fmt.Errorf("retry failed: %v", err)
			// 		}
			// 	}

			// }
		default:
			fmt.Println("default Error")
			return err
		}
		return nil
		return err
	}
	return nil
}

func (brpc *BrokerRPCServer) recoverPhase(serviceMethod string, msg *m.Message, peerAddrs map[BrokerNodeID]string) (err error) {
	fmt.Println("Begin Recover")
	errorCh := make(chan error, 1)
	wg := sync.WaitGroup{}

	for k, v := range peerAddrs {
		if k == broker.brokerNodeID {
			continue
		}
		wg.Add(1)

		go func(brokerID BrokerNodeID, brokerAddr string) {
			defer func() {
				if p := recover(); p != nil {
					err = NewConnectionErr(brokerID, brokerAddr, fmt.Errorf("%v", p))
				}
			}()
			defer wg.Done()
			rpcClient, err := vrpc.RPCDial("tcp", brokerAddr, logger, loggerOptions)
			defer rpcClient.Close()
			if err != nil {
				errorCh <- NewConnectionErr(brokerID, brokerAddr, err)
				return
			}
			var ack bool
			if err := RpcCallTimeOut(rpcClient, fmt.Sprintf("BrokerRPCServer.Commit%vRPC", serviceMethod), msg, &ack); err != nil {
				switch err.(type) {
				case *RPCTimedout:
					errorCh <- NewTimeoutErr(brokerID, brokerAddr, err)
				default:
					errorCh <- NewConnectionErr(brokerID, brokerAddr, err)
				}
				return
			}
		}(k, v)
	}

	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case err := <-errorCh:
		broker.transactionCache.Add(msg.Hash(), ABORT)
		return NewRecoveryErr(err)
	case <-c:
		fmt.Println("Commit Phase Done")
	}

	if _, exists := peerAddrs[broker.brokerNodeID]; exists {
		// Local Commit
		var ack bool

		method := reflect.ValueOf(brpc).MethodByName(fmt.Sprintf("Commit%vRPC", serviceMethod))
		if err := method.Call([]reflect.Value{reflect.ValueOf(msg), reflect.ValueOf(&ack)})[0].Interface(); err != nil {
			broker.transactionCache.Add(msg.Hash(), ABORT)
			newErr := err.(error)
			return NewTransactionErr(newErr, "local canCommit")
		}
	}
	fmt.Println("Done Recover")
	return nil
}

func (brpc *BrokerRPCServer) canCommit(serviceMethod string, msg *m.Message, peerAddrs map[BrokerNodeID]string) (map[BrokerNodeID]State, error) {
	// canCommitPhase
	fmt.Println("canCommitPhase")
	v, exist := broker.transactionCache.Get(msg.Hash())
	var s State

	if exist {
		s, ok := v.(State)
		if !ok {
			return nil, fmt.Errorf("Couldn't typecast interface value: %v to State", s)
		}
	} else {
		s = READY
	}

	peerTransactionState := make(map[BrokerNodeID]State)
	peerTransactionState[broker.brokerNodeID] = s

	var wg sync.WaitGroup
	errorCh := make(chan error, 1)

	for brokerID, brokerAddr := range peerAddrs {
		wg.Add(1)
		go func(brokerID BrokerNodeID, brokerAddr string) {
			// Prevent Closure
			defer func() {
				if p := recover(); p != nil {
					errorCh <- NewConnectionErr(brokerID, brokerAddr, fmt.Errorf("%v", p))
				}
			}()
			defer wg.Done()
			rpcClient, err := vrpc.RPCDial("tcp", brokerAddr, logger, loggerOptions)
			defer rpcClient.Close()
			if err != nil {
				errorCh <- NewConnectionErr(brokerID, brokerAddr, err)
				return
			}
			var s State
			if err := RpcCallTimeOut(rpcClient, fmt.Sprintf("BrokerRPCServer.CanCommitRPC"), msg, &s); err != nil {
				switch err.(type) {
				case *RPCTimedout:
					errorCh <- NewTimeoutErr(brokerID, brokerAddr, err)
				default:
					errorCh <- NewConnectionErr(brokerID, brokerAddr, err)
				}
				return
			}
			peerTransactionState[brokerID] = s
		}(brokerID, brokerAddr)
	}

	broker.transactionCache.Add(msg.Hash(), WAIT)

	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()

	select {
	case err := <-errorCh:
		broker.transactionCache.Add(msg.Hash(), ABORT)
		// best effort error msg sent
		brpc.abort(msg, peerAddrs)
		return nil, err
	case <-c:
	}

	// Local canCommit
	method := reflect.ValueOf(brpc).MethodByName(fmt.Sprintf("CanCommitRPC"))
	if err := method.Call([]reflect.Value{reflect.ValueOf(msg), reflect.ValueOf(&s)})[0].Interface(); err != nil {
		broker.transactionCache.Add(msg.Hash(), ABORT)
		brpc.abort(msg, peerAddrs)
		errTmp := err.(error)
		return nil, errTmp
	}

	for _, v := range peerTransactionState {
		if v == COMMIT || v == PREPARE {
			return peerTransactionState, nil
		}
	}

	fmt.Println("canCommitPhase Done")

	return nil, nil
}

func (brpc *BrokerRPCServer) preCommit(serviceMethod string, msg *m.Message, peerAddrs map[BrokerNodeID]string) (err error) {
	// preCommitPhase
	fmt.Println("PreCommit Phase")
	errorCh := make(chan error, 1)
	wg := sync.WaitGroup{}
	for brokerID, brokerAddr := range peerAddrs {
		wg.Add(1)
		go func(brokerID BrokerNodeID, brokerAddr string) {
			defer func() {
				if p := recover(); p != nil {
					err = NewConnectionErr(brokerID, brokerAddr, fmt.Errorf("%v", p))
				}
			}()
			defer wg.Done()
			rpcClient, err := vrpc.RPCDial("tcp", brokerAddr, logger, loggerOptions)
			defer rpcClient.Close()
			if err != nil {
				errorCh <- NewConnectionErr(brokerID, brokerAddr, err)
				return
			}
			var ack bool
			if err := RpcCallTimeOut(rpcClient, fmt.Sprintf("BrokerRPCServer.PreCommitRPC"), msg, &ack); err != nil {
				switch err.(type) {
				case *RPCTimedout:
					errorCh <- NewTimeoutErr(brokerID, brokerAddr, err)
				default:
					errorCh <- NewConnectionErr(brokerID, brokerAddr, err)
				}
				return
			}
			if !ack {
				errorCh <- fmt.Errorf("peer disagrees")
			}
		}(brokerID, brokerAddr)
	}

	broker.transactionCache.Add(msg.Hash(), PREPARE)

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
			broker.transactionCache.Add(msg.Hash(), ABORT)
			brpc.abort(msg, peerAddrs)
		default:
			var ack bool
			method := reflect.ValueOf(brpc).MethodByName(fmt.Sprintf("Commit%vRPC", serviceMethod))
			if err := method.Call([]reflect.Value{reflect.ValueOf(msg), reflect.ValueOf(&ack)})[0].Interface(); err != nil {
				brpc.abort(msg, peerAddrs)
				broker.transactionCache.Add(msg.Hash(), ABORT)
				return fmt.Errorf("coordinator failed: transaction aborted: %v", err)
			}
			broker.transactionCache.Add(msg.Hash(), COMMIT)
		}
		return err
	case <-c:
		fmt.Println("PreCommit Phase Done")
	}
	return nil
}

func (brpc *BrokerRPCServer) commit(serviceMethod string, msg *m.Message, peerAddrs map[BrokerNodeID]string) (err error) {
	fmt.Println("Commit Phase")
	errorCh := make(chan error, 1)
	wg := sync.WaitGroup{}
	for brokerID, brokerAddr := range peerAddrs {
		wg.Add(1)
		go func(brokerID BrokerNodeID, brokerAddr string) {
			defer func() {
				if p := recover(); p != nil {
					err = NewConnectionErr(brokerID, brokerAddr, fmt.Errorf("%v", p))
				}
			}()
			defer wg.Done()
			rpcClient, err := vrpc.RPCDial("tcp", brokerAddr, logger, loggerOptions)
			defer rpcClient.Close()
			if err != nil {
				errorCh <- NewConnectionErr(brokerID, brokerAddr, err)
				return
			}
			var ack bool
			if err := RpcCallTimeOut(rpcClient, fmt.Sprintf("BrokerRPCServer.Commit%vRPC", serviceMethod), msg, &ack); err != nil {
				switch err.(type) {
				case *RPCTimedout:
					errorCh <- NewTimeoutErr(brokerID, brokerAddr, err)
				default:
					errorCh <- NewConnectionErr(brokerID, brokerAddr, err)
				}
				return
			}

			if !ack {
				errorCh <- fmt.Errorf("peer disagrees")
			}
		}(brokerID, brokerAddr)
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
			broker.transactionCache.Add(msg.Hash(), ABORT)
			brpc.abort(msg, peerAddrs)
		default:
			var ack bool
			method := reflect.ValueOf(brpc).MethodByName(fmt.Sprintf("Commit%vRPC", serviceMethod))

			if err := method.Call([]reflect.Value{reflect.ValueOf(msg), reflect.ValueOf(&ack)})[0].Interface(); err != nil {
				brpc.abort(msg, peerAddrs)
				broker.transactionCache.Add(msg.Hash(), ABORT)
				return fmt.Errorf("coordinator failed: transaction aborted: %v", err)
			}
			broker.transactionCache.Add(msg.Hash(), COMMIT)
		}
		return err
	case <-c:
		fmt.Println("Commit Phase Done")
	}

	// Local Commit
	var ack bool

	method := reflect.ValueOf(brpc).MethodByName(fmt.Sprintf("Commit%vRPC", serviceMethod))

	if err := method.Call([]reflect.Value{reflect.ValueOf(msg), reflect.ValueOf(&ack)})[0].Interface(); err != nil {
		brpc.abort(msg, peerAddrs)
		broker.transactionCache.Add(msg.Hash(), ABORT)
		return fmt.Errorf("coordinator failed: transaction aborted: %v", err)
	}

	broker.transactionCache.Add(msg.Hash(), COMMIT)

	return nil
}

func (brpc *BrokerRPCServer) abort(msg *m.Message, peerAddrs map[BrokerNodeID]string) {
	errorCh := make(chan error, 1)
	wg := sync.WaitGroup{}

	for brokerID, brokerAddr := range peerAddrs {
		wg.Add(1)
		go func(brokerID BrokerNodeID, brokerAddr string) {
			defer func() {
				if p := recover(); p != nil {
					fmt.Println(NewAbortErr(fmt.Errorf("%v", p)))
				}
			}()
			defer wg.Done()
			rpcClient, err := vrpc.RPCDial("tcp", brokerAddr, logger, loggerOptions)
			defer rpcClient.Close()
			if err != nil {
				fmt.Println(NewAbortErr(NewConnectionErr(brokerID, brokerAddr, err)))
				return
			}
			var ack bool
			if err := RpcCallTimeOut(rpcClient, fmt.Sprintf("BrokerRPCServer.AbortRPC"), msg, &ack); err != nil {
				fmt.Println(NewAbortErr(NewConnectionErr(brokerID, brokerAddr, err)))
				return
			}
			if !ack {
				errorCh <- fmt.Errorf("peer disagrees")
			}
		}(brokerID, brokerAddr)
	}
	wg.Wait()
	fmt.Println("Abort Done")
}

//-------------------------------------------------------------------------------------------------------------------------------------

func (brpc *BrokerRPCServer) CanCommitRPC(msg *m.Message, state *State) error {

	v, exist := broker.transactionCache.Get(msg.Hash())
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

	broker.transactionCache.Add(msg.Hash(), APPROVE)
	*state = APPROVE
	return nil
}

func (brpc *BrokerRPCServer) CommitPublishMessageRPC(msg *m.Message, ack *bool) error {
	*ack = true
	// zero-copy write
	fname := fmt.Sprintf("./disk/%v_%v_%v", config.BrokerNodeID, msg.Topic, msg.PartitionIdx)
	err := basicIO.WriteFile(fname, msg.Text, true)
	if err != nil {
		return err
	}
	indexID := (PartitionID)(msg.Topic + "_" + strconv.FormatUint(uint64(msg.PartitionIdx), 10))
	partition, ok := broker.partitionMap[indexID]
	// not found topic or partition
	if !ok {
		*ack = false
		return nil
	}
	partition.contentMu.Lock()
	partition.LastContentIndex++
	partition.Contents[partition.LastContentIndex] = msg.Text
	partition.contentMu.Unlock()
	println("Message Comitted")
	return nil
}

func (brpc *BrokerRPCServer) PreCommitRPC(msg *m.Message, ack *bool) error {
	*ack = false
	broker.transactionCache.Add(msg.Hash(), PREPARE)
	*ack = true
	return nil
}

func (brpc *BrokerRPCServer) AbortRPC(msg *m.Message, ack *bool) error {
	*ack = true
	broker.transactionCache.Add(msg.Hash(), ABORT)
	return nil
}

func (brpc *BrokerRPCServer) CommitCreateNewPartitionRPC(message *m.Message, ack *bool) error {

	println("******", message.PartitionIdx)

	partition := &Partition{
		TopicName:        message.Topic,
		PartitionIdx:     message.PartitionIdx,
		ReplicationNum:   uint8(message.ReplicaNum),
		Partitions:       message.Partitions,
		Role:             ROLE(message.Role),
		LeaderIP:         broker.brokerAddr,
		Followers:        make(map[BrokerNodeID]string),
		Contents:         make([]string, 100),
		contentMu:        &sync.Mutex{},
		LastContentIndex: -1,
	}

	broker.partitionMu.Lock()
	broker.partitionMap[PartitionID(fmt.Sprintf("%v_%v", partition.TopicName, partition.PartitionIdx))] = partition
	// broker.partitionMap[PartitionID(partition.HashString())] = partition
	broker.partitionMu.Unlock()

	for nid, ip := range message.IPs {
		partition.Followers[BrokerNodeID(nid)] = ip
	}

	fname := fmt.Sprintf("./disk/%v_%v_%v", config.BrokerNodeID, partition.TopicName, partition.PartitionIdx)
	err := basicIO.WriteFile(fname, "", false)
	if err != nil {
		return err
	}

	fmt.Println("Successfully Added Partition", fname)

	*ack = true
	return nil
}

//---------------------------------------------------------------------------------------------------------

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

func NewConnectionErr(nodeID BrokerNodeID, addr string, err error) *ConnectionErr {
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

func NewTimeoutErr(nodeID BrokerNodeID, addr string, err error) *TimeoutErr {
	return &TimeoutErr{
		Addr:   addr,
		NodeID: nodeID,
		Err:    err,
	}
}

func (e *TimeoutErr) Error() string {
	return fmt.Sprintf("connection timed out - %v - %v: %v", e.NodeID, e.Addr, e.Err)
}

func (p *Partition) HashString() string {
	var buf = []byte{}
	buf = append(buf, []byte(p.TopicName)...)
	buf = append(buf, []byte(strconv.FormatUint(uint64(p.PartitionIdx), 10))...)

	hash := sha1.Sum(buf)
	return string(hash[:])
}

/* Provider / Consumer services */
// TODO:
func (brpc *BrokerRPCServer) PublishMessage(msg *m.Message, ack *bool) error {
	*ack = false
	if msg.Type == m.PUSHMESSAGE {
		indexID := (PartitionID)(msg.Topic + "_" + strconv.FormatUint(uint64(msg.PartitionIdx), 10))
		println(indexID)
		partition, ok := broker.partitionMap[indexID]
		// not found topic or partition
		if !ok {
			*ack = false
			return nil
		}
		if err := brpc.threePC("PublishMessage", msg, partition.Followers); err != nil {
			return err
		}
		println("Message Published to: " + msg.Topic)
		*ack = true
	}
	return nil
}

func (mrpc *BrokerRPCServer) ConsumeAt(request *m.Message, response *m.Message) error {

	if request.Type == m.CONSUME_MESSAGE {

		indexID := (PartitionID)(request.Topic + "_" + strconv.FormatUint(uint64(request.PartitionIdx), 10))

		partition, ok := broker.partitionMap[indexID]

		// not found topic or partition
		println("Request Idx")
		println(request.Index)
		println("Last content index")
		println(partition.LastContentIndex)
		if !ok || request.Index > partition.LastContentIndex {
			response.Index = partition.LastContentIndex
			response.Text = "Topic/Partition/Index not found"
			return nil
		}

		response.Index = partition.LastContentIndex
		//fileName := basicIO.Sendfile(,config.BrokerNodeID + "_" + indexID)
		println("Read from " + partition.TopicName)
		// do stuff here to get the payload from disk
		response.Payload = []byte(partition.Contents[request.Index])
	}

	return nil
}

func (mrpc *BrokerRPCServer) GetLatestIndex(request *m.Message, response *int) error {
	if request.Type == m.GET_LATEST_INDEX {
		indexID := (PartitionID)(request.Topic + "_" + strconv.FormatUint(uint64(request.PartitionIdx), 10))
		partition, err := broker.partitionMap[indexID]
		if !err || request.Index > partition.LastContentIndex {
			*response = -1
			return nil
		}
		// not found topic or partition
		println(partition.LastContentIndex)
		*response = partition.LastContentIndex
	}
	return nil
}
