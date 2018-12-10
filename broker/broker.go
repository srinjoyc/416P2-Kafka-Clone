package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"

	IOlib "../lib/IOlib"
	m "../lib/message"
	"github.com/DistributedClocks/GoVector/govec/vrpc"
)

type BrokerRPCServer int

type Status int

type consumerId string

type record struct {
	data      [512]byte
	timestamp time.Time
}

type partition []*record

type NodeID string

type TopicID string

type State uint

type Topic struct {
	TopicName    string
	ReplicaNum   uint8
	PartitionNum uint8
	Role         Status
	Buffer       []*record
	FollowerList map[NodeID]net.Addr
}

const (
	Leader Status = iota
	Follower
)

const (
	READY State = iota
	WAIT
	APPROVE
	PREPARE
	COMMIT
	ABORT
)

type BrokerNode struct {
	brokerNodeID NodeID
	brokerAddr net.Addr
	managerAddr net.Addr
	topicList map[TopicID]*Topic
}

type ConnectionErr struct {
	Addr   net.Addr
	NodeID NodeID
	Err    error
}

type TransactionErr struct {
	Err error
	Msg string
}

type RPCTimedout struct {
	ServiceMethod string
}

var broker *BrokerNode

// Initialize starts the node as a Broker node in the network
func InitBroker(addr string) error {
	broker = &BrokerNode{
		brokerNodeID: NodeID(config.BrokerNodeID),
		topicList: make(map[TopicID]*Topic),
	}

	brokerAddr, err := net.ResolveTCPAddr("tcp", config.BrokerIP)
	if err!= nil{
		return err
	}
	broker.brokerAddr = brokerAddr

	managerAddr, err := net.ResolveTCPAddr("tcp", config.ManagerIP)
	if err!= nil{
		return err
	}
	broker.managerAddr = managerAddr

	go spawnListener(addr)
	if err := registerBrokerWithManager(); err != nil {
		return err
	}
	for {
	}
	return nil
}

// Spawn a rpc listen client
func spawnListener(addr string) {
	fmt.Println(addr)

	bRPC := new(BrokerRPCServer)
	server := rpc.NewServer()
	server.Register(bRPC)

	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
	}

	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
	}

	fmt.Printf("Serving Server at: %v\n", tcpAddr.String())

	vrpc.ServeRPCConn(server, listener, logger, loggerOptions)
}

func (bs *BrokerRPCServer) CreateTopic(message *m.Message, ack *bool) error {
	*ack = true
	fmt.Println("\nLeader ID", config.BrokerNodeID)
	fmt.Println("Leader Topic Name", message.Topic)

	topic := &Topic{
		TopicName:    message.Topic,
		ReplicaNum:   uint8(message.ReplicaNum),
		PartitionNum: message.Partition,
		Role:         Leader,
	}
	
	var wg sync.WaitGroup
	errorCh := make(chan error, 1)

	for _, addr := range message.IPs {
		tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
		if err != nil {
			fmt.Println(err)
			continue
		}

		wg.Add(1)

		go func(addr net.Addr) {
			
			defer func() {
				if p := recover(); p != nil {
					errorCh <- fmt.Errorf("bad connection - %v: %v", addr, p)
				}
			}()
			defer wg.Done()

			rpcClient, err := vrpc.RPCDial("tcp", tcpAddr.String(), logger, loggerOptions)
			defer rpcClient.Close()
			if err != nil {
				errorCh <- fmt.Errorf("bad connection - %v: %v", addr, err)
				return
			}
			
			var res string

			if err := RpcCallTimeOut(rpcClient, fmt.Sprintf("BrokerRPCServer.GetNodeID"), string(broker.brokerNodeID), &res); err != nil {
				errorCh <- fmt.Errorf("bad connection - %v: %v", addr, err)
				return
			}

			topic.FollowerList[NodeID(res)] = addr

		}(tcpAddr)

	}

	wg.Wait()

	topicID := TopicID(message.ID)


	if _, exist := broker.topicList[topicID]; exist {
		return fmt.Errorf("Topic ID has already existed")
	}

	followerMessage := &m.Message{
		Topic:      message.Topic,
		Type:       message.Type,
		Partition:  message.Partition,
		ReplicaNum: message.ReplicaNum,
		IPs:        message.IPs,
	}


	if err := bs.threePC("AddTopic", followerMessage, topic.FollowerList); err!=nil{
		return err
	}

	return nil
}

func broadcastToFollowers(message m.Message, addr string, w *sync.WaitGroup) error {

	client, err := vrpc.RPCDial("tcp", addr, logger, loggerOptions)
	if err != nil {
		log.Fatal(err)
	}

	var ack bool
	println("broadcasting", addr)
	if err := client.Call("BrokerRPCServer.StartFollower", message, &ack); err != nil {
		return err
	}

	w.Done()

	return nil
}

func (bs *BrokerRPCServer) GetNodeID(srcNodeID string, nodeID *string) error {
	*nodeID = string(broker.brokerNodeID)
	return nil
}

func (bs *BrokerRPCServer) CommitAddTopic(message *m.Message, ack *bool) error {
	fmt.Println("Start Follower")
	fmt.Println("Topic:", message.Topic)

	*ack = true
	topic := &Topic{
		TopicName:        message.Topic,
		ReplicaNum:       uint8(message.ReplicaNum),
		PartitionNum:     uint8(message.Partition),
		Role:             Follower,
	}

	if _, exist := broker.topicList[TopicID(message.ID)]; exist {
		*ack = false
		return fmt.Errorf("Topic ID has already existed")
	}

	if message.Type == m.CREATE_NEW_TOPIC {
		for i := 0; i < int(message.Partition); i++ {
			filepostion := "./disk/broker_" + config.BrokerNodeID + "_" + message.Topic + "_" + strconv.Itoa(i)
			// b.topicList[topic.topicID].Buffer[i] = []byte("")
			IOlib.WriteFile(filepostion, "", false)
		}
	}

	broker.topicList[TopicID(message.ID)] = topic

	return nil
}

// func (b *BrokerServer) AppendToPartition(m *Message, res *bool) error {
// 	topicId := m.Topic
// 	var rec record
// 	copy(rec[:], m.Payload.Marshall())
// 	Broker.topicList[topicId].partition = append(Broker.topicList[topicId].partition, &rec)
// 	*res = true
// 	return nil
// }

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

func (bs *BrokerRPCServer) threePC(serviceMethod string, msg *m.Message, peerAddrs map[NodeID]net.Addr) error {
	// canCommitPhase
	peerTransactionState, err := bs.canCommit(serviceMethod, msg, peerAddrs)

	if err != nil {
		fmt.Println("Can commit Err: ", err)
		switch err.(type) {
		case *TransactionErr:
			e := err.(*TransactionErr)
			switch (e.Err).(type) {
			case *ConnectionErr:
				fmt.Println("attempt to retry by delete peer")

				ce := (e.Err).(*ConnectionErr)
				deleteMsg := m.Message{
					ID:        string(ce.NodeID),
					Text:      ce.Addr.String(),
					Proposer:  string(broker.brokerNodeID),
					Timestamp: time.Now(),
				}
				var ack bool
				if err := bs.DeletePeer(&deleteMsg, &ack); err != nil {
					return fmt.Errorf("delete node failed: %v", err)
				}

				if ack {
				}

			}
		default:
			fmt.Println("default Error")
			return err
		}
		return nil
	}

	// recovery if needed
	if peerTransactionState != nil {
		var recoverPeerAddr map[NodeID]net.Addr

		for k, v := range *peerTransactionState {
			if v == COMMIT {
			}
		}

		if err := bs.recoverPhase(serviceMethod, msg, recoverPeerAddr); err != nil {
			return err
		}
		return nil
	}
	// preCommitPhase
	if err := bs.preCommit(msg, peerAddrs); err != nil {
		switch err.(type) {
		case *TransactionErr:
			e := err.(*TransactionErr)
			switch (e.Err).(type) {
			case *ConnectionErr:
				fmt.Println("attempt to retry by delete peer")
				ce := (e.Err).(*ConnectionErr)

				deleteMsg := m.Message{
					ID:        string(ce.NodeID),
					Text:      ce.Addr.String(),
					Proposer:  string(broker.brokerNodeID),
					Timestamp: time.Now(),
				}

				var ack bool
				if err := bs.DeletePeer(&deleteMsg, &ack); err != nil {
					return fmt.Errorf("delete node failed: %v", err)
				}

				if ack {
				}
			}
		default:
			fmt.Println("default Error")
			return err
		}
		return nil
	}

	// commitPhase
	if err := bs.commit(serviceMethod, msg, peerAddrs); err != nil {
		switch err.(type) {
		case *TransactionErr:
			e := err.(*TransactionErr)
			switch (e.Err).(type) {
			case *ConnectionErr:
				fmt.Println("attempt to retry by delete peer")
				ce := (e.Err).(*ConnectionErr)
				deleteMsg := m.Message{
					ID:        string(ce.NodeID),
					Text:      ce.Addr.String(),
					Proposer:  string(broker.brokerNodeID),
					Timestamp: time.Now(),
				}
				var ack bool
				if err := bs.DeletePeer(&deleteMsg, &ack); err != nil {
					return fmt.Errorf("delete node failed: %v", err)
				}

			
				if ack {
				}
			}
		default:
			fmt.Println("default Error")
			return err
		}
		return nil
	}
	return nil
}

func (bs *BrokerRPCServer) recoverPhase(serviceMethod string, msg *m.Message, peerAddrs map[NodeID]net.Addr) (err error) {
	fmt.Println("Begin Recover")
	errorCh := make(chan error, 1)
	wg := sync.WaitGroup{}


	for k, v := range peerAddrs {
		if k == broker.brokerNodeID {
			continue
		}
		wg.Add(1)
		go func(brokerNodeID NodeID, brokerrPeerAddr net.Addr) {
			defer func() {
				if p := recover(); p != nil {
				}
			}()
			defer wg.Done()
			defer rpcClient.Close()
			if err != nil {
				return
			}
			var ack bool
			if err := RpcCallTimeOut(rpcClient, fmt.Sprintf("ManagerRPCServer.Commit%vRPC", serviceMethod), msg, &ack); err != nil {
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
		return fmt.Errorf("recover aborted: %v", err)
	case <-c:
		fmt.Println("Commit Phase Done")
	}

	if _, exists := peerAddrs[manager.ManagerNodeID]; exists {
		// Local Commit
		var ack bool

		method := reflect.ValueOf(mrpc).MethodByName(fmt.Sprintf("Commit%vRPC", serviceMethod))
		if err := method.Call([]reflect.Value{reflect.ValueOf(msg), reflect.ValueOf(&ack)})[0].Interface(); err != nil {
			return fmt.Errorf("coordinator failed: transaction aborted: %v", err)
		}
	}
	fmt.Println("Done Recover")
	return nil
}

func (bs *BrokerRPCServer) canCommit(serviceMethod string, msg *m.Message, peerAddrs map[NodeID]net.Addr) (*map[NodeID]State, error) {
	// canCommitPhase
	fmt.Println("CanCommitPhase")
	var wg sync.WaitGroup
	errorCh := make(chan error, 1)
	fmt.Println("Break1")
	peerTransactionState := make(map[ManagerNodeID]State)

	for managerID, managerPeer := range peerAddrs {
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


	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()

	select {
	case err := <-errorCh:
		fmt.Println("Abort Transaction")
		return nil, NewTransactionErr(err, "canCommit")
	case <-c:
		fmt.Println("CanCommitPhase Done")
	}

	fmt.Println("Break4")
	// Local canCommit
	var s State
	method := reflect.ValueOf(bs).MethodByName(fmt.Sprintf("CanCommitRPC"))
	if err := method.Call([]reflect.Value{reflect.ValueOf(msg), reflect.ValueOf(&s)})[0].Interface(); err != nil {
		return nil, fmt.Errorf("coordinator failed: transaction aborted: %v", err)
	}
	peerTransactionState[broker.brokerNodeID] = s
	for _, v := range peerTransactionState {
		if v == COMMIT {
			return &peerTransactionState, nil
		}
	}
	fmt.Println("Peer Transaction Map", peerTransactionState)
	return nil, nil
}

func (bs *BrokerRPCServer) preCommit(msg *m.Message, peerAddrs map[NodeID]net.Addr) (err error) {
	// preCommitPhase
	fmt.Println("PreCommit Phase")
	errorCh := make(chan error, 1)
	wg := sync.WaitGroup{}
	for managerID, managerPeer := range peerAddrs {
		wg.Add(1)
		go func() {
			defer func() {
				if p := recover(); p != nil {
					err = NewConnectionErr(managerID,brokerPeerAddr, fmt.Errorf("%v", p))
				}
			}()
			defer wg.Done()
			rpcClient, err := vrpc.RPCDial("tcp", broker.br.String(), logger, loggerOptions)
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
		}()
	}

	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case err := <-errorCh:
		return NewTransactionErr(err, "preCommit")
	case <-c:
		fmt.Println("PreCommit Phase Done")
	}

	return nil
}

func (bs *BrokerRPCServer) commit(serviceMethod string, msg *m.Message, peerAddrs map[NodeID]net.Addr) (err error) {
	errorCh := make(chan error, 1)
	wg := sync.WaitGroup{}
	for managerID, managerPeer := range peerAddrs {
		wg.Add(1)
		go func() {
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
		return NewTransactionErr(err, "commit")
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

func (bs *BrokerRPCServer) DeletePeer(msg *m.Message, ack *bool) error {
	*ack = false

	delPeerID := ManagerNodeID(msg.ID)

	var newPeerAddr  = map[ManagerNodeID]net.Addr{}

	manager.ManagerMutex.Lock()
	for k, v := range manager.ManagerPeers {
		if k == delPeerID {
			continue
		}
		newPeerAddr[k] = v
	}
	manager.ManagerMutex.Unlock()

	if err := bs.threePC("DeletePeer", msg, newPeerAddr); err != nil {
		return err
	}

	*ack = true
	return nil
}




func NewConnectionErr(nodeID NodeID, addr net.Addr, err error) *ConnectionErr {
	return &ConnectionErr{
		Addr:   addr,
		NodeID: nodeID,
		Err:    err,
	}
}

func (e *ConnectionErr) Error() string {
	return fmt.Sprintf("bad connection - %v - %v: %v", e.NodeID, e.Addr.String(), e.Err)
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