package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"

	IOlib "../lib/IOlib"
	m "../lib/message"
	"github.com/DistributedClocks/GoVector/govec/vrpc"
)

type BrokerRPCServer int

type Status int

type consumerId string

type record [512]byte

type partition []*record

type Peer struct {
	addr net.Addr
}

type Topic struct {
	topicID          string
	ReplicaNum       int
	partitionNum     uint8
	Role             Status
	Buffer           map[int][]byte
	FollowerList     []string
	FollowerPeerList []string
}

const (
	Leader Status = iota
	Follower
)

type broker struct {
	topicList map[string]*Topic
}

var b *broker

// Initialize starts the node as a Broker node in the network
func InitBroker(addr string) error {

	b = &broker{
		topicList: make(map[string]*Topic),
	}

	go spawnListener(addr)

	if err := registerBrokerWithManager(); err != nil {
		return err
	}

	fmt.Println("Init Borker")

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

// func handleConnection(conn net.Conn) {
// 	dec := gob.NewDecoder(conn)
// 	message := &m.Message{}
// 	dec.Decode(message)

// 	switch message.Type {

// 	case m.NEW_TOPIC:
// 		if err := startLeader(message); err != nil {

// 		} else {
// 			enc := gob.NewEncoder(conn)
// 			if err = enc.Encode(message); err != nil {
// 				log.Fatal("encode error:", err)
// 			}
// 		}

// 	default:
// 		// freebsd, openbsd,
// 		// plan9, windows...
// 	}

// }

// func startBroker(message *m.Message) error {

// 	topic := &Topic{
// 		topicID:        message.Topic,
// 		partitionIdx:   message.Partition,
// 		partition:      partition{},
// 		consumerOffset: make(map[consumerId]uint),
// 		Status:         Leader,
// 		FollowerList:   make(map[net.Addr]bool),
// 	}

// 	if _, exist := b.topicList[topic.topicID]; exist {
// 		return fmt.Errorf("Topic ID has already existed")
// 	}

// 	followersIP := strings.Split(message.Text, ",")

// 	followerMessage := &m.Message{Topic: message.Topic, Partition: message.Partition}

// 	for _, ip := range followersIP {
// 		go broadcastToFollowers(m, ip)
// 	}

// 	b.topicList[topic.topicID] = topic

// 	// b.topicList = append(b.topicList, topic)

// 	fmt.Println("Started Leader")

// 	return nil
// }

// func startLeader(message *m.Message) error {
// 	topic := &Topic{
// 		topicID:        message.Topic,
// 		partitionIdx:   message.Partition,
// 		partition:      partition{},
// 		consumerOffset: make(map[consumerId]uint),
// 		Status:         Leader,
// 		FollowerList:   make(map[net.Addr]bool),
// 	}
// 	return nil

// }

// func broadcastToFollowers(message *m.Message, addr string) error {

// 	destAddr, err := net.ResolveTCPAddr("tcp", addr)
// 	if err != nil {
// 		return err
// 	}

// 	lAddr, err := net.ResolveTCPAddr("tcp", config.BrokerIPPort)
// 	if err != nil {
// 		return err
// 	}

// 	preparePhase(message, destAddr)

// 	conn, err := net.DialTCP("tcp", lAddr, destAddr)

// 	if err != nil {
// 		return err
// 	}

// 	enc := gob.NewEncoder(conn)

// 	if err := enc.Encode(message); err != nil {
// 		return err
// 	}

// 	dec := gob.NewDecoder(conn)

// 	revMessage := &m.Message{}

// 	if err := dec.Decode(revMessage); err != nil{
// 		return err
// 	}

// 	return nil
// }

func (bs *BrokerRPCServer) StartLeader(message *m.Message, ack *bool) error {
	*ack = true
	fmt.Println("\nLeader ID", config.BrokerNodeID)
	fmt.Println("Leader Topic Name", message.Topic)

	topic := &Topic{
		topicID:      message.Topic,
		ReplicaNum:   message.ReplicaNum,
		partitionNum: message.Partition,
		Role:         Leader,
		FollowerList: message.IPs,
	}

	fmt.Printf("topic: %+v\n", topic)

	if _, exist := b.topicList[topic.topicID]; exist {
		return fmt.Errorf("Topic ID has already existed")
	}

	if message.Type == m.CREATE_NEW_TOPIC {
		for i := 0; i < int(message.Partition); i++ {
			filepostion := "./disk/broker_" + config.BrokerNodeID + "_" + message.Topic + "_" + strconv.Itoa(i)
			// b.topicList[topic.topicID].Buffer[i] = []byte("")
			IOlib.WriteFile(filepostion, "", false)
		}
	}

	followersIP := message.IPs

	followerMessage := &m.Message{
		Topic:      message.Topic,
		Type:       message.Type,
		Partition:  message.Partition,
		ReplicaNum: message.ReplicaNum,
		IPs:        message.IPs,
	}

	fmt.Printf("followerMessage: %+v\n", followerMessage)

	var waitGroup sync.WaitGroup

	waitGroup.Add(len(followersIP))

	fmt.Println("follower len", len(followersIP))

	for _, ip := range followersIP {
		fmt.Println("Looping", ip)
		go broadcastToFollowers(*followerMessage, ip, &waitGroup)
	}
	waitGroup.Wait()

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

func (bs *BrokerRPCServer) Ping(message *m.Message, ack *bool) error {

	fmt.Println("I've been pinged by: ", message.Text)
	for {
	}
	*ack = true
	return nil
}

func (bs *BrokerRPCServer) StartFollower(message *m.Message, ack *bool) error {
	fmt.Println("Start Follower")
	fmt.Println("Topic:", message.Topic)

	*ack = true

	topic := &Topic{
		topicID:          message.Topic,
		ReplicaNum:       message.ReplicaNum,
		partitionNum:     message.Partition,
		Role:             Leader,
		FollowerPeerList: message.IPs,
	}

	if _, exist := b.topicList[topic.topicID]; exist {
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

	return nil
}

// func (b *BrokerServer) InitNewTopic(m *Message, res *bool) error {
// 	topic := new(Topic)
// 	topic.id = m.Topic

// 	Broker.topicList[topic.id] = topic

// 	*res = true
// 	return nil
// }

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
