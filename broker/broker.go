package main

import (
	"fmt"
	"net"
	"net/rpc"
	"os"

	"../lib/message"
)

type Packet interface {
	Marshall() []byte
}

type Status int

type BrokerServer int

type consumerId string

type record [512]byte

type partition []*record

type Peer struct {
	addr net.Addr
}

type Topic struct {
	topicID        string
	partitionIdx   uint8
	partition      partition
	consumerOffset map[consumerId]uint
	Status
	FollowerList map[net.Addr]bool
}

// type Message struct {
// 	Topic   string
// 	ID      string
// 	PartitionIdx uint8
// 	Payload Packet
// }

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
	spawnListener(addr)
	fmt.Println("Init Borker")
	return nil
}

func spawnListener(addr string) {
	bServer := new(BrokerServer)
	rpc.Register(bServer)

	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
	}

	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
	}

	fmt.Printf("Serving Server at: %v\n", tcpAddr.String())

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}

		// message, _ := bufio.NewReader(conn).ReadString('\n')
		// fmt.Println(string(message))
		rpc.ServeConn(conn)
	}
}

func (b *BrokerServer) StartLeader(m *message.Message, ack *bool) error {

	topic := Topic{
		topicID:        m.ID,
		partitionIdx:   m.PartitionIdx,
		partition:      partition{},
		consumerOffset: make(map[consumerId]uint),
		Status:         Leader,
		FollowerList:   make(map[net.Addr]bool),
	}

	broker.topicList = append(broker.topicList, topic)

	fmt.Println("Starting Leader")
	*ack = true
	return nil
}

func (b *BrokerServer) InitNewTopic(m *Message, res *bool) error {
	topic := new(Topic)
	topic.id = m.Topic

	Broker.topicList[topic.id] = topic

	*res = true
	return nil
}

func (b *BrokerServer) AppendToPartition(m *Message, res *bool) error {
	topicId := m.Topic
	var rec record
	copy(rec[:], m.Payload.Marshall())
	Broker.topicList[topicId].partition = append(Broker.topicList[topicId].partition, &rec)
	*res = true
	return nil
}

func (b *BrokerServer) AddClient(m *Message, res *bool) error {
	topicId := m.Topic
	var rec record
	copy(rec[:], m.Payload.Marshall())
	Broker.topicList[topicId].partition = append(Broker.topicList[topicId].partition, &rec)
	*res = true
	return nil
}

func (b *BrokerServer) DispatchData(m *Message, res *bool) error {
	topicID := m.Topic
	clientId := m.Payload.Marshall()

	Broker.topicList[topicID].consumerOffset[consumerId(clientId)] = 0
	return nil
}

func (b *BrokerServer) AddFollowerToTopic(m *Message, res *bool) error {
	topicID := m.Topic
	followerAddr := m.Payload.Marshall()

	tcpAddr, err := net.ResolveTCPAddr("tcp", string(followerAddr))
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
	}

	Broker.topicList[topicID].FollowerList = append(Broker.topicList[topicID].FollowerList, tcpAddr)
	return nil
}

func broadcastToFollowers(stub interface{}) error {
	return nil
}
