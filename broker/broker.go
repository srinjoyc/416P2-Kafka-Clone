package broker

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
)

type Packet interface{
	Unmarshall() []byte
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
	id string
	partition
	consumerOffset map[consumerId]uint
	Status
	FollowerList []net.Addr
}

type Message struct {
	Topic string
	Payload Packet
}

const (
	Manager Status = iota
	Follower
)

type broker struct {
	topicList map[string]*Topic
}

var Broker *broker

// Initialize starts the node as a Broker node in the network
func Initialize(addr string) error {

	Broker = &broker{
		topicList: make(map[string]*Topic),
	}
	spawnListener(addr)
	fmt.Println("Inited Node as a Leader Node.")

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

	fmt.Printf("Serving RPC Server at: %v\n", tcpAddr.String())

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go rpc.ServeConn(conn)
	}
}

func (b *BrokerServer) StartLeader(m *Message, res *bool){
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
	copy(rec[:], m.Payload.Unmarshall())
	Broker.topicList[topicId].partition = append(Broker.topicList[topicId].partition, &rec)
	*res = true
	return nil
}

func (b *BrokerServer) AddClient(m *Message, res *bool) error {
	topicId := m.Topic
	var rec record
	copy(rec[:], m.Payload.Unmarshall())
	Broker.topicList[topicId].partition = append(Broker.topicList[topicId].partition, &rec)
	*res = true
	return nil
}

func (b *BrokerServer) DispatchData(m *Message, res *bool) error{
	topicID := m.Topic
	clientId := m.Payload.Unmarshall()

	Broker.topicList[topicID].consumerOffset[consumerId(clientId)] = 0
	return nil
}

func (b *BrokerServer) AddFollowerToTopic(m *Message, res *bool) error{
	topicID := m.Topic
	followerAddr := m.Payload.Unmarshall()

	tcpAddr, err := net.ResolveTCPAddr("tcp", string(followerAddr))
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
	}

	Broker.topicList[topicID].FollowerList = append(Broker.topicList[topicID].FollowerList, tcpAddr)
	return nil
}

func broadcastToFollowers(stub interface{}) error{
	return nil
}

