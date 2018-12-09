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
	"time"

	m"../lib/message"

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
	State
}

type State uint

const(
	READY State = iota
	WAIT
	PREPARE
	COMMIT
	ABORT
)


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
	PeerManagerNodeIP string
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
		State: READY,
	}

	logger = govec.InitGoVector(string(manager.ManagerNodeID), fmt.Sprintf("%v-logfile", manager.ManagerNodeID), govec.GetDefaultConfig())
	loggerOptions = govec.GetDefaultLogOptions()
	
	if len(config.PeerManagerNodeIP) != 0{
		if err := manager.registerPeerRequest(config.PeerManagerNodeIP); err!= nil{
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
		ID: string(mn.ManagerNodeID),
		Text: mn.ManagerIP.String(),
	}

	fmt.Println("Ready to invoke service")
	if err := rpcClient.Call("ManagerRPCServer.RegisterPeer", newMsg, &peerList); err != nil {
		return err
	}

	manager.ManagerMutex.Lock()
	for k, v := range peerList{
		// Skip registering own IP to the Peer Map
		if ManagerNodeID(k) == manager.ManagerNodeID{
			continue
		}

		tcpAddr, err := net.ResolveTCPAddr("tcp", v)
		if err!= nil{
			continue
		}
		manager.ManagerPeers[ManagerNodeID(k)] = tcpAddr
	}
	manager.ManagerMutex.Unlock()

	fmt.Println("Done RegisterPeerRequest")

	fmt.Println("PrintPeerMap")
	fmt.Println(manager.ManagerPeers)


	// var ack bool
	// managerTCPAddr, ok := manager.ManagerIP.(*net.TCPAddr)
	// if !ok {
	// 	return fmt.Errorf("Cannot type-assert net.Conn interface to TCPAddr Pointer")
	// }
	// 2-phase Commit
	// if err := rpcClient.Call("ManagerRPCServer.AddPeerCommit", managerTCPAddr, &res); err != nil {
	// 	return err
	// }

	// fmt.Println("Manager Node ID", res)
	// manager.ManagerMutex.Lock()
	// manager.ManagerPeers[res] = rAddr
	// manager.ManagerMutex.Unlock()
	// fmt.Printf("Added Peer request: %v - %v\n", res, rAddr.String())
	return nil
}

func (mrpc *ManagerRPCServer) RegisterPeer(msg *m.Message, peerList *map[string]string) (err error) {
	defer func(){
		if p:= recover(); p != nil{
			manager.State = ABORT
			err = fmt.Errorf("transaction aborted: %v", p)
		}
	}()

	manager.ManagerMutex.Lock()
	// canCommitPhase
	var wg sync.WaitGroup
	fmt.Println("Can Commit Phase")

	errorCh := make(chan error, 1)

	for _, managerPeer := range manager.ManagerPeers{
		wg.Add(1)
		go manager.CanCommitRegisterPeer(managerPeer, msg, &wg, errorCh)
	}

	manager.State = WAIT

	c := make(chan struct{})
    go func() {
        defer close(c)
        wg.Wait()
    }()

	select{
	case err := <- errorCh:
		manager.State = ABORT
		fmt.Println("Abort Transaction")
		return fmt.Errorf("transaction aborted: %v", err)
	case <- c:
		fmt.Println("All Tasks Completed")
	}

	fmt.Println("Did I make it through?")
	var localAck = false
	manager.ManagerMutex.Unlock()
	if err := mrpc.CanCommitRegisterPeerRPC(msg, &localAck); err != nil {
		manager.State = ABORT
		return fmt.Errorf("coordinator failed: transaction aborted: %v", err)
	}
	if !localAck{
		manager.State = ABORT
		return fmt.Errorf("transaction aborted: peerID exists in coordinator")
	}

	// preCommitPhase
	fmt.Println("PreCommit Phase")
	errorCh = make(chan error, 1)
	var wg1 sync.WaitGroup

	for _, managerPeer := range manager.ManagerPeers{
		wg1.Add(1)
		go manager.PreCommitRegisterPeer(managerPeer, msg,&wg1, errorCh)
	}

	fmt.Println("Sleeping...")
	time.Sleep(time.Duration(5) * time.Second)

	select{
	case err := <- errorCh:
		manager.State = ABORT
		return fmt.Errorf("transaction aborted: %v", err)
	default:
		wg1.Wait()
	}

	// commitPhase
	fmt.Println("Commit Phase")
	errorCh = make(chan error, 1)
	var wg2 sync.WaitGroup

	for _, managerPeer := range manager.ManagerPeers{
		wg2.Add(1)
		go manager.CommitRegisterPeer(managerPeer, msg, &wg2, errorCh)
	}

	select{
	case err := <- errorCh:
		manager.State = ABORT
		return fmt.Errorf("transaction aborted: %v", err)
	default:
		wg2.Wait()
	}
	fmt.Println("Local Commit")

	var ack bool
	mrpc.CommitRegisterPeerRPC(msg, &ack)

	manager.ManagerMutex.Lock()
	for k, v := range manager.ManagerPeers{
		(*peerList)[string(k)] = v.String()
	}
	manager.ManagerMutex.Unlock()
	(*peerList)[string(manager.ManagerNodeID)] = manager.ManagerIP.String()

	return nil
}

func (mn *ManagerNode) CommitRegisterPeer(managerPeerAddr net.Addr, msg *m.Message, wg *sync.WaitGroup ,errorCh chan error){
	defer wg.Done()
	rpcClient, err := vrpc.RPCDial("tcp", managerPeerAddr.String(), logger, loggerOptions)
	defer rpcClient.Close()
	if err!=nil{
		errorCh <- err
		return
	}
	var ack bool
	if err := RpcCallTimeOut(rpcClient, "ManagerRPCServer.CommitRegisterPeerRPC", msg, &ack); err != nil{
		errorCh <- err
		return
	}
	if !ack{
		errorCh <- fmt.Errorf("peer disagrees")
	}	
}

func (mrpc *ManagerRPCServer) CommitRegisterPeerRPC(msg *m.Message, ack *bool) error{
	*ack = false
	peerManagerID := ManagerNodeID(msg.ID)
	
	peerManagerAddr, err := net.ResolveTCPAddr("tcp",msg.Text)

	fmt.Println(peerManagerAddr, peerManagerID)

	if err != nil{
		return err
	}

	manager.ManagerMutex.Lock()
	manager.ManagerPeers[peerManagerID] = peerManagerAddr
	manager.ManagerMutex.Unlock()

	fmt.Printf("added peer - %v - %v\n", peerManagerID, peerManagerAddr)
	fmt.Println("Peer Map: ", manager.ManagerPeers)

	manager.State = COMMIT
	*ack = true
	return nil
}

func (mn *ManagerNode) PreCommitRegisterPeer(managerPeerAddr net.Addr, msg *m.Message, wg *sync.WaitGroup ,errorCh chan error){
	defer wg.Done()
	rpcClient, err := vrpc.RPCDial("tcp", managerPeerAddr.String(), logger, loggerOptions)
	defer rpcClient.Close()
	if err!=nil{
		errorCh <- err
		return
	}
	var ack bool
	if err := RpcCallTimeOut(rpcClient, "ManagerRPCServer.PreCommitRegisterPeerRPC", msg, &ack); err != nil{
		errorCh <- err
		return
	}
	if !ack{
		errorCh <- fmt.Errorf("peer disagrees")
	}	
}

func (mrpc *ManagerRPCServer) PreCommitRegisterPeerRPC(msg *m.Message, ack *bool) error{
	*ack = false
	manager.State = PREPARE
	*ack = true
	return nil
}

func (mn *ManagerNode) CanCommitRegisterPeer(managerPeerAddr net.Addr, msg *m.Message, wg *sync.WaitGroup ,errorCh chan error){
	defer func(){
		if p:= recover(); p != nil{
			errorCh <- fmt.Errorf("bad connection - %v: %v", managerPeerAddr, p)
		}
	}()
	defer wg.Done()

	fmt.Println(managerPeerAddr.String())

	rpcClient, err := vrpc.RPCDial("tcp", managerPeerAddr.String(), logger, loggerOptions)
	defer rpcClient.Close()
	if err!=nil{
		errorCh <- fmt.Errorf("manager peer - %v: %v",managerPeerAddr,err)
		return
	}
	var ack bool
	if err := RpcCallTimeOut(rpcClient, "ManagerRPCServer.CanCommitRegisterPeerRPC", msg, &ack); err != nil{
		errorCh <- fmt.Errorf("manager peer - %v: %v",managerPeerAddr,err)
		return
	}
	if !ack{
		errorCh <- fmt.Errorf("peer - %v disagrees", managerPeerAddr)
	}	
}

func (mrpc *ManagerRPCServer) CanCommitRegisterPeerRPC(msg *m.Message, ack *bool) error{
	*ack = false
	peerManagerID := ManagerNodeID(msg.ID)

	manager.ManagerMutex.Lock()
	defer manager.ManagerMutex.Unlock()

	if _, exist := manager.ManagerPeers[peerManagerID]; exist {
		fmt.Println("Manager", peerManagerID)
		manager.State = ABORT
		return nil
	}


	*ack = true
	manager.State = READY
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
	defer rpcClient.Close()
	if err != nil {
		return err
	}

	if err := RpcCallTimeOut(rpcClient, "BrokerRPCServer.Ping", newMsg, &ack); err != nil{
		return err
	}
	// if err := manager.addBroker(BrokerID(m.ID), rAddr); err != nil {
	// 	return err
	// }
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

// func (mrpc *ManagerRPCServer) RegisterBroker(m *message.Message, ack *bool) error {

// }

func main() {

	if len(os.Args) != 2 {
		fmt.Println("Please provide config filename. e.g. m1.json, m2.json")
		return
	}
	configFileName := os.Args[1]

	if err:=Initialize(configFileName);err!=nil{
		fmt.Println(err)
		os.Exit(1)
	}

}

func RpcCallTimeOut(rpcClient *rpc.Client,serviceMethod string, args interface{}, reply interface{}) error{
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
