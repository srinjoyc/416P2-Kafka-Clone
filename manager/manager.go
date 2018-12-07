package main

import (
	"bufio"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	"../lib/IOlib"
	"../lib/message"
)

type configSetting struct {
	ManagerNodeID         string
	ProviderIPPort        string
	BrokerIPPort          string
	NeighborManagerIPPort string
	PeerManagerNodeIP     []string
}

var config configSetting

var brokersList struct {
	list  []string
	mutex sync.Mutex
}

// manager keeps track of all free nodes (i.e. nodes that are ready to
// be 'provisioned'.  If this manager goes down, it will need to ensure that the other managers
// have the correct copy of this list.
var freeNodesList struct {
	list  []string
	mutex sync.Mutex
}

/* readConfigJSON
 * Desc:
 *		read the configration from file into struct config
 *
 * @para configFile: relative url of configuration file
 * @retrun: None
 */
func readConfigJSON(configFile string) {
	jsonFile, err := os.Open(configFile)
	defer jsonFile.Close()
	if err != nil {
		fmt.Println(err) // if we os.Open returns an error then handle it
	}
	json.Unmarshal([]byte(IOlib.ReadFileByte(configFile)), &config)
}

/* listenProvider
 * Desc:
 * 		this is a goroutine dealing with requests from provider routine
 *
 * @para IPPort string:
 *		the ip and port opened for messages from provider routine
 */
func listenProvider() {
	listener, err := net.Listen("tcp4", config.ProviderIPPort)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer listener.Close()

	fmt.Println("Listening provider at :" + config.ProviderIPPort)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go handleProviderMessage(conn)
	}
}

/* dealProvider
 * @para conn:
 *		the ip and port opened for messages from provider routine
 *
 * Desc:
 *
 */
func handleProviderMessage(conn net.Conn) {
	// decode the serialized message from the connection
	defer conn.Close()
	dec := gob.NewDecoder(conn)

	incoming := &message.Message{}
	dec.Decode(incoming) // decode the infomation into initialized message

	// if-else branch to deal with different types of messages
	if incoming.Type == "Text" {
		fmt.Printf("Receive Provider Msg: {pID:%s, type:%s, partition:%s, text:%s}\n", incoming.ID, incoming.Type, incoming.Partition, incoming.Text)

		// code about append text

	} else if incoming.Type == "CreateTopic" {
		fmt.Printf("Receive Provider Msg: {pID:%s, type:%s, topic:%s}\n", incoming.ID, incoming.Type, incoming.Topic)

		// code about topic

	}

	// write the success response
	enc := gob.NewEncoder(conn)
	err := enc.Encode(message.Message{config.ManagerNodeID, "response", "succeed", "", ""})
	if err != nil {
		log.Fatal("encode error:", err)
	}
}

func listenBroker() {
	listener, err := net.Listen("tcp4", config.BrokerIPPort)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer listener.Close()

	fmt.Println("Listening broker at :" + config.BrokerIPPort)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go handleBrokerMessage(conn)
	}
}

func handleBrokerMessage(conn net.Conn) {
	defer conn.Close()
	// decode the serialized message from the connection
	dec := gob.NewDecoder(conn)
	incoming := &message.Message{}
	dec.Decode(incoming) // decode the infomation into initialized message

	// if-else branch to deal with different types of messages
	if incoming.Type == "New Broker" {
		fmt.Printf("Receive Broker Msg: {pID:%s, type:%s, partition:%s, text:%s}\n", incoming.ID, incoming.Type, incoming.Partition, incoming.Text)

		// code about append the broker
		brokersList.mutex.Lock()
		flag := false
		for _, ip := range brokersList.list {
			if ip == incoming.Text {
				flag = true
				break
			}
		}
		if flag == false {
			brokersList.list = append(brokersList.list, incoming.Text)
		}
		brokersList.mutex.Unlock()
	}

	// write the success response
	enc := gob.NewEncoder(conn)
	err := enc.Encode(message.Message{config.ManagerNodeID, "response", "succeed", "", ""})
	if err != nil {
		log.Fatal("encode error:", err)
	}
}

// Initialize starts the node as a Manager node in the network
func Initialize() bool {
	configFilename := os.Args[1]
	readConfigJSON(configFilename)

	println("Manager", config.ManagerNodeID, "starts")

	go listenProvider()
	go listenBroker()

	return true
}

func main() {
	Initialize()

	// terminal controller like shell
	reader := bufio.NewReader(os.Stdin)
	for {
		cmd, _ := reader.ReadString('\n')
		if cmd == "broker\n" {
			fmt.Println(brokersList.list)
		}
	}
}
