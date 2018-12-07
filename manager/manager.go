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

	"../IOlib"
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

// Message is used for communication among nodes
type Message struct {
	ID        string
	Type      string
	Text      string
	Topic     string
	Partition string
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
	if err != nil {
		fmt.Println(err) // if we os.Open returns an error then handle it
	}
	json.Unmarshal([]byte(IOlib.ReadFileByte(configFile)), &config)
	defer jsonFile.Close()
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
			return
		}
		go dealProvider(conn)
	}
}

/* dealProvider
 * @para conn:
 *		the ip and port opened for messages from provider routine
 *
 * Desc:
 *
 */
func dealProvider(conn net.Conn) {
	// decode the serialized message from the connection
	dec := gob.NewDecoder(conn)
	message := &Message{}
	dec.Decode(message) // decode the infomation into initialized message

	// if-else branch to deal with different types of messages
	if message.Type == "Text" {
		fmt.Printf("Receive Provider Msg: {pID:%s, type:%s, partition:%s, text:%s}\n", message.ID, message.Type, message.Partition, message.Text)

		// code about append text

	} else if message.Type == "CreateTopic" {
		fmt.Printf("Receive Provider Msg: {pID:%s, type:%s, topic:%s}\n", message.ID, message.Type, message.Topic)

		// code about topic

	}

	// write the success response
	enc := gob.NewEncoder(conn)
	err := enc.Encode(Message{config.ManagerNodeID, "response", "succeed", "", ""})
	if err != nil {
		log.Fatal("encode error:", err)
	}
	conn.Close()
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
			return
		}
		go dealBroker(conn)
	}
}

func dealBroker(conn net.Conn) {
	// decode the serialized message from the connection
	dec := gob.NewDecoder(conn)
	message := &Message{}
	dec.Decode(message) // decode the infomation into initialized message

	// if-else branch to deal with different types of messages
	if message.Type == "New Broker" {
		fmt.Printf("Receive Broker Msg: {pID:%s, type:%s, partition:%s, text:%s}\n", message.ID, message.Type, message.Partition, message.Text)

		// code about append the broker
		brokersList.mutex.Lock()
		flag := false
		for _, ip := range brokersList.list {
			if ip == message.Text {
				flag = true
				break
			}
		}
		if flag == false {
			brokersList.list = append(brokersList.list, message.Text)
		}
		brokersList.mutex.Unlock()

	}

	// write the success response
	enc := gob.NewEncoder(conn)
	err := enc.Encode(Message{config.ManagerNodeID, "response", "succeed", "", ""})
	if err != nil {
		log.Fatal("encode error:", err)
	}
	conn.Close()
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
