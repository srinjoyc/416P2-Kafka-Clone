package main

import (
	"bufio"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"

	"../lib/IOlib"
	"../lib/message"
)

type configSetting struct {
	BrokerNodeID   string
	BrokerIP       string
	ConsumerIPPort string
	FollowerIPs    []string
	ManagerIPs     []string
}

// Message is used for communication among nodes
type Message struct {
	ID        string
	Type      string
	Text      string
	Topic     string
	Partition string
}

var config configSetting

/* readConfigJSON
 * Desc:
 *		read the configration from file into struct config
 *
 * @para configFile: relative url of file of configuration
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

/* provideMsg
 * para message string
 *
 * Desc:
 * 		send the message to kafka node by remoteIPPort
 */
func provideMsg(remoteIPPort string, message Message) error {
	tcplAddr, err := net.ResolveTCPAddr("tcp", config.BrokerIP)
	tcprAddr, err := net.ResolveTCPAddr("tcp", remoteIPPort)
	if err != nil {
		println("Fail to connect kafka manager" + remoteIPPort)
		return err
	}
	conn, err := net.DialTCP("tcp", tcplAddr, tcprAddr)
	if err != nil {
		println("Fail to connect kafka manager" + remoteIPPort)
		return err
	}
	defer conn.Close()

	// send message
	enc := gob.NewEncoder(conn)
	err = enc.Encode(message)
	if err != nil {
		log.Fatal("encode error:", err)
	}

	// response
	dec := gob.NewDecoder(conn)
	response := &Message{}
	dec.Decode(response)
	fmt.Printf("Response : {kID:%s, status:%s}\n", response.ID, response.Text)

	return nil
}

func informManager() {
	message := Message{config.BrokerNodeID, "NewBroker", config.BrokerIP, "", ""}
	for i := 0; i < len(config.ManagerIPs); i++ {
		provideMsg(config.ManagerIPs[i], message)
	}
}

func listenForMessages() {
	listener, err := net.Listen("tcp4", config.BrokerIP)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer listener.Close()

	fmt.Println("Listening to other managers at :" + config.BrokerIP)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		go processMessage(conn)
	}
}
func processMessage(conn net.Conn) {
	// decode the serialized message from the connection
	dec := gob.NewDecoder(conn)
	msg := &message.Message{}
	dec.Decode(msg) // decode the infomation into initialized message

	if msg.Type == message.INFO {
		fmt.Printf("Receive info Msg: {pID:%s, type:%s, partition:%s, text:%s}\n", msg.ID, msg.Type, msg.Text)
		// code about append text
	}
}

// Initialize starts the node as a broker node in the network
func Initialize() bool {
	configFilename := os.Args[1]
	readConfigJSON(configFilename)

	informManager() // when a new broker starts, it will inform the manager nodes
	go listenForMessages()
	return true
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Please provide config filename. e.g. b1.json, b2.json")
		return
	}

	Initialize()

	// terminal controller like shell
	reader := bufio.NewReader(os.Stdin)
	for {
		text, _ := reader.ReadString('\n')
		println(text)
	}
}
