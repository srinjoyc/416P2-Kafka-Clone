package main

import (
	"bufio"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"

	"./follower"
	"./leader"
)

type configSetting struct {
	NodeID             string
	ProviderIPPort     string
	ConsumerIPPort     string
	InnerRPCIPPort     string
	PeerKafkaNodeAddrs []string
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

/* readFileByte
 * Desc:
 *		read and then return the byte of Content from file in corresponding path
 * @para: filePath: relative url of file
 * @Return: []byte
 */
func readFileByte(filePath string) []byte {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Fatal(err)
	}
	return data
}

/* readConfigJSON
 * Desc:
 *		read the configration from file into struct config
 * @para configFile: relative url of file of configuration
 * @retrun: None
 */
func readConfigJSON(configFile string) {
	jsonFile, err := os.Open(configFile)
	if err != nil {
		fmt.Println(err) // if we os.Open returns an error then handle it
	}
	json.Unmarshal([]byte(readFileByte(configFile)), &config)
	defer jsonFile.Close()
}

/* listenProvider
 * Desc:
 * 		this is a goroutine dealing with requests from provider routine
 * @para IPPort string:
 *		the ip and port opened for messages from provider routine
 */
func listenProvider(IPPort string) {
	listener, err := net.Listen("tcp4", IPPort)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer listener.Close()

	fmt.Println("Listening provider at : " + IPPort)

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
 * Desc:
 * 		terminal controller like shell
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
	err := enc.Encode(Message{config.NodeID, "response", "succeed", "", ""})
	if err != nil {
		log.Fatal("encode error:", err)
	}
	conn.Close()
}

func initialize() {
	leader.Initialize()
	follower.Initialize()
}

/* commender
 * Desc:
 * 		terminal controller like shell
 */
func commender() {
	reader := bufio.NewReader(os.Stdin)
	for {
		text, _ := reader.ReadString('\n')
		println(text)
	}
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Please provide config filename. e.g. k1.json, k2.json")
		return
	}
	configFilename := os.Args[1]
	readConfigJSON(configFilename)

	// initialize()
	go listenProvider(config.ProviderIPPort)

	commender()
}
