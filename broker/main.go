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

	"../lib/message"
)

type configSetting struct {
	BrokerNodeID   string
	BrokerIPPort   string
	ManagerIPPort  string
	ConsumerIPPort string
	ManagerIPs     []string
}

type s string

// Message is used for communication among nodes
// type Message struct {
// 	ID        string
// 	Type      string
// 	Text      string
// 	Topic     string
// 	Partition string
// }

var config configSetting

/* readConfigJSON
 * Desc:
 *		read the configration from file into struct config
 *
 * @para configFile: relative url of file of configuration
 * @retrun: None
 */
func readConfigJSON(configFile string) error {
	configByte, err := ioutil.ReadFile(configFile)

	if err != nil {
		fmt.Println(err)
	}

	if err := json.Unmarshal(configByte, &config); err != nil {
		return err
	}

	return nil
}

/* provideMsg
 * para message string
 *
 * Desc:
 * 		send the message to kafka node by remoteIPPort
 */
func provideMsg(remoteIPPort string, message Message) error {
	conn, err := net.Dial("tcp", remoteIPPort)
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
	fmt.Printf("Response : {kID:%s, status:%s}\n", response.ID, response.Payload.Marshall())

	return nil
}

func informManager() {
	m := message.Message{config.BrokerNodeID, "New Broker", s(config.ManagerIPPort)}
	for i := 0; i < len(config.ManagerIPs); i++ {
		provideMsg(config.ManagerIPs[i], message)
	}
}

// Initialize starts the node as a broker node in the network
func Initialize() error {
	configFilename := os.Args[1]
	if err := readConfigJSON(configFilename); err != nil {
		return err
	}

	fmt.Println(config.BrokerIPPort)
	informManager() // when a new broker starts, it will inform the manager nodes

	return nil
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Please provide config filename. e.g. b1.json, b2.json")
		return
	}

	err := Initialize()
	checkError(err)

	err = InitBroker(config.BrokerIPPort)
	checkError(err)

	// terminal controller like shell
	reader := bufio.NewReader(os.Stdin)

	for {
		text, _ := reader.ReadString('\n')
		println(text)
	}
}

func (s s) Marshall() []byte {
	return []byte(string(s))
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
