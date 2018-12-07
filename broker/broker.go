package main

import (
	"bufio"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"

	"../IOlib"
)

type configSetting struct {
	BrokerNodeID   string
	ManagerIPPort  string
	ConsumerIPPort string
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
	fmt.Printf("Response : {kID:%s, status:%s}\n", response.ID, response.Text)

	return nil
}

func informManager() {
	message := Message{config.BrokerNodeID, "New Broker", config.ManagerIPPort, "", ""}
	for i := 0; i < len(config.ManagerIPs); i++ {
		provideMsg(config.ManagerIPs[i], message)
	}
}

// Initialize starts the node as a broker node in the network
func Initialize() bool {
	configFilename := os.Args[1]
	readConfigJSON(configFilename)

	informManager() // when a new broker starts, it will inform the manager nodes

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
