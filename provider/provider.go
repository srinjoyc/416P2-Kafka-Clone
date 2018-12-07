package main

import (
	"bufio"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"../lib/IOlib"
	"../lib/message"
)

type configSetting struct {
	ProviderID          string
	KafkaManagerIPPorts []string
}

var config configSetting

/* readConfigJSON
 * Desc:
 *		read the configration from file into struct config
 * @para configFile: relative url of file of configuration
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

/* provideMsg
 * para message string
 *
 * Desc:
 * 		send the message to kafka node by remoteIPPort
 */
func provideMsg(remoteIPPort string, outgoing message.Message) error {
	conn, err := net.Dial("tcp", remoteIPPort)
	if err != nil {
		println("Fail to connect kafka manager" + remoteIPPort)
		return err
	}
	defer conn.Close()

	// send message
	enc := gob.NewEncoder(conn)
	err = enc.Encode(outgoing)
	if err != nil {
		log.Fatal("encode error:", err)
	}

	// response
	dec := gob.NewDecoder(conn)
	response := &message.Message{}
	dec.Decode(response)
	fmt.Printf("Response : {kID:%s, status:%s}\n", response.ID, response.Text)

	return nil
}

/* provideMsgToKafka
 * para message string
 * 		the message string needs sending
 * Desc:
 * 		send the message to connected kafka nodes
 */
func provideMsgToKafka(topic string, partition string, msg string) {
	message := message.Message{config.ProviderID, "Text", msg, topic, partition}
	for i := 0; i < len(config.KafkaManagerIPPorts); i++ {
		if nil == provideMsg(config.KafkaManagerIPPorts[i], message) {
			break
		}
		// if one manager is down, connect another one
		// tries to connect to all managers, stopping at the first successful connection
	}
}

func createTopicInKafka(topicName string) {
	message := message.Message{config.ProviderID, "CreateTopic", "", topicName, ""}
	for i := 0; i < len(config.KafkaManagerIPPorts); i++ {
		if nil == provideMsg(config.KafkaManagerIPPorts[i], message) {
			break
		}
		// if one manager is down, connect another one
		// tries to connect to all managers, stopping at the first successful connection
	}
}

func shell() {
	// terminal controller like shell
	println("*******************")
	println("* Instuction: there are several kinds of operations\n* msg: upload the message into connected kafka nodes\n* file: upload the file to connected nodes")
	println("*******************")
	reader := bufio.NewReader(os.Stdin)
	for {
		print("cmd: ")
		cmd, _ := reader.ReadString('\n')
		if cmd == "msg\n" { // input send a messge
			var message, partition, topic string

			// read data from stdin
			print("Input topic: ")
			fmt.Scanln(&topic)
			print("Input partition: ")
			fmt.Scanln(&partition)
			print("Input message: ")
			message, _ = reader.ReadString('\n')
			message = message[:len(message)-1]

			// provide message
			provideMsgToKafka(topic, partition, message)
		} else if cmd == "file\n" { // input the filename of a set of messages, the messages are divided by '\n'
			var topic, partition string

			// read data from stdin
			print("Input topic: ")
			fmt.Scanln(&topic)
			print("Input partition list such as 1,2,3: ")
			fmt.Scanln(&partition)
			print("Input file name:")
			filename, _ := reader.ReadString('\n')
			data := strings.Split(string(IOlib.ReadFileByte(filename[:len(filename)-1])), "\n")

			// provide messages
			for i := 0; i < len(data); i++ {
				provideMsgToKafka(topic, partition, data[i])
			}
		} else if cmd == "createtopic\n" {
			print("Input topic name:")
			topic, _ := reader.ReadString('\n')
			topic = topic[:len(topic)-1]
			createTopicInKafka(topic)
		}
	}
}

func initialize() {
	readConfigJSON(os.Args[1])
}

func main() {
	initialize()

	if os.Args[2] == "shell" {
		shell()
	} else if os.Args[2] == "createtopic" {
		if len(os.Args) != 4 {
			println("Incorrect Format: go run provider.go [config] createtopic [topic]")
			return
		}
		createTopicInKafka(os.Args[3])
	} else if os.Args[2] == "appendmessage" {
		if len(os.Args) != 6 {
			println("Incorrect Format: go run provider.go [config] appendmessage [topic] [partition] [message]")
			return
		}
		topic := os.Args[3]
		partition := os.Args[4]
		message := os.Args[5]
		provideMsgToKafka(topic, partition, message)
	}
}
