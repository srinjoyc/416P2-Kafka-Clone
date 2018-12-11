package main

// This is a sample client cli program

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"../lib/IOlib"
	message "../lib/message"

	"github.com/DistributedClocks/GoVector/govec"
	"github.com/DistributedClocks/GoVector/govec/vrpc"
)

type configSetting struct {
	ProviderID          string
	KafkaManagerIPPorts string
}

var config configSetting
var logger *govec.GoLog
var loggerOptions govec.GoLogOptions
var errInvalidArgs = errors.New("invalid arguments")

// possible cmds that the shell can perform
var cmds = map[string]func(...string) error{
	"CreateNewTopic": func(args ...string) (err error) {
		if len(args) != 3 {
			return errInvalidArgs
		}

		// topic should be first arg
		topic := args[0]

		// num partitions should be second arg
		numPartitions, err := strconv.Atoi(args[1])
		if err != nil {
			return errInvalidArgs
		}

		// num replicas should be third arg
		numReplicas, err := strconv.Atoi(args[2])
		if err != nil {
			return errInvalidArgs
		}

		createNewTopic(topic, uint8(numPartitions), numReplicas)
		return nil
	},
	"GetLeader": func(args ...string) error {
		if len(args) != 2 {
			return errInvalidArgs
		}

		// topic should be first arg
		topic := args[0]

		// partition number should be second arg
		partitionNum, err := strconv.Atoi(args[1])
		if err != nil {
			return errInvalidArgs
		}
		getLeader(topic, uint8(partitionNum))
		return nil
	},
}

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

// reads from config before booting 'shell'
func init() {
	readConfigJSON(os.Args[1])
}

/* provideMsg
 * para message string
 *
 * Desc:
 * 		send the message to kafka node by remoteIPPort
 */
// func provideMsg(remoteIPPort string, outgoing message.Message) error {
// 	conn, err := net.Dial("tcp", remoteIPPort)
// 	if err != nil {
// 		println("Fail to connect kafka manager" + remoteIPPort)
// 		return err
// 	}
// 	defer conn.Close()

// 	// send message
// 	enc := gob.NewEncoder(conn)
// 	err = enc.Encode(outgoing)
// 	if err != nil {
// 		log.Fatal("encode error:", err)
// 	}

// 	// response
// 	dec := gob.NewDecoder(conn)
// 	response := &message.Message{}
// 	dec.Decode(response)
// 	fmt.Println(response)
// 	fmt.Printf("Response : {kID:%s, status:%s}\n", response.ID, response.Text)

// 	return nil
// }

// /* provideMsgToKafka
//  * para message string
//  * 		the message string needs sending
//  * Desc:
//  * 		send the message to connected kafka nodes
//  */
// func provideMsgToKafka(topic string, partition string, msg string) {
// 	message := message.Message{config.ProviderID, "Text", msg, topic, partition}
// 	for i := 0; i < len(config.KafkaManagerIPPorts); i++ {
// 		if nil == provideMsg(config.KafkaManagerIPPorts[i], message) {
// 			break
// 		}
// 		// if one manager is down, connect another one
// 		// tries to connect to all managers, stopping at the first successful connection
// 	}
// }

// func createTopicInKafka(topicName string) {
// 	message := message.Message{config.ProviderID, "CreateTopic", "", topicName, ""}
// 	for i := 0; i < len(config.KafkaManagerIPPorts); i++ {
// 		if nil == provideMsg(config.KafkaManagerIPPorts[i], message) {
// 			break
// 		}
// 		// if one manager is down, connect another one
// 		// tries to connect to all managers, stopping at the first successful connection
// 	}
// }

// func shell() {
// 	reader := bufio.NewReader(os.Stdin)
// 	for {
// 		print("cmd: ")
// 		cmd, _ := reader.ReadString('\n')
// 		if cmd == "msg\n" { // input send a messge
// 			var message, partition, topic string

// 			// read data from stdin
// 			print("Input topic: ")
// 			fmt.Scanln(&topic)
// 			print("Input partition: ")
// 			fmt.Scanln(&partition)
// 			print("Input message: ")
// 			message, _ = reader.ReadString('\n')
// 			message = message[:len(message)-1]

// 			// provide message
// 			provideMsgToKafka(topic, partition, message)
// 		} else if cmd == "file\n" { // input the filename of a set of messages, the messages are divided by '\n'
// 			var topic, partition string

// 			// read data from stdin
// 			print("Input topic: ")
// 			fmt.Scanln(&topic)
// 			print("Input partition list such as 1,2,3: ")
// 			fmt.Scanln(&partition)
// 			print("Input file name:")
// 			filename, _ := reader.ReadString('\n')
// 			data := strings.Split(string(IOlib.ReadFileByte(filename[:len(filename)-1])), "\n")

// 			// provide messages
// 			for i := 0; i < len(data); i++ {
// 				provideMsgToKafka(topic, partition, data[i])
// 			}
// 		} else if cmd == "createtopic\n" {
// 			print("Input topic name:")
// 			topic, _ := reader.ReadString('\n')
// 			topic = topic[:len(topic)-1]
// 			createTopicInKafka(topic)
// 		}
// 	}
// }

func createNewTopic(topic string, partitionNumber uint8, replicaNum int) {
	logger = govec.InitGoVector("client", "clientlogfile", govec.GetDefaultConfig())
	loggerOptions = govec.GetDefaultLogOptions()
	client, err := vrpc.RPCDial("tcp", config.KafkaManagerIPPorts, logger, loggerOptions)

	if err != nil {
		log.Fatal(err)
	}
	var response message.Message
	err = client.Call("ManagerRPCServer.CreateNewTopic",
		message.Message{
			ID:         config.ProviderID,
			Type:       message.NEW_TOPIC,
			Topic:      topic,
			Role:       message.PROVIDER,
			Timestamp:  time.Now(),
			Partitions: partitionNumber,
			ReplicaNum: replicaNum,
			Proposer:   config.ProviderID,
		},
		&response)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Success: %v\n", response.IPs)
}

func getLeader(topic string, partitionNumber uint8) {
	logger = govec.InitGoVector("client", "clientlogfile", govec.GetDefaultConfig())
	loggerOptions = govec.GetDefaultLogOptions()
	client, err := vrpc.RPCDial("tcp", config.KafkaManagerIPPorts, logger, loggerOptions)

	if err != nil {
		log.Fatal(err)
	}
	var response string
	err = client.Call("ManagerRPCServer.GetLeader",
		message.Message{
			ID:           config.ProviderID,
			Type:         message.GET_LEADER,
			Topic:        topic,
			Role:         message.PROVIDER,
			Timestamp:    time.Now(),
			PartitionIdx: partitionNumber,
		},
		&response)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Success: %v\n", response)
}

func main() {
	runShell()
	// if os.Args[2] == "shell" {
	// 	shell()
	// } else if os.Args[2] == "createtopic" {
	// 	if len(os.Args) != 4 {
	// 		println("Incorrect Format: go run provider.go [config] createtopic [topic]")
	// 		return
	// 	}
	// 	createTopicInKafka(os.Args[3])
	// } else if os.Args[2] == "appendmessage" {
	// 	if len(os.Args) != 6 {
	// 		println("Incorrect Format: go run provider.go [config] appendmessage [topic] [partition] [message]")
	// 		return
	// 	}

	// 	//provideMsgToKafka(topic, partition, message)
	// }

	// topic := os.Args[3]
	// argMsg := os.Args[5]
	// // send msg
	// msg := message.Message{config.ProviderID, message.NEW_TOPIC, argMsg, topic, 0, message.PROVIDER, time.Now()}
	// provideMsg(config.KafkaManagerIPPorts[0], msg)
}

func runShell() {
	// start our "shell" in a polling loop
	reader := bufio.NewReader(os.Stdin)
	for {

		// read the entire string user typed
		fullCmd, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println(err)
			continue
		}

		// trim off trailing newline
		fullCmd = fullCmd[:len(fullCmd)-1]

		// seperate string by spaces so we can parse arguments
		seperated := strings.Split(fullCmd, " ")
		if len(seperated) < 2 {
			fmt.Println("invalid command")
			continue
		}

		// handle the command with provided arguments
		cmd, args := seperated[0], seperated[1:]
		if cmdHandler, ok := cmds[cmd]; ok {
			if err := cmdHandler(args...); err != nil {
				fmt.Println(err)
				continue
			}
		} else {
			fmt.Println("invalid command")
			continue
		}
	}
}
