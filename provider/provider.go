package main

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

// reads from config before booting 'shell'
func init() {
	readConfigJSON(os.Args[1])
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
		return
	},
	"GetTopicList": func(args ...string) (err error) {
		getTopicList()
		return
	},
	"Publish": func(args ...string) (err error) {
		if len(args) != 3 {
			return errInvalidArgs
		}

		// topic should be first arg
		topic := args[0]

		// partition number should be second arg
		partitionNum, err := strconv.Atoi(args[1])
		if err != nil {
			return errInvalidArgs
		}

		// message text should be third arg
		text := args[2]

		publishMessage(topic, uint8(partitionNum), text)
		return
	},
	"Subscribe": func(args ...string) (err error) {
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

		subscribe(topic, uint8(partitionNum))
		return
	},
	"ConsumeAt": func(args ...string) (err error) {
		if len(args) != 3 {
			return errInvalidArgs
		}

		// topic should be first arg
		topic := args[0]

		// partition number should be second arg
		partitionNum, err := strconv.Atoi(args[1])
		if err != nil {
			return errInvalidArgs
		}

		// index at which to consume should be third arg
		index, err := strconv.Atoi(args[2])
		if err != nil {
			return errInvalidArgs
		}

		consumeAt(topic, uint8(partitionNum), index)
		return
	},
}

// possible ips that the client can connect to
type topicPartition struct {
	topic     string
	partition uint8
}

var ips = make(map[topicPartition]string)

var logger *govec.GoLog
var loggerOptions govec.GoLogOptions

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

	for i, leaderIP := range response.IPs {
		partition, err := strconv.Atoi(i)
		if err != nil {
			continue
		}
		ips[topicPartition{topic, uint8(partition)}] = leaderIP
	}

	fmt.Printf("Successfully created topic %s with %d partitions\n", topic, partitionNumber)
}

func getTopicList() {
	logger = govec.InitGoVector("client", "clientlogfile", govec.GetDefaultConfig())
	loggerOptions = govec.GetDefaultLogOptions()
	client, err := vrpc.RPCDial("tcp", config.KafkaManagerIPPorts, logger, loggerOptions)
	if err != nil {
		log.Fatal(err)
	}

	var response map[string]uint8
	err = client.Call("ManagerRPCServer.GetTopicList",
		message.Message{
			ID:        config.ProviderID,
			Type:      message.TOPIC_LIST,
			Role:      message.PROVIDER,
			Timestamp: time.Now(),
			Proposer:  config.ProviderID,
		},
		&response)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Got list of topics:\n\n")
	for topic, partitions := range response {
		fmt.Printf("Topic: %s\n", topic)
		fmt.Printf("\tPartitions: ")
		for i := 0; i < int(partitions); i++ {
			fmt.Printf("%d, ", i)
		}
		fmt.Printf("\n")
	}
}

func publishMessage(topic string, partitionNumber uint8, text string) {
	leaderIP := ips[topicPartition{topic, partitionNumber}]
	logger = govec.InitGoVector("client", "clientlogfile", govec.GetDefaultConfig())
	loggerOptions = govec.GetDefaultLogOptions()
	client, err := vrpc.RPCDial("tcp", leaderIP, logger, loggerOptions)
	if err != nil {
		log.Fatal(err)
	}

	var response string
	err = client.Call("BrokerRPCServer.PublishMessage",
		message.Message{
			ID:           config.ProviderID,
			Type:         message.PUSHMESSAGE,
			Topic:        topic,
			Text:         text,
			Role:         message.PROVIDER,
			Timestamp:    time.Now(),
			PartitionIdx: partitionNumber,
			Proposer:     config.ProviderID,
		},
		&response)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Successfully published")
}

func subscribe(topic string, partitionNumber uint8) {
	leaderIP := ips[topicPartition{topic, partitionNumber}]
	logger = govec.InitGoVector("client", "clientlogfile", govec.GetDefaultConfig())
	loggerOptions = govec.GetDefaultLogOptions()
	client, err := vrpc.RPCDial("tcp", leaderIP, logger, loggerOptions)
	if err != nil {
		log.Fatal(err)
	}

	var latestIndex int
	err = client.Call("BrokerRPCServer.GetLatestIndex",
		message.Message{
			ID:           config.ProviderID,
			Type:         message.GET_LATEST_INDEX,
			Topic:        topic,
			Role:         message.PROVIDER,
			Timestamp:    time.Now(),
			PartitionIdx: partitionNumber,
			Proposer:     config.ProviderID,
		},
		&latestIndex)
	if err != nil {
		fmt.Printf("Failed to subscribe\n")
		return
	}

	// TODO: verify with srinjoy
	go func() {
		for {
			var response message.Message
			err = client.Call("BrokerRPCServer.ConsumeAt",
				message.Message{
					ID:           config.ProviderID,
					Type:         message.CONSUME_MESSAGE,
					Topic:        topic,
					Role:         message.PROVIDER,
					Timestamp:    time.Now(),
					PartitionIdx: partitionNumber,
					Index:        latestIndex,
				},
				&response)
			if err != nil {
				fmt.Printf("Failed to consume data from index %d\n", latestIndex)
				continue
			}
			fmt.Println(string(response.Payload))
			latestIndex++
		}
	}()
}

func consumeAt(topic string, partitionNumber uint8, index int) {
	leaderIP := ips[topicPartition{topic, partitionNumber}]
	logger = govec.InitGoVector("client", "clientlogfile", govec.GetDefaultConfig())
	loggerOptions = govec.GetDefaultLogOptions()
	client, err := vrpc.RPCDial("tcp", leaderIP, logger, loggerOptions)
	if err != nil {
		log.Fatal(err)
	}
	var response message.Message
	err = client.Call("BrokerRPCServer.ConsumeAt",
		message.Message{
			ID:           config.ProviderID,
			Type:         message.CONSUME_MESSAGE,
			Topic:        topic,
			Role:         message.PROVIDER,
			Timestamp:    time.Now(),
			PartitionIdx: partitionNumber,
			Index:        index,
		},
		&response)
	if err != nil {
		fmt.Printf("Failed to consume data from index %d\n", index)
		return
	}
	fmt.Println(string(response.Payload))
}

// helper
func getLeader(topic string, partitionNumber uint8) (leaderIP string) {
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
	return
}

func main() {
	runShell()
}

func runShell() {
	// start our "shell" in a polling loop
	fmt.Printf(`
Hi! Possible commands:

	> CreateNewTopic <topicName> <numPartitions> <numReplicas>
	> GetTopicList
	> Publish <topicName> <partitionNum> <message>
	> Subscribe <topicName> <partitionNum>
	> ConsumeAt <topicName> <partitionNum> <index>

`)
	reader := bufio.NewReader(os.Stdin)
	for {

		// display prompt
		fmt.Printf("> ")

		// read the entire string user typed
		fullCmd, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println(err)
			continue
		}

		// trim off trailing newline
		fullCmd = fullCmd[:len(fullCmd)-1]

		// empty input, try again
		if fullCmd == "" {
			continue
		}

		// seperate string by spaces so we can parse arguments
		seperated := strings.Split(fullCmd, " ")
		if len(seperated) < 2 && seperated[0] != "GetTopicList" {
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
