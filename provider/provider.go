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
	"strings"
)

type configSetting struct {
	ProviderID         string
	RemoteKafkaIPPorts []string
}

var config configSetting

// Message is used for communication among nodes
type Message struct {
	ID   string
	Type string // "cmd" or "text"
	Text string
}

/*
@ para filePath: string
@ Return: []byte
Desc: read and then return the byte of Content from file in corresponding path
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

/* provideMsg
 * para message string
 *
 * Desc:
 * 		send the message to kafka node by remoteIPPort
 */
func provideMsg(remoteIPPort string, msg string) error {
	conn, err := net.Dial("tcp", remoteIPPort)
	if err != nil {
		println("Fail to connect kafka " + remoteIPPort)
		return err
	}
	defer conn.Close()

	// send message
	enc := gob.NewEncoder(conn)
	err = enc.Encode(Message{config.ProviderID, "text", msg})
	if err != nil {
		log.Fatal("encode error:", err)
	}

	// response
	dec := gob.NewDecoder(conn)
	response := &Message{}
	dec.Decode(response)
	fmt.Printf("Response : %+v\n", response)

	return nil
}

/* provideMsgToKafka
 * para message string
 * 		the message string needs sending
 * Desc:
 * 		send the message to connected kafka nodes
 */
func provideMsgToKafka(message string) {
	for i := 0; i < len(config.RemoteKafkaIPPorts); i++ {
		provideMsg(config.RemoteKafkaIPPorts[i], message)
	}
}

func initialize() {
	readConfigJSON(os.Args[1])
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Please provide config filename. e.g. p1.json, p2.json")
		return
	}

	initialize()

	// terminal controller like shell
	println("*******************")
	println("* Instuction: there are several kinds of operations\n* msg: upload the message into connected kafka nodes\n* file: upload the file to connected nodes")
	println("*******************")
	reader := bufio.NewReader(os.Stdin)
	for {
		print("cmd: ")
		cmd, _ := reader.ReadString('\n')
		if cmd == "msg\n" { // input send a messge
			print("Input a message:")
			text, _ := reader.ReadString('\n')
			provideMsgToKafka(text)
		} else if cmd == "file\n" { // input the filename of a set of messages, the messages are divided by '\n'
			print("Input file name:")
			filename, _ := reader.ReadString('\n')
			data := strings.Split(string(readFileByte(filename[:len(filename)-1])), "\n")
			for i := 0; i < len(data); i++ {
				provideMsgToKafka(data[i])
			}
		}
	}
}
