package main

import (
	"strings"
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"time"

	m "../lib/message"
	"github.com/DistributedClocks/GoVector/govec"
	"github.com/DistributedClocks/GoVector/govec/vrpc"
)

type configSetting struct {
	BrokerNodeID string
	BrokerIP     string
	ManagerIP    string
}

var config configSetting

var logger *govec.GoLog
var loggerOptions govec.GoLogOptions

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

// Initialize starts the node as a broker node in the network

func Initialize() error {
	configFilename := os.Args[1]

	_, err := os.Stat("disk")
	if err != nil {
		os.Mkdir("disk", os.ModePerm)
	}

	if err := readConfigJSON(configFilename); err != nil {
		return err
	}

	logger = govec.InitGoVector(config.BrokerNodeID, fmt.Sprintf("%v-logfile", config.BrokerNodeID), govec.GetDefaultConfig())
	loggerOptions = govec.GetDefaultLogOptions()

	fmt.Println(config.BrokerIP)
	return nil
}

func registerBrokerWithManager() error {

	fmt.Println("ManagerIP", config.ManagerIP)
	managerAddr, err := net.ResolveTCPAddr("tcp", config.ManagerIP)
	checkError(err)

	rpcClient, err := vrpc.RPCDial("tcp", managerAddr.String(), logger, loggerOptions)
	defer rpcClient.Close()
	checkError(err)

	message := m.Message{
		ID:        config.BrokerNodeID,
		Text:      config.BrokerIP,
		Timestamp: time.Now(),
	}
	var response m.Message
	rpcClient.Call("ManagerRPCServer.AddBroker", message, &response)
	checkError(err)

	for k, v := range response.IPs{
		if BrokerNodeID(k) == broker.brokerNodeID{
			continue
		}
		broker.brokerPeers[BrokerNodeID(k)] = v
	}

	fmt.Println("Broker List")

	managerIPs := strings.Split(response.Text, ",")

	broker.ManagerIPs = managerIPs

	return nil
}

func main() {

	if len(os.Args) != 2 {
		fmt.Println("Please provide config filename. e.g. b1.json, b2.json")
		return
	}

	go shell()

	err := Initialize()
	checkError(err)

	err = InitBroker(config.BrokerIP)
	checkError(err)
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintln(os.Stdout, err.Error())
		os.Exit(1)
	}
}

func shell() {
	fmt.Println("Shell Started")

	reader := bufio.NewReader(os.Stdin)
	for {
		cmd, _ := reader.ReadString('\n')
		if cmd == "partition\n" {
			fmt.Println("Here's partition")
			fmt.Printf("%v\n", broker.partitionMap)
		}else if cmd == "managers\n" {
			for _, v := range broker.ManagerIPs{
				fmt.Println(v)
			}
		}else if cmd == "broker\n" {
			for k, v := range broker.brokerPeers{
				fmt.Println(k,v)
			}
		}
		
		// if cmd == "report\n" {
		// 	fmt.Println("127.0.0.1:3001 reported")
		// 	err := reportNodeFailure("127.0.0.1:3001")
		// 	if err != nil {
		// 		println(err.Error)
		// 	}
		// }
		// } else if cmd == "ring\n" {

		// 	server, _ := ring.GetNode("my_key")
		// 	println(server)

		// } else if cmd == "ring2\n" {
		// 	var v string
		// 	fmt.Scanf("%s", &v)

		// 	var n int
		// 	fmt.Scanf("%d", &n)
		// 	server := getHashingNodes(v, n)
		// 	fmt.Printf("%v\n", server)
		// } else if cmd == "topicmap\n" {

		// } else if cmd == "peer\n" {
		// 	fmt.Println(manager.ManagerPeers)
		// }
	}
}
