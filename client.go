package main

import "net"

func sendTCP(remoteIPPort string, content string) (string, error) {
	conn, err := net.Dial("tcp", remoteIPPort)
	defer conn.Close() /// wait
	if err != nil {
		return "", nil
	}

	conn.Write([]byte(content))
	buf := make([]byte, 1024)
	c, err := conn.Read(buf)
	if err != nil {
		return "", nil
	}
	// fmt.Println("Reply:", string(buf[0:c]))

	return string(buf[0:c]), nil
}
func main() {
	sendTCP("127.0.0.1:8080", "STOP\n")
}
