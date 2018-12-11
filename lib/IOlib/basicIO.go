package IOlib

import (
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
)

/* readFileByte
 * Desc:
 *		read and then return the byte of Content from file in corresponding path
 * @para: filePath: relative url of file
 * @Return: []byte
 */
func ReadFileByte(filePath string) []byte {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Fatal(err)
	}
	return data
}

// Sendfile: Zero copy
// Sendfile sends count bytes from f to remote a TCP connection.
// f offset is always relative to the current offset.
func Sendfile(conn *net.TCPConn, filename string, startOffset int64, length int64) (n int64, err error) {
	f, err := os.Open(filename)
	f.Seek(startOffset, 0)
	lr := &io.LimitedReader{N: length, R: f}
	n, err = conn.ReadFrom(lr)
	return
}

/*
Name: writeFile
@ para: filePath string
@ para: content string
@ para: appendEnable string
@ Return: None
Func: write the string content into assigned path by method of overwriting or appending
*/
func WriteFile(filePath string, content string, appendEnable bool) error {
	if appendEnable == false {
		f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
		if err != nil {
			return err
		}
		f.WriteString(content)
	} else {
		f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.ModePerm)
		if err != nil {
			return err
		}
		f.WriteString(content)
	}
	return nil
}
