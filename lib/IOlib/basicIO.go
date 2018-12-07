package IOlib
import (
	"io/ioutil"
	"log"
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


