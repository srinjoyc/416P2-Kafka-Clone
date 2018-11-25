package manager

import (
	"fmt"
)

// Initialize starts the node as a Manager node in the network
func Initialize() bool {
	fmt.Println("Inited Node as a Manager Node.")
	return true
}
