package message

import "time"

type OPCODE uint

type ROLE uint

const (
	NEW_BROKER   OPCODE = iota
	NEW_TOPIC           // start a new topic (manager chooses leader/followers and start the leader)
	START_LEADER        // start a leader given a particular topic & partition
	GET_LEADER          // push to an existing topic
	INFO                //random text msg about the system
	LEADER_NODE_DOWN
	FOLLOWER_NODE_DOWN
	PROMOTE
	MANAGER_SYNC
	MANAGER_PUSH
	MANAGER_RESPONSE_TO_PROVIDER
)

const (
	MANAGER ROLE = iota
	LEADER
	FOLLOWER
	PROVIDER
	UNID
)

// Message is used for communication among nodes
type Message struct {
	ID        string
	Type      OPCODE
	Text      string
	Topic     string
	Partition uint8
	Role      ROLE
	Timestamp time.Time
	Ack       bool
	IPs       []string
}
