package message

import (
	"time"
)

type OPCODE uint

type ROLE uint

const (
	NEW_BROKER OPCODE = iota
	NEW_TOPIC
	NEW_MESSAGE
	DISPATCH
	Start_Follower
	Start_Leader
	Response
)

const (
	MANAGER ROLE = iota
	LEADER
	FOLLOWER
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
	timestamp time.Time
}
