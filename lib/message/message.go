package message

import (
	"crypto/sha1"
	"strconv"
	"time"
)

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
	CREATE_NEW_TOPIC
	PUSHMESSAGE
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
	ID         string
	Type       OPCODE
	Text       string
	Topic      string
	Partition  uint8
	Role       ROLE
	Proposer   string
	IPs        []string
	Timestamp  time.Time
	ReplicaNum int
}

func (m *Message) Hash() [sha1.Size]byte {
	var buf []byte

	buf = append(buf, []byte(m.ID)...)
	buf = append(buf, []byte(strconv.FormatUint(uint64(m.Type), 10))...)
	buf = append(buf, []byte(m.Text)...)
	buf = append(buf, []byte(m.Topic)...)
	buf = append(buf, []byte(strconv.FormatUint(uint64(m.Partition), 10))...)
	buf = append(buf, []byte(strconv.FormatUint(uint64(m.Role), 10))...)
	buf = append(buf, []byte(m.Proposer)...)

	for _, ip := range m.IPs {
		buf = append(buf, []byte(ip)...)
	}

	timeByte, _ := m.Timestamp.MarshalText()
	buf = append(buf, timeByte...)

	return sha1.Sum(buf)
}
