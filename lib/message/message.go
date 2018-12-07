package message

// Message is used for communication among nodes
type Message struct {
	ID        string
	Type      string
	Text      string
	Topic     string
	Partition string
}
