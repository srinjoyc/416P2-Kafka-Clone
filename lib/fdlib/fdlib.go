/*

This package specifies the API to the failure detector library to be
used in assignment 1 of UBC CS 416 2018W1.

You are *not* allowed to change the API below. For example, you can
modify this file by adding an implementation to Initialize, but you
cannot change its API.

*/

package fdlib

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

func init() {
	// Give us line numbers!
	log.SetFlags(log.Lshortfile)
}

// previouslyInitialized tells the library whether or not Initialize has
// been previously called.  Initialize can only be called once.
var previouslyInitialized bool

// Errors.
// multipleInvocationErr signifies that callingFunc can only be called once.
// needsIntermittentCallerr signifies that callingFunc can only be called with intermittent calls to intermittentFunc.
var (
	multipleInvocationErr    = func(callingFunc string) error { return fmt.Errorf("%s can only be called once", callingFunc) }
	needsIntermittentCallErr = func(callingFunc, intermittentFunc string) error {
		return fmt.Errorf("%s can only be called with intermittent calls to %s", callingFunc, intermittentFunc)
	}
	hBeatNotFound = func(key HBeatMessage) error {
		return fmt.Errorf("Heartbeat key %v was not found in hBeatSendTimes map")
	}
)

//////////////////////////////////////////////////////
// Define the message types fdlib has to use to communicate to other
// fdlib instances. We use Go's type declarations for this:
// https://golang.org/ref/spec#Type_declarations
////////////////////////////////////////////////////////

// Heartbeat message.
type HBeatMessage struct {
	EpochNonce uint64 // Identifies this fdlib instance/epoch.
	SeqNum     uint64 // Unique for each heartbeat in an epoch.
}

// An ack message; response to a heartbeat.
type AckMessage struct {
	HBEatEpochNonce uint64 // Copy of what was received in the heartbeat.
	HBEatSeqNum     uint64 // Copy of what was received in the heartbeat.
}

// Notification of a failure, signal back to the client using this
// library.
type FailureDetected struct {
	UDPIpPort string    // The RemoteIP:RemotePort of the failed node.
	Timestamp time.Time // The time when the failure was detected.
}

// Implements FD.
type fdNode struct {

	// Used to respond to heartbeats from other nodes.
	responding         bool
	respondingUDPAddr  *net.UDPAddr
	stopRespondingChan chan struct{}

	// Used to monitor other nodes.
	epochNonce            uint64
	monitoringConns       map[string]*monitor
	failureNotificationCh chan FailureDetected
}

//	 Actively monitors some connection with heartbeats.
type monitor struct {
	stopPollingConnChan   chan struct{}
	stopMonitoringChan    chan struct{}
	failureNotificationCh chan FailureDetected
	mut                   sync.Mutex
	localUDPAddr          *net.UDPAddr
	remoteUDPAddr         *net.UDPAddr
	epochNonce            uint64
	lostMsgThresh         uint8
	lostMsgs              uint8
	currSeqNum            uint64
	hBeatSendTimes        map[HBeatMessage]time.Time
	currRtt               time.Duration
}

//////////////////////////////////////////////////////

// An FD interface represents an instance of the fd
// library. Interfaces are everywhere in Go:
// https://gobyexample.com/interfaces
type FD interface {
	// Tells the library to start responding to heartbeat messages on
	// a local UDP IP:port. Can return an error that is related to the
	// underlying UDP connection.
	StartResponding(LocalIpPort string) (err error)

	// Tells the library to stop responding to heartbeat
	// messages. Always succeeds.
	StopResponding()

	// Tells the library to start monitoring a particular UDP IP:port
	// with a specific lost messages threshold. Can return an error
	// that is related to the underlying UDP connection.
	AddMonitor(LocalIpPort string, RemoteIpPort string, LostMsgThresh uint8) (err error)

	// Tells the library to stop monitoring a particular remote UDP
	// IP:port. Always succeeds.
	RemoveMonitor(RemoteIpPort string)

	// Tells the library to stop monitoring all nodes.
	StopMonitoring()
}

// The constructor for a new FD object instance. Note that notifyCh
// can only be received on by the client that receives it from
// initialize:
// https://www.golang-book.com/books/intro/10
func Initialize(EpochNonce uint64, ChCapacity uint8) (fd FD, notifyCh <-chan FailureDetected, err error) {
	// We can only call this function once.
	// Multiple initializations will result in fd and notifyCh being set to nil.
	if previouslyInitialized {
		return nil, nil, multipleInvocationErr("Initialize")
	}

	previouslyInitialized = true
	notifyChannel := make(chan FailureDetected, ChCapacity)
	fd = &fdNode{
		epochNonce:            EpochNonce,
		monitoringConns:       make(map[string]*monitor),
		failureNotificationCh: notifyChannel,
	}
	return fd, notifyChannel, nil
}

// If error is non-nil, print it out and return it.
func checkError(err error) (duplErr error) {
	if err != nil {
		log.Printf("Error: %s\n", err.Error())
		return err
	}
	return err
}

func logErrorsButTimeout(err error) (duplErr error) {
	if err != nil {
		if errT, ok := err.(net.Error); ok && errT.Timeout() {
			// If this is a network timeout error as described above, don't log it.
			return err
		}
		// Otherwise we should still log this error and continue.
		log.Printf("Error: %s\n", err.Error())
		return err
	}
	return err
}

// Methods defined on Receivers.

func (node *fdNode) StartResponding(LocalIpPort string) (err error) {
	// Make sure that this function has not been called twice in a row.
	// You must make intermittent calls to StopResponding() in between calls
	// to this function.
	if node.responding == true {
		return needsIntermittentCallErr("StartResponding", "StopResponding")
	}
	node.responding = true

	// First convert 'string' format IpPorts to UDPAddr used by Go std library.
	localUDPAddr, err := net.ResolveUDPAddr("udp", LocalIpPort)
	if checkError(err) != nil {
		return err
	}
	node.respondingUDPAddr = localUDPAddr

	// Create connection on a localport, and start listening in a goroutine!
	conn, err := net.ListenUDP("udp", localUDPAddr)
	if checkError(err) != nil {
		return err
	}
	log.Printf("Node set to responding on LocalIpPort: %s\n", LocalIpPort)

	node.stopRespondingChan = make(chan struct{})
	go func() {
		defer conn.Close()
		for {
			select {
			case <-node.stopRespondingChan:
				// Break out of this goroutine when StopResponding() is called,
				// Or more specifically when close(node.stopChan) is called.
				return
			default:
				// Time out on reads and writes every second.  This allows
				// our goroutine to get a chance to be exited, by breaking into
				// the next iteration of the for loop every second.  In terms
				// of reading data, we are not incurring any losses as if the goroutine
				// has not ended, we just jump right back into a read.
				conn.SetReadDeadline(time.Now().Add(time.Second))

				// Otherwise respond!
				b := make([]byte, 1024)
				nRead, remoteUDPAddr, err := conn.ReadFromUDP(b)
				if logErrorsButTimeout(err) != nil {
					continue
				}

				// Get the heartbeat.
				var hBeat HBeatMessage
				rBuf := bytes.NewBuffer(b[:nRead])
				dec := gob.NewDecoder(rBuf)
				err = dec.Decode(&hBeat)
				if err != nil {
					continue
				}
				log.Printf("Received a heartbeat from RemoteIpPort: %s, with data: %v\n", remoteUDPAddr.String(), hBeat)

				// Create a new response ack.
				ack := createResponseAck(hBeat)

				// Encode the response ack.
				var wBuf bytes.Buffer
				enc := gob.NewEncoder(&wBuf)
				err = enc.Encode(ack)
				if checkError(err) != nil {
					continue
				}

				// Send it off!
				// Same logic for deadlines as above.
				conn.SetWriteDeadline(time.Now().Add(time.Second))
				_, err = conn.WriteToUDP(wBuf.Bytes(), remoteUDPAddr)
				if logErrorsButTimeout(err) != nil {
					continue
				}
				log.Printf("Sent ack to RemoteIpPort: %s, with data: %v\n", remoteUDPAddr.String(), ack)
			}
		}
	}()
	return
}

func (node *fdNode) StopResponding() {
	close(node.stopRespondingChan)
	node.responding = false
	log.Printf("Node set to stop responding on LocalIpPort: %s\n", node.respondingUDPAddr.String())
}

func (node *fdNode) AddMonitor(LocalIpPort string, RemoteIpPort string, LostMsgThresh uint8) (err error) {

	// First convert 'string' format IpPorts to UDPAddr used by Go std library.
	localUDPAddr, err := net.ResolveUDPAddr("udp", LocalIpPort)
	if checkError(err) != nil {
		return err
	}

	remoteUDPAddr, err := net.ResolveUDPAddr("udp", RemoteIpPort)
	if checkError(err) != nil {
		return err
	}

	// Re-initialize everything except for currRtt - we would like to preserve that
	// if possible.
	monitoringConn := &monitor{
		localUDPAddr:          localUDPAddr,
		remoteUDPAddr:         remoteUDPAddr,
		lostMsgThresh:         LostMsgThresh,
		lostMsgs:              0,
		currSeqNum:            0,
		epochNonce:            node.epochNonce,
		failureNotificationCh: node.failureNotificationCh,
		hBeatSendTimes:        make(map[HBeatMessage]time.Time),
		stopPollingConnChan:   make(chan struct{}),
		stopMonitoringChan:    make(chan struct{}),
	}

	// Preserve any old RTT value for connections we previously had with identical
	// local and remote addresses.
	if previousConn, ok := node.monitoringConns[LocalIpPort]; ok {
		// This connection has an RTT history.  Take that as current RTT.
		log.Println("Previously monitored node found")
		monitoringConn.currRtt = previousConn.currRtt
	} else {
		// Otherwise default RTT is 3.0 seconds.
		log.Println("New node found")
		monitoringConn.currRtt = time.Duration(3 * time.Second)
	}

	node.monitoringConns[LocalIpPort] = monitoringConn
	log.Printf("Added monitored node:\n\tLocalIpPort: %s\n\tRemoteIpPort: %s\n", LocalIpPort, RemoteIpPort)

	// Start monitoring RemoteIpPort!
	go monitoringConn.startMonitoring()
	log.Printf("Starting to monitor node with RTT of: %s\n", monitoringConn.currRtt.String())

	return
}

func (node *fdNode) RemoveMonitor(RemoteIpPort string) {
	// Find the correct monitoring connection that has the remote address
	// RemoteIpPort, and stop monitoring that connection.
	for _, conn := range node.monitoringConns {
		if conn.remoteUDPAddr.String() == RemoteIpPort {
			conn.stopMonitoring()
			log.Printf("Removing monitor at RemoteIpPort: %s\n", RemoteIpPort)
			return
		}
	}
	log.Printf("Monitor at RemoteIpPort: %s not found.  No-op.\n", RemoteIpPort)
}

func (node *fdNode) StopMonitoring() {
	// Close all open monitoring connections, and remove them from the node.
	log.Println("Removing all monitors")
	for _, conn := range node.monitoringConns {
		conn.stopMonitoring()
		log.Printf("Removed monitor at: %s\n", conn.remoteUDPAddr)
	}
}

////////////////////////////// End implements FD /////////////////////////////////////

// Create a response AckMessage with equivalent fields
// to the received HBeatMessage.
func createResponseAck(hBeat HBeatMessage) AckMessage {
	return AckMessage{
		hBeat.EpochNonce, hBeat.SeqNum,
	}
}

// Same as above, reversed.
func recreateCorrespondingHBeat(ack AckMessage) HBeatMessage {
	return HBeatMessage{
		ack.HBEatEpochNonce, ack.HBEatSeqNum,
	}
}

// Check if a particular AckMessage ack corresponds to the most recently
// sent heartbeat by this monitor.
func (monitor *monitor) isCorrectAck(ack AckMessage) (correct bool) {
	desiredEpochNonce, desiredSeqNum := monitor.currHBeat()
	return desiredEpochNonce == ack.HBEatEpochNonce && desiredSeqNum == ack.HBEatSeqNum
}

// Check if a particular AckMessage has the same HBEatEpochNonce of this monitor.
func (monitor *monitor) hasCorrectEpochNonce(ack AckMessage) (correct bool) {
	desiredEpochNonce, _ := monitor.currHBeat()
	return desiredEpochNonce == ack.HBEatEpochNonce
}

// Goroutine safe get lost messages.
func (monitor *monitor) getLostMsgs() (lostMsgs uint8) {
	monitor.mut.Lock()
	defer monitor.mut.Unlock()
	return monitor.lostMsgs
}

// Goroutine safe set lost messages.
func (monitor *monitor) setLostMsgs(to uint8) {
	monitor.mut.Lock()
	monitor.lostMsgs = to
	monitor.mut.Unlock()
}

// Goroutine safe increment lost messages.
func (monitor *monitor) incLostMsgs() {
	monitor.mut.Lock()
	monitor.lostMsgs++
	monitor.mut.Unlock()
}

// Goroutine safe get current heartbeat details.
func (monitor *monitor) currHBeat() (epochNonce, seqNum uint64) {
	monitor.mut.Lock()
	defer monitor.mut.Unlock()
	return monitor.epochNonce, monitor.currSeqNum
}

// Goroutine safe set current monitor rtt to rtt.
func (monitor *monitor) setCurrRtt(rtt time.Duration) {
	monitor.mut.Lock()
	monitor.currRtt = rtt
	monitor.mut.Unlock()
}

// Goroutine safe get current monitor rtt.
func (monitor *monitor) getCurrRtt() (rtt time.Duration) {
	monitor.mut.Lock()
	defer monitor.mut.Unlock()
	return monitor.currRtt
}

// Goroutine safe increment monitor sequence number.
func (monitor *monitor) incSeqNum() {
	monitor.mut.Lock()
	monitor.currSeqNum++
	monitor.mut.Unlock()
}

// Goroutine safe get send time of a certain heartbeat.
func (monitor *monitor) getSendTime(hBeat HBeatMessage) (sendTime time.Time, err error) {
	monitor.mut.Lock()
	defer monitor.mut.Unlock()

	if sendTime, ok := monitor.hBeatSendTimes[hBeat]; ok {
		return sendTime, nil
	}

	// Return the "Zero-time" and an error.
	return time.Time{}, hBeatNotFound(hBeat)
}

func (monitor *monitor) setSendTimeNow(hBeat HBeatMessage) {
	monitor.mut.Lock()
	monitor.hBeatSendTimes[hBeat] = time.Now()
	monitor.mut.Unlock()
}

// Send a new heartbeat.
func (monitor *monitor) sendHBeat(conn *net.UDPConn) (err error) {
	// Increment the sequence number of this heartbeat and create the struct.
	// Note: incrementing first means that the first seqNum will be 1, followed by
	// 2, ...etc.
	monitor.incSeqNum()
	hBeat := HBeatMessage{
		EpochNonce: monitor.epochNonce,
		SeqNum:     monitor.currSeqNum,
	}

	// Encode this heartbeat into a buffer buf.
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err = enc.Encode(hBeat)
	if checkError(err) != nil {
		return err
	}

	// Send the heartbeat o monitor.remoteUDPAddr.
	_, err = conn.Write(buf.Bytes())
	if checkError(err) != nil {
		return err
	}
	log.Println("Sent heartbeat:", hBeat)

	// Save this hBeats send time.
	monitor.setSendTimeNow(hBeat)
	return
}

// Must be started as a goroutine. Start monitoring with this monitor.
func (monitor *monitor) startMonitoring() (err error) {

	// Listen for acks at monitor.localUDPAddr.
	conn, err := net.DialUDP("udp", monitor.localUDPAddr, monitor.remoteUDPAddr)
	if checkError(err) != nil {
		return
	}

	defer log.Println("Exited goroutine sending heartbeats to:", monitor.remoteUDPAddr.String())

	ackChan := make(chan AckMessage)

	// Spin up a goroutine which polls from conn, trying to receive ACKS.
	// Pass in the mutex, so its available in this goroutine even when startMonitoring ends.
	go func() {
		// This goroutine may continue to read for up to a second after stopPollingConnChan
		// receives a notification to terminate the goroutine.  That means that we
		// should close the connection here, as opposed to in the heartbeat loop,
		// to avoid reading from an already closed connection.
		defer func() {
			conn.Close()
			log.Println("Exited ack polling goroutine polling on LocalIpPort:", monitor.localUDPAddr.String())
		}()

		for {
			select {
			case <-monitor.stopPollingConnChan:
				// End this goroutine.
				return
			default:
				// Time out on reads and writes every second.  This allows
				// our goroutine to get a chance to be exited, by breaking into
				// the next iteration of the for loop every second.  In terms
				// of reading data, we are not incurring any losses as if the goroutine
				// has not ended, we just jump right back into a read.
				conn.SetReadDeadline(time.Now().Add(time.Second))

				// Try and read from the connection.
				// Here, we are polling for ACKS.
				b := make([]byte, 1024)
				nRead, err := conn.Read(b)
				if logErrorsButTimeout(err) != nil {
					continue
				}

				// Decode the message into an AckMessage.
				var ack AckMessage
				buf := bytes.NewBuffer(b[:nRead])
				decoder := gob.NewDecoder(buf)
				err = decoder.Decode(&ack)
				if err != nil {
					continue
				}

				// If this ack has the wrong EpochNonce, just throw it out.
				// We definately can't use this!
				if !monitor.hasCorrectEpochNonce(ack) {
					continue
				}

				// Lets do the calculation for the rtt of whatever seqNum
				// this hBeat <-> ack transaction was.
				hBeat := recreateCorrespondingHBeat(ack)
				sendTime, err := monitor.getSendTime(hBeat)
				if checkError(err) != nil {
					// If the sendTime couldn't be found, this means that the ack we received is totally
					// rogue.  Who knows where this came from? Who cares! Ignore and proceed onwards.
					continue
				}

				rtt := time.Since(sendTime)
				monitor.setCurrRtt((rtt + monitor.getCurrRtt()) / 2)

				// Set our lost messages count to 0.
				monitor.setLostMsgs(0)

				// If this is the ack of the most recent heartbeat, send it down the channel.
				if monitor.isCorrectAck(ack) {
					// Only send into ackchan if its non-nil, i.e. we are still monitoring.
					// Ackchan is accessed from two goroutines which modify its nil/non-nil state;
					// we should use a mutex for this case.
					monitor.mut.Lock()
					if ackChan != nil {
						ackChan <- ack
					}
					monitor.mut.Unlock()
				}
			}
		}
	}()

	// Send our initial heartbeat.
	err = monitor.sendHBeat(conn)
	if checkError(err) != nil {
		return err
	}

	for {
		// Get the most up to date rtt for this heartbeat.
		currRtt := monitor.getCurrRtt()
		log.Println("Current rtt for this node:", currRtt)

		select {
		case <-monitor.stopMonitoringChan:
			// If this function is called as a goroutine, this
			// provides a way out.
			return
		case <-time.After(currRtt):
			// We timed out and lost a message.
			log.Println("Timed out!")

			// Increment our number of lost messages.
			monitor.incLostMsgs()
			log.Printf("Number of lost messages: %d\n", monitor.getLostMsgs())

			// If we've hit the lost messages threshold,
			// we need to stop monitoring this node and
			// send a failure notification.
			if monitor.getLostMsgs() >= monitor.lostMsgThresh {
				// We are now done monitoring this node.
				// End this goroutine by closing stopMonitoringChan, but first set ackChan
				// to nil so we don't accidentally process one more ACK.
				// We need to use a mutex here to avoid a data race when setting ackChan to nil.
				monitor.mut.Lock()
				ackChan = nil
				monitor.mut.Unlock()
				monitor.stopMonitoring()

				// Send failure notification.
				log.Printf("Met the lost message threshold of %d\n", monitor.lostMsgThresh)
				log.Printf("Sending failure and halting monitoring of node at RemoteIpPort: %s\n", monitor.remoteUDPAddr.String())
				monitor.failureNotificationCh <- FailureDetected{
					UDPIpPort: monitor.remoteUDPAddr.String(),
					Timestamp: time.Now(),
				}
			} else {
				// Otherwise, send another heartbeat.
				err := monitor.sendHBeat(conn)
				checkError(err)
			}

		case ack := <-ackChan:
			// We received the correct ack!
			log.Printf("Received ack: %v\n", ack)

			// Lets wait until the last second before sending our next heartbeat.  This
			// allows time for any old acks to come in and stabilize this monitors current
			// best rtt guess. To make the calculation of how long to wait, we need the
			// rtt that was used for this heartbeat, its original send time, and the current time.
			triggerringHBeat := recreateCorrespondingHBeat(ack)
			sendTime, err := monitor.getSendTime(triggerringHBeat)
			if checkError(err) != nil {
				// Theoretically, this should NEVER happen because this ack would
				// not have been sent through ackChan if it wasn't the correct one.
				// Regardless, check it anyways.
				continue
			}

			// Wait for send time + rtt used.
			toWait := time.Until(sendTime.Add(currRtt))

			// Wait to allow rtt stabilization.
			// We are not using time.Sleep because time.Sleep only guarantees
			// to sleep for AT LEAST the specified duration.  Probably not an issue,
			// but we cannot tolerate sleeping for too long, so do it this way.
			select {
			case <-time.After(toWait):
				break
			}

			// We can now send another heartbeat with the most up to date rtt.
			err = monitor.sendHBeat(conn)
			checkError(err)
		}
	}
}

func (monitor *monitor) stopMonitoring() {
	close(monitor.stopMonitoringChan)
	close(monitor.stopPollingConnChan)
}
