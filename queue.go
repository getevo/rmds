package rmds

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/getevo/evo/lib/log"
	"github.com/golang/snappy"
)

// Helper function for min operation
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type MessageQueue struct {
	conn          *Connection
	consumers     map[string]*MessageConsumer // Per-receiver consumers
	mu            sync.RWMutex
	wg            sync.WaitGroup
}

// MessageConsumer handles one receiver with strict FIFO ordering
type MessageConsumer struct {
	receiver     string
	queue        *MessageQueue
	stopChan     chan struct{}
	ackChan      chan string // Channel to receive ACKs for this receiver
}

type QueuedMessage struct {
	Message    *Message
	Receiver   string
	Retries    int
	LastRetry  time.Time
	DatabaseID string // Unique ID for database operations
}

func NewMessageQueue(conn *Connection) *MessageQueue {
	return &MessageQueue{
		conn:      conn,
		consumers: make(map[string]*MessageConsumer),
	}
}

func (q *MessageQueue) Start() {
	// Start consumers for existing receivers
	q.startConsumersForExistingReceivers()
}

// Start consumers for receivers that already have pending messages
func (q *MessageQueue) startConsumersForExistingReceivers() {
	// Get all receivers that have pending messages in the database
	receivers, err := q.conn.db.GetPendingReceivers()
	if err != nil {
		return
	}
	
	for _, receiver := range receivers {
		q.getOrCreateConsumer(receiver)
	}
}

// getOrCreateConsumer gets or creates a consumer for a receiver
func (q *MessageQueue) getOrCreateConsumer(receiver string) *MessageConsumer {
	q.mu.Lock()
	defer q.mu.Unlock()
	
	consumer, exists := q.consumers[receiver]
	if !exists {
		consumer = &MessageConsumer{
			receiver: receiver,
			queue:    q,
			stopChan: make(chan struct{}),
			ackChan:  make(chan string, 100), // Buffer for ACKs
		}
		q.consumers[receiver] = consumer
		
		// Start the consumer goroutine
		q.wg.Add(1)
		go consumer.run()
		
		log.Debug("[QUEUE DEBUG] Started consumer for receiver %s", receiver)
	}
	
	return consumer
}

// shouldRetryToReceiver checks if we should retry delivery to a receiver
// based on last seen time (must be within KeepaliveInterval * 2)
func (c *MessageConsumer) shouldRetryToReceiver() bool {
	discovery := c.queue.conn.discovery
	discovery.mu.RLock()
	defer discovery.mu.RUnlock()
	
	node, exists := discovery.nodes[c.receiver]
	if !exists {
		// Node not discovered yet - check database for topology
		return discovery.conn.db.IsNodeAlive(c.receiver)
	}
	
	// Check if node was seen recently enough to retry delivery
	maxOfflineTime := c.queue.conn.config.KeepaliveInterval * 2
	timeSinceLastSeen := time.Since(node.LastSeen)
	
	shouldRetry := timeSinceLastSeen <= maxOfflineTime
	log.Debug("[CONSUMER DEBUG] Node %s last seen %v ago, max offline time %v, should retry: %v", 
		c.receiver, timeSinceLastSeen, maxOfflineTime, shouldRetry)
	
	return shouldRetry
}

// run is the main consumer loop - reads one message, sends it, waits for ACK, repeat
func (c *MessageConsumer) run() {
	defer c.queue.wg.Done()
	log.Debug("[CONSUMER DEBUG] Consumer for receiver %s started", c.receiver)
	
	for {
		select {
		case <-c.stopChan:
			log.Debug("[CONSUMER DEBUG] Consumer for receiver %s stopped", c.receiver)
			return
		case <-c.queue.conn.ctx.Done():
			log.Debug("[CONSUMER DEBUG] Consumer for receiver %s stopped (context done)", c.receiver)
			return
		default:
			// Try to get next message from database
			if !c.processNextMessage() {
				// No message found, small delay to prevent tight loop
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
}

// processNextMessage returns true if a message was processed, false if no message available
func (c *MessageConsumer) processNextMessage() bool {
	// Get the next pending message from database
	messages, err := c.queue.conn.db.GetPendingMessages(c.receiver)
	if err != nil || len(messages) == 0 {
		return false // No messages to process
	}
	
	stored := messages[0] // Get first message (oldest)
	
	// Extract original message ID from unique database ID (format: originalID-receiver)
	originalID := stored.ID
	if dashIdx := len(stored.ID) - len(stored.Receiver) - 1; dashIdx > 0 && stored.ID[dashIdx] == '-' {
		originalID = stored.ID[:dashIdx]
	}
	
	msg := &Message{
		ID:        originalID,
		Channel:   stored.Channel,
		Sender:    stored.Sender,
		Data:      stored.Data,
		CreatedAt: stored.CreatedAt,
		ExpiresAt: stored.ExpiresAt,
	}
	
	qmsg := &QueuedMessage{
		Message:    msg,
		Receiver:   stored.Receiver,
		Retries:    stored.RetryCount,
		LastRetry:  stored.LastRetry,
		DatabaseID: stored.ID, // Keep unique ID for database operations
	}
	
	log.Debug("[CONSUMER DEBUG] Processing message %s to receiver %s", msg.ID, c.receiver)
	
	// Check if message expired
	if time.Now().After(msg.ExpiresAt) {
		log.Debug("[CONSUMER DEBUG] Message %s expired, dropping", msg.ID)
		c.queue.conn.statistics.IncrementDropped()
		c.queue.conn.db.MarkMessageSent(qmsg.DatabaseID)
		return true
	}
	
	// Check if receiver should receive retries based on last seen time
	if !c.shouldRetryToReceiver() {
		log.Debug("[CONSUMER DEBUG] Receiver %s offline too long, pausing retries", c.receiver)
		time.Sleep(5 * time.Second)
		return true
	}
	
	// Check if connected to NATS
	if !c.queue.conn.isConnected() {
		log.Debug("[CONSUMER DEBUG] Not connected to NATS, waiting")
		time.Sleep(2 * time.Second)
		return true
	}
	
	// Send the message
	if c.sendMessage(qmsg) {
		// Message sent successfully, now BLOCK waiting for ACK
		c.waitForACK(qmsg)
		return true
	}
	
	return true
}

// sendMessage publishes message to NATS
func (c *MessageConsumer) sendMessage(qmsg *QueuedMessage) bool {
	data, err := json.Marshal(qmsg.Message)
	if err != nil {
		log.Error("[CONSUMER DEBUG] Failed to marshal message %s: %v", qmsg.Message.ID, err)
		c.queue.conn.statistics.IncrementDropped()
		return false
	}

	if c.queue.conn.config.EnableCompression {
		data = snappy.Encode(nil, data)
	}

	subject := fmt.Sprintf("%schannel.%s.%s", c.queue.conn.config.NATSPrefix, qmsg.Message.Channel, c.receiver)
	log.Debug("[CONSUMER DEBUG] Publishing message %s to NATS subject: %s", qmsg.Message.ID, subject)

	if err := c.queue.conn.nc.Publish(subject, data); err != nil {
		log.Error("[CONSUMER DEBUG] Failed to publish message %s: %v", qmsg.Message.ID, err)
		return false
	}

	log.Debug("[CONSUMER DEBUG] Successfully published message %s to NATS", qmsg.Message.ID)
	c.queue.conn.db.MarkMessageSent(qmsg.DatabaseID)
	c.queue.conn.statistics.IncrementDelivered()
	return true
}

// waitForACK blocks until ACK is received or timeout
func (c *MessageConsumer) waitForACK(qmsg *QueuedMessage) {
	timeout := time.After(c.queue.conn.config.ACKTimeout)
	
	log.Debug("[CONSUMER DEBUG] Waiting for ACK for message %s", qmsg.Message.ID)
	
	select {
	case ackMsgID := <-c.ackChan:
		if ackMsgID == qmsg.Message.ID {
			log.Debug("[CONSUMER DEBUG] Received ACK for message %s - proceeding to next", qmsg.Message.ID)
			// Use the unique database ID for acknowledgment
			c.queue.conn.db.MarkMessageAcknowledged(qmsg.DatabaseID, c.receiver)
			c.queue.conn.statistics.IncrementReceivedAcks()
		} else {
			log.Debug("[CONSUMER DEBUG] Received ACK for different message %s (expected %s)", ackMsgID, qmsg.Message.ID)
			// Put it back in the channel for other messages
			select {
			case c.ackChan <- ackMsgID:
			default:
			}
		}
	case <-timeout:
		log.Debug("[CONSUMER DEBUG] ACK timeout for message %s - will retry", qmsg.Message.ID)
		// Don't mark as acknowledged, will be retried next time
	case <-c.stopChan:
		return
	case <-c.queue.conn.ctx.Done():
		return
	}
}

// OLD METHODS REMOVED - Now using producer-consumer pattern

// Enqueue is the PRODUCER - simply writes message to database
func (q *MessageQueue) Enqueue(msg *Message, receiver string) {
	log.Debug("[PRODUCER DEBUG] Enqueueing message %s for receiver %s", msg.ID, receiver)
	
	// Generate unique ID per receiver but keep original message ID for ACK tracking
	uniqueID := fmt.Sprintf("%s-%s", msg.ID, receiver)
	
	storedMsg := &StoredMessage{
		ID:        uniqueID,
		Channel:   msg.Channel,
		Sender:    msg.Sender,
		Receiver:  receiver,
		Data:      msg.Data,
		CreatedAt: msg.CreatedAt,
		ExpiresAt: msg.ExpiresAt,
		Status:    "pending",
	}

	if err := q.conn.db.StoreMessage(storedMsg); err != nil {
		log.Error("[PRODUCER DEBUG] Failed to store message %s in database: %v", msg.ID, err)
		return
	}

	log.Debug("[PRODUCER DEBUG] Message %s stored in database for receiver %s", msg.ID, receiver)
	
	// Ensure consumer exists for this receiver
	q.getOrCreateConsumer(receiver)
}

// DeliverACK delivers an ACK to the appropriate consumer
func (q *MessageQueue) DeliverACK(messageID string, receiver string) {
	q.mu.RLock()
	consumer, exists := q.consumers[receiver]
	q.mu.RUnlock()
	
	if exists {
		select {
		case consumer.ackChan <- messageID:
			log.Debug("[QUEUE DEBUG] Delivered ACK for message %s to consumer %s", messageID, receiver)
		default:
			log.Debug("[QUEUE DEBUG] ACK channel full for consumer %s, dropping ACK for message %s", receiver, messageID)
		}
	} else {
		log.Debug("[QUEUE DEBUG] No consumer found for receiver %s, cannot deliver ACK for message %s", receiver, messageID)
	}
}

// FlushPendingMessages is called when NATS reconnects - consumers will automatically pick up messages
func (q *MessageQueue) FlushPendingMessages() {
	log.Debug("[QUEUE DEBUG] NATS reconnected - consumers will resume processing")
	// No action needed - consumers are already running and will pick up messages
}

func (q *MessageQueue) Stop() {
	q.mu.Lock()
	for receiver, consumer := range q.consumers {
		close(consumer.stopChan)
		log.Debug("[QUEUE DEBUG] Stopping consumer for receiver %s", receiver)
	}
	q.mu.Unlock()
	
	q.wg.Wait()
}
