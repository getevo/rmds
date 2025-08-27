package rmds

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/getevo/hash"
	"github.com/getevo/network"
	"github.com/golang/snappy"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

type ChannelMode int

const (
	ReadOnly ChannelMode = iota
	WriteOnly
	RW
)

type Option func(*Connection)

type Connection struct {
	config          *Config
	nc              *nats.Conn
	db              *Database
	channels        map[string]*Channel
	mu              sync.RWMutex
	ctx             context.Context
	cancel          context.CancelFunc
	queue           *MessageQueue
	discovery       *Discovery
	statistics      *Statistics
	Mgmt            *Management
	isRunning       bool
	natsStatus      bool
	statusMu        sync.RWMutex
	ackSubscription *nats.Subscription
}

type Channel struct {
	conn       *Connection
	name       string
	mode       ChannelMode
	sub        *nats.Subscription
	handlers   []MessageHandler
	mu         sync.RWMutex
	subscribed bool
}

type Message struct {
	ID        string    `json:"id"`
	Channel   string    `json:"channel"`
	Sender    string    `json:"sender"`
	Data      []byte    `json:"data"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

type MessageHandler func(*Message)

type SendOption func(*sendOptions)

type sendOptions struct {
	timeout time.Duration
}

func New(config *Config, opts ...Option) (*Connection, error) {
	if config == nil {
		config = DefaultConfig()
	}

	if config.NodeID == "" {
		net, _ := network.GetConfig()
		hostname, _ := os.Hostname()
		nodeID := hostname + "_"
		if net != nil && net.LocalIP != nil {
			nodeID += net.LocalIP.String() + "_"
		}
		if net != nil && net.HardwareAddress != nil {
			nodeID += hash.CRC32String(net.HardwareAddress.String())
		}
		nodeID = strings.ReplaceAll(nodeID, "[^a-zA-Z0-9_]", "_")
		config.NodeID = nodeID
	}

	ctx, cancel := context.WithCancel(context.Background())

	conn := &Connection{
		config:   config,
		channels: make(map[string]*Channel),
		ctx:      ctx,
		cancel:   cancel,
	}

	for _, opt := range opts {
		opt(conn)
	}

	var err error
	dbPath := config.GetDatabasePath()
	conn.db, err = NewDatabase(dbPath)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	conn.queue = NewMessageQueue(conn)
	conn.discovery = NewDiscovery(conn)
	conn.statistics = NewStatistics()
	conn.Mgmt = NewManagement(conn)

	go conn.maintainConnection()
	go conn.queue.Start()
	go conn.discovery.Start()
	go conn.cleanupRoutine()

	conn.isRunning = true

	return conn, nil
}

func (c *Connection) maintainConnection() {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			if !c.isConnected() {
				c.connect()
			}
			time.Sleep(c.config.DeliveryRetryInterval)
		}
	}
}

func (c *Connection) connect() error {
	opts := []nats.Option{
		nats.MaxReconnects(-1),
		nats.ReconnectWait(c.config.DeliveryRetryInterval),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			c.setNATSStatus(false)
			c.statistics.IncrementReconnects()
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			c.setNATSStatus(true)
			c.statistics.IncrementReconnects()
			c.resubscribeChannels()
			// Immediately flush any pending messages when NATS reconnects
			if c.queue != nil {
				go c.queue.FlushPendingMessages()
			}
		}),
		nats.ConnectHandler(func(nc *nats.Conn) {
			c.setNATSStatus(true)
			// Also flush pending messages on initial connect
			if c.queue != nil {
				go c.queue.FlushPendingMessages()
			}
			// Subscribe to ACKs on initial connect
			go c.subscribeToACKs()
		}),
	}

	nc, err := nats.Connect(c.buildNATSURL(), opts...)
	if err != nil {
		c.setNATSStatus(false)
		return err
	}

	c.mu.Lock()
	c.nc = nc
	c.mu.Unlock()

	c.setNATSStatus(true)
	return nil
}

func (c *Connection) buildNATSURL() string {
	if len(c.config.NATSServers) == 0 {
		return "nats://localhost:4222"
	}
	url := ""
	for i, server := range c.config.NATSServers {
		if i > 0 {
			url += ","
		}
		url += server
	}
	return url
}

func (c *Connection) isConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.nc != nil && c.nc.IsConnected()
}

func (c *Connection) setNATSStatus(status bool) {
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	c.natsStatus = status
}

func (c *Connection) getNATSStatus() bool {
	c.statusMu.RLock()
	defer c.statusMu.RUnlock()
	return c.natsStatus
}

func (c *Connection) Join(channelName string, mode ChannelMode) *Channel {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ch, exists := c.channels[channelName]; exists {
		return ch
	}

	ch := &Channel{
		conn:     c,
		name:     channelName,
		mode:     mode,
		handlers: []MessageHandler{},
	}

	c.channels[channelName] = ch

	if mode == ReadOnly || mode == RW {
		go func() {
			// Retry subscription until successful
			for {
				if err := ch.subscribe(); err == nil {
					break
				}
				fmt.Printf("[SUBSCRIBE DEBUG] Retrying subscription in 1 second...\n")
				time.Sleep(time.Second)
				select {
				case <-c.ctx.Done():
					return
				default:
				}
			}
		}()
	}

	c.discovery.AnnounceChannel(channelName, mode)

	return ch
}

func (ch *Channel) subscribe() error {
	if ch.mode == WriteOnly {
		fmt.Printf("[SUBSCRIBE DEBUG] Channel '%s' is WriteOnly, skipping subscription\n", ch.name)
		return nil
	}

	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.subscribed {
		fmt.Printf("[SUBSCRIBE DEBUG] Channel '%s' already subscribed\n", ch.name)
		return nil
	}

	if !ch.conn.isConnected() {
		fmt.Printf("[SUBSCRIBE DEBUG] Not connected to NATS, cannot subscribe to channel '%s'\n", ch.name)
		return fmt.Errorf("not connected to NATS")
	}

	subject := fmt.Sprintf("%schannel.%s.%s", ch.conn.config.NATSPrefix, ch.name, ch.conn.config.NodeID)
	fmt.Printf("[SUBSCRIBE DEBUG] Subscribing to NATS subject: %s\n", subject)

	sub, err := ch.conn.nc.Subscribe(subject, func(msg *nats.Msg) {
		fmt.Printf("[SUBSCRIBE DEBUG] Received NATS message on subject: %s\n", subject)
		ch.handleMessage(msg.Data)
	})

	if err != nil {
		fmt.Printf("[SUBSCRIBE DEBUG] Failed to subscribe to subject '%s': %v\n", subject, err)
		return err
	}

	ch.sub = sub
	ch.subscribed = true
	fmt.Printf("[SUBSCRIBE DEBUG] Successfully subscribed to subject: %s\n", subject)

	return nil
}

func (ch *Channel) handleMessage(data []byte) {
	fmt.Printf("[RMDS DEBUG] handleMessage: Received message on channel '%s' for node '%s'\n", ch.name, ch.conn.config.NodeID)

	if ch.conn.config.EnableCompression {
		decompressed, err := snappy.Decode(nil, data)
		if err == nil {
			data = decompressed
		}
	}

	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		fmt.Printf("[RMDS DEBUG] handleMessage: Failed to unmarshal message: %v\n", err)
		return
	}

	fmt.Printf("[RMDS DEBUG] handleMessage: Parsed message ID=%s from sender=%s\n", msg.ID, msg.Sender)

	if time.Now().After(msg.ExpiresAt) {
		fmt.Printf("[RMDS DEBUG] handleMessage: Message expired, dropping\n")
		ch.conn.statistics.IncrementDropped()
		return
	}

	ch.conn.statistics.IncrementProcessed()

	ch.sendAck(msg.ID, msg.Sender)

	ch.mu.RLock()
	handlers := make([]MessageHandler, len(ch.handlers))
	copy(handlers, ch.handlers)
	ch.mu.RUnlock()

	fmt.Printf("[RMDS DEBUG] handleMessage: Calling %d handlers for message\n", len(handlers))
	for _, handler := range handlers {
		go handler(&msg)
	}
}

func (ch *Channel) sendAck(messageID, sender string) {
	if !ch.conn.isConnected() {
		return
	}

	ack := map[string]string{
		"message_id": messageID,
		"receiver":   ch.conn.config.NodeID,
	}

	msgData, _ := json.Marshal(ack)

	if ch.conn.config.EnableCompression {
		compressed := snappy.Encode(nil, msgData)
		msgData = compressed
	}

	subject := fmt.Sprintf("%sack.%s", ch.conn.config.NATSPrefix, sender)

	ch.conn.nc.Publish(subject, msgData)
	ch.conn.statistics.IncrementSentAcks()
}

func (ch *Channel) OnMessage(handler MessageHandler) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.handlers = append(ch.handlers, handler)
}

func (ch *Channel) SendMessage(data interface{}, opts ...SendOption) error {
	if ch.mode == ReadOnly {
		return fmt.Errorf("channel is read-only")
	}

	options := &sendOptions{
		timeout: ch.conn.config.MessageExpiry,
	}

	for _, opt := range opts {
		opt(options)
	}

	var msgData []byte
	var err error

	switch v := data.(type) {
	case []byte:
		msgData = v
	case string:
		msgData = []byte(v)
	default:
		msgData, err = json.Marshal(v)
		if err != nil {
			return err
		}
	}

	msg := &Message{
		ID:        uuid.New().String(),
		Channel:   ch.name,
		Sender:    ch.conn.config.NodeID,
		Data:      msgData,
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(options.timeout),
	}

	nodes := ch.conn.discovery.GetChannelNodes(ch.name)
	fmt.Printf("[RMDS DEBUG] SendMessage: Found %d channel nodes for channel '%s': %v\n", len(nodes), ch.name, nodes)

	for _, nodeID := range nodes {
		if nodeID == ch.conn.config.NodeID {
			continue
		}
		fmt.Printf("[RMDS DEBUG] SendMessage: Enqueueing message %s to node %s\n", msg.ID, nodeID)
		ch.conn.queue.Enqueue(msg, nodeID)
	}

	ch.conn.statistics.IncrementQueued(uint64(len(nodes)))

	return nil
}

func (ch *Channel) Unsubscribe() error {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.sub != nil {
		err := ch.sub.Unsubscribe()
		ch.sub = nil
		ch.subscribed = false
		ch.conn.discovery.RemoveChannel(ch.name)
		return err
	}

	return nil
}

func (c *Connection) resubscribeChannels() {
	c.mu.RLock()
	channels := make([]*Channel, 0, len(c.channels))
	for _, ch := range c.channels {
		channels = append(channels, ch)
	}
	c.mu.RUnlock()

	for _, ch := range channels {
		if ch.mode == ReadOnly || ch.mode == RW {
			ch.subscribe()
		}
		c.discovery.AnnounceChannel(ch.name, ch.mode)
	}

	// Subscribe to ACKs for the producer-consumer pattern
	c.subscribeToACKs()
}

// subscribeToACKs sets up ACK subscription for the producer-consumer pattern
func (c *Connection) subscribeToACKs() {
	if !c.isConnected() {
		return
	}

	ackSubject := fmt.Sprintf("%sack.%s", c.config.NATSPrefix, c.config.NodeID)

	sub, err := c.nc.Subscribe(ackSubject, func(msg *nats.Msg) {
		c.handleACK(msg)
	})

	if err != nil {
		fmt.Printf("[ACK DEBUG] Failed to subscribe to ACK subject %s: %v\n", ackSubject, err)
		return
	}

	fmt.Printf("[ACK DEBUG] Subscribed to ACK subject: %s\n", ackSubject)

	// Store subscription for cleanup
	c.mu.Lock()
	c.ackSubscription = sub
	c.mu.Unlock()
}

// handleACK processes incoming ACKs and delivers them to the appropriate consumer
func (c *Connection) handleACK(msg *nats.Msg) {
	data := msg.Data
	if c.config.EnableCompression {
		if decompressed, err := snappy.Decode(nil, data); err == nil {
			data = decompressed
		}
	}

	var ack map[string]string
	if err := json.Unmarshal(data, &ack); err != nil {
		fmt.Printf("[ACK DEBUG] Failed to unmarshal ACK: %v\n", err)
		return
	}

	messageID := ack["message_id"]
	receiver := ack["receiver"]

	if messageID == "" || receiver == "" {
		fmt.Printf("[ACK DEBUG] Invalid ACK - missing message_id or receiver\n")
		return
	}

	fmt.Printf("[ACK DEBUG] Received ACK for message %s from receiver %s\n", messageID, receiver)

	// Deliver ACK to the appropriate consumer
	if c.queue != nil {
		c.queue.DeliverACK(messageID, receiver)
	}
}

func (c *Connection) Unsubscribe() error {
	c.cancel()
	c.isRunning = false

	c.mu.RLock()
	channels := make([]*Channel, 0, len(c.channels))
	for _, ch := range c.channels {
		channels = append(channels, ch)
	}
	c.mu.RUnlock()

	for _, ch := range channels {
		ch.Unsubscribe()
	}

	c.discovery.Shutdown()

	if c.nc != nil {
		c.nc.Close()
	}

	if c.db != nil {
		c.db.Close()
	}

	return nil
}

func (c *Connection) cleanupRoutine() {
	ticker := time.NewTicker(c.config.MessageCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.db.CleanupOldMessages(c.config.DeliveredMessageRetention)
		}
	}
}

func SetID(id string) Option {
	return func(c *Connection) {
		c.config.NodeID = id
	}
}

func SetNatsPrefix(prefix string) Option {
	return func(c *Connection) {
		c.config.NATSPrefix = prefix
	}
}

func SetNATS(servers string) Option {
	return func(c *Connection) {
		urls := []string{}
		for _, s := range splitAndTrim(servers, ",") {
			urls = append(urls, s)
		}
		c.config.NATSServers = urls
	}
}

func Timeout(d time.Duration) SendOption {
	return func(o *sendOptions) {
		o.timeout = d
	}
}

func splitAndTrim(s, sep string) []string {
	parts := []string{}
	for _, part := range splitString(s, sep) {
		if trimmed := trimSpace(part); trimmed != "" {
			parts = append(parts, trimmed)
		}
	}
	return parts
}

func splitString(s, sep string) []string {
	var result []string
	start := 0
	for i := 0; i <= len(s)-len(sep); i++ {
		if s[i:i+len(sep)] == sep {
			result = append(result, s[start:i])
			start = i + len(sep)
			i = start - 1
		}
	}
	result = append(result, s[start:])
	return result
}

func trimSpace(s string) string {
	start := 0
	end := len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\n' || s[start] == '\r') {
		start++
	}
	for start < end && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\n' || s[end-1] == '\r') {
		end--
	}
	return s[start:end]
}

func (c *Connection) GetDiscovery() *Discovery {
	return c.discovery
}

func (c *Connection) GetNC() *nats.Conn {
	return c.nc
}
