package rmds

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/getevo/evo/lib/log"
	"github.com/golang/snappy"
	"github.com/nats-io/nats.go"
)

type Discovery struct {
	conn     *Connection
	nodes    map[string]*NodeInfo
	channels map[string]map[string]ChannelMode
	mu       sync.RWMutex
	ticker   *time.Ticker
}

type NodeInfo struct {
	ID       string
	LastSeen time.Time
	Channels map[string]ChannelMode
}

type KeepAlive struct {
	NodeID   string                 `json:"node_id"`
	Channels map[string]ChannelMode `json:"channels"`
	Time     time.Time              `json:"time"`
}

func NewDiscovery(conn *Connection) *Discovery {
	return &Discovery{
		conn:     conn,
		nodes:    make(map[string]*NodeInfo),
		channels: make(map[string]map[string]ChannelMode),
	}
}

func (d *Discovery) Start() {
	// Load topology from database first
	d.LoadTopologyFromDatabase()

	d.ticker = time.NewTicker(d.conn.config.KeepaliveInterval)

	go d.sendKeepAlivesWithDelay()
	go d.listenForKeepAlives()
	go d.cleanupStaleNodes()
}

func (d *Discovery) sendKeepAlivesWithDelay() {
	// Wait for the configured initial delay before sending first keepalive
	log.Debug("[DISCOVERY DEBUG] Waiting %v before sending first keepalive", d.conn.config.FirstKeepaliveDelay)
	time.Sleep(d.conn.config.FirstKeepaliveDelay)

	// Now start regular keepalive sending
	d.sendKeepAlives()
}

func (d *Discovery) sendKeepAlives() {
	for {
		select {
		case <-d.conn.ctx.Done():
			return
		case <-d.ticker.C:
			if !d.conn.isConnected() {
				continue
			}

			d.mu.RLock()
			channels := make(map[string]ChannelMode)
			for name, ch := range d.conn.channels {
				channels[name] = ch.mode
			}
			d.mu.RUnlock()

			keepAlive := &KeepAlive{
				NodeID:   d.conn.config.NodeID,
				Channels: channels,
				Time:     time.Now(),
			}

			data, err := json.Marshal(keepAlive)
			if err != nil {
				continue
			}

			if d.conn.config.EnableCompression {
				data = snappy.Encode(nil, data)
			}

			subject := fmt.Sprintf("%s%s", d.conn.config.NATSPrefix, d.conn.config.KeepaliveChannel)
			d.conn.nc.Publish(subject, data)
			d.conn.statistics.IncrementSentKeepAlives()
		}
	}
}

func (d *Discovery) listenForKeepAlives() {
	for {
		if !d.conn.isConnected() {
			time.Sleep(time.Second)
			continue
		}

		subject := fmt.Sprintf("%s%s", d.conn.config.NATSPrefix, d.conn.config.KeepaliveChannel)
		sub, err := d.conn.nc.Subscribe(subject, func(msg *nats.Msg) {
			d.handleKeepAlive(msg.Data)
		})

		if err != nil {
			time.Sleep(time.Second)
			continue
		}

		<-d.conn.ctx.Done()
		sub.Unsubscribe()
		return
	}
}

func (d *Discovery) handleKeepAlive(data []byte) {
	if d.conn.config.EnableCompression {
		if decompressed, err := snappy.Decode(nil, data); err == nil {
			data = decompressed
		}
	}

	var keepAlive KeepAlive
	if err := json.Unmarshal(data, &keepAlive); err != nil {
		log.Error("[DISCOVERY DEBUG] Failed to unmarshal keepalive: %v", err)
		return
	}

	log.Debug("[DISCOVERY DEBUG] Received keepalive from node '%s' with channels: %+v", keepAlive.NodeID, keepAlive.Channels)

	if keepAlive.NodeID == d.conn.config.NodeID {
		log.Debug("[DISCOVERY DEBUG] Ignoring keepalive from self")
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	node, exists := d.nodes[keepAlive.NodeID]
	if !exists {
		log.Debug("[DISCOVERY DEBUG] New node discovered: %s", keepAlive.NodeID)
		node = &NodeInfo{
			ID:       keepAlive.NodeID,
			Channels: make(map[string]ChannelMode),
		}
		d.nodes[keepAlive.NodeID] = node
	} else {
		log.Debug("[DISCOVERY DEBUG] Updating existing node: %s", keepAlive.NodeID)
	}

	node.LastSeen = keepAlive.Time
	node.Channels = keepAlive.Channels

	for channel, mode := range keepAlive.Channels {
		if d.channels[channel] == nil {
			d.channels[channel] = make(map[string]ChannelMode)
		}
		d.channels[channel][keepAlive.NodeID] = mode
		log.Debug("[DISCOVERY DEBUG] Node '%s' registered on channel '%s' with mode %d", keepAlive.NodeID, channel, mode)

		// Save to database immediately
		modeStr := ""
		switch mode {
		case ReadOnly:
			modeStr = "reader"
		case WriteOnly:
			modeStr = "writer"
		case RW:
			modeStr = "rw"
		}

		if modeStr != "" {
			if err := d.conn.db.SaveTopologyNode(keepAlive.NodeID, channel, modeStr, true); err != nil {
				log.Error("[DISCOVERY DEBUG] Error saving topology node %s/%s: %v",
					keepAlive.NodeID, channel, err)
			}
		}
	}

	d.conn.statistics.IncrementReceivedKeepAlives()
}

func (d *Discovery) cleanupStaleNodes() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-d.conn.ctx.Done():
			return
		case <-ticker.C:
			d.mu.Lock()
			cutoff := time.Now().Add(-d.conn.config.NodeOfflineTimeout)

			for nodeID, node := range d.nodes {
				if node.LastSeen.Before(cutoff) {
					log.Debug("[DISCOVERY DEBUG] Node %s is now offline", nodeID)

					// Mark node as offline in database instead of deleting
					if err := d.conn.db.UpdateNodeStatus(nodeID, false); err != nil {
						log.Error("[DISCOVERY DEBUG] Error marking node %s as offline: %v", nodeID, err)
					}

					delete(d.nodes, nodeID)

					for channel := range d.channels {
						delete(d.channels[channel], nodeID)
					}
				}
			}
			d.mu.Unlock()
		}
	}
}

func (d *Discovery) IsNodeAlive(nodeID string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// First check in-memory nodes
	node, exists := d.nodes[nodeID]
	if exists {
		return time.Since(node.LastSeen) < d.conn.config.NodeOfflineTimeout
	}

	// Fallback to database when node not in memory
	return d.conn.db.IsNodeAlive(nodeID)
}

func (d *Discovery) GetChannelNodes(channel string) []string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	nodes := []string{}
	log.Debug("[DISCOVERY DEBUG] GetChannelNodes: Looking for nodes on channel '%s'", channel)

	// First try in-memory discovery
	if channelNodes, exists := d.channels[channel]; exists {
		log.Debug("[DISCOVERY DEBUG] Channel '%s' has nodes: %+v", channel, channelNodes)
		for nodeID, mode := range channelNodes {
			log.Debug("[DISCOVERY DEBUG] Node '%s' has mode %d (ReadOnly=%d, RW=%d)", nodeID, mode, ReadOnly, RW)
			if mode == ReadOnly || mode == RW {
				// Always include receivers in target list for message queuing
				// Even offline receivers should get messages queued for reliable delivery
				log.Debug("[DISCOVERY DEBUG] Adding node '%s' to target list (alive=%v)", nodeID, d.IsNodeAlive(nodeID))
				nodes = append(nodes, nodeID)
			} else {
				log.Debug("[DISCOVERY DEBUG] Node '%s' mode %d is not ReadOnly or RW", nodeID, mode)
			}
		}
	}

	// Fallback to database when no receivers found in memory
	if len(nodes) == 0 {
		log.Debug("[DISCOVERY DEBUG] No receivers found in memory for channel '%s', checking database", channel)
		if dbNodes, err := d.conn.db.GetAllNodesForChannel(channel); err == nil && len(dbNodes) > 0 {
			nodes = dbNodes
			log.Debug("[DISCOVERY DEBUG] Found %d nodes in database for channel '%s': %v", len(nodes), channel, nodes)
		} else if err != nil {
			log.Error("[DISCOVERY DEBUG] Error getting nodes from database: %v", err)
		} else {
			log.Debug("[DISCOVERY DEBUG] No nodes found in database for channel '%s'", channel)
		}
	}

	log.Debug("[DISCOVERY DEBUG] GetChannelNodes returning %d nodes: %v", len(nodes), nodes)
	return nodes
}

func (d *Discovery) GetAllNodes() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	nodes := []string{}
	for nodeID := range d.nodes {
		nodes = append(nodes, nodeID)
	}
	return nodes
}

func (d *Discovery) AnnounceChannel(channel string, mode ChannelMode) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.channels[channel] == nil {
		d.channels[channel] = make(map[string]ChannelMode)
	}
	d.channels[channel][d.conn.config.NodeID] = mode
}

func (d *Discovery) RemoveChannel(channel string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.channels[channel] != nil {
		delete(d.channels[channel], d.conn.config.NodeID)
	}
}

func (d *Discovery) Shutdown() {
	if d.ticker != nil {
		d.ticker.Stop()
	}

	if d.conn.isConnected() {
		data, _ := json.Marshal(map[string]string{
			"node_id": d.conn.config.NodeID,
			"action":  "shutdown",
		})

		subject := fmt.Sprintf("%sshutdown", d.conn.config.NATSPrefix)
		d.conn.nc.Publish(subject, data)
	}
}

func (d *Discovery) GetTopology() map[string]interface{} {
	d.mu.RLock()
	defer d.mu.RUnlock()

	topology := map[string]interface{}{
		"nodes":    make([]map[string]interface{}, 0),
		"channels": make(map[string][]string),
	}

	for nodeID, node := range d.nodes {
		nodeInfo := map[string]interface{}{
			"id":        nodeID,
			"last_seen": node.LastSeen,
			"alive":     d.IsNodeAlive(nodeID),
			"channels":  node.Channels,
		}
		topology["nodes"] = append(topology["nodes"].([]map[string]interface{}), nodeInfo)
	}

	for channel, nodes := range d.channels {
		nodeList := []string{}
		for nodeID := range nodes {
			nodeList = append(nodeList, nodeID)
		}
		topology["channels"].(map[string][]string)[channel] = nodeList
	}

	return topology
}

// LoadTopologyFromDatabase loads the topology from SQLite database on startup
func (d *Discovery) LoadTopologyFromDatabase() {
	log.Debug("[DISCOVERY DEBUG] Loading topology from database")

	nodes, err := d.conn.db.GetTopologyNodes()
	if err != nil {
		log.Error("[DISCOVERY DEBUG] Error loading topology from database: %v", err)
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	for _, topologyNode := range nodes {
		nodeID := topologyNode.NodeID

		// Create or update node info
		node, exists := d.nodes[nodeID]
		if !exists {
			node = &NodeInfo{
				ID:       nodeID,
				Channels: make(map[string]ChannelMode),
			}
			d.nodes[nodeID] = node
		}

		node.LastSeen = topologyNode.LastSeen

		// Parse mode string to ChannelMode
		var mode ChannelMode
		switch topologyNode.Mode {
		case "reader":
			mode = ReadOnly
		case "writer":
			mode = WriteOnly
		case "rw":
			mode = RW
		default:
			continue
		}

		node.Channels[topologyNode.Channel] = mode

		// Update channels map
		if d.channels[topologyNode.Channel] == nil {
			d.channels[topologyNode.Channel] = make(map[string]ChannelMode)
		}

		// Only include if node was alive when stored
		if topologyNode.IsAlive {
			d.channels[topologyNode.Channel][nodeID] = mode
			log.Debug("[DISCOVERY DEBUG] Loaded node '%s' on channel '%s' with mode %s",
				nodeID, topologyNode.Channel, topologyNode.Mode)
		}
	}

	log.Debug("[DISCOVERY DEBUG] Loaded %d topology nodes from database", len(nodes))
}

// SaveTopologyToDatabase saves current topology state to database
func (d *Discovery) SaveTopologyToDatabase() {
	d.mu.RLock()
	defer d.mu.RUnlock()

	for nodeID, node := range d.nodes {
		isAlive := d.IsNodeAlive(nodeID)

		// Update node status first
		if err := d.conn.db.UpdateNodeStatus(nodeID, isAlive); err != nil {
			log.Error("[DISCOVERY DEBUG] Error updating node status for %s: %v", nodeID, err)
		}

		// Save each channel this node participates in
		for channel, mode := range node.Channels {
			modeStr := ""
			switch mode {
			case ReadOnly:
				modeStr = "reader"
			case WriteOnly:
				modeStr = "writer"
			case RW:
				modeStr = "rw"
			}

			if modeStr != "" {
				if err := d.conn.db.SaveTopologyNode(nodeID, channel, modeStr, isAlive); err != nil {
					log.Error("[DISCOVERY DEBUG] Error saving topology node %s/%s: %v",
						nodeID, channel, err)
				}
			}
		}
	}
}
