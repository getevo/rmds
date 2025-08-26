package rmds

import (
	"encoding/json"
	"fmt"
)

const Self = "self"

type Management struct {
	conn *Connection
}

func NewManagement(conn *Connection) *Management {
	return &Management{conn: conn}
}

func (m *Management) Topology() map[string]interface{} {
	return m.conn.discovery.GetTopology()
}

func (m *Management) Nodes() []string {
	return m.conn.discovery.GetAllNodes()
}

func (m *Management) Channels() []string {
	m.conn.mu.RLock()
	defer m.conn.mu.RUnlock()

	channels := []string{}
	for name := range m.conn.channels {
		channels = append(channels, name)
	}
	return channels
}

func (m *Management) GetSubscribers(channelID string) []string {
	return m.conn.discovery.GetChannelNodes(channelID)
}

func (m *Management) Kick(nodeID string) error {
	if !m.conn.isConnected() {
		return fmt.Errorf("not connected to NATS")
	}

	kick := map[string]string{
		"action": "kick",
		"target": nodeID,
		"from":   m.conn.config.NodeID,
	}

	data, err := json.Marshal(kick)
	if err != nil {
		return err
	}

	subject := fmt.Sprintf("%smanagement.kick", m.conn.config.NATSPrefix)
	return m.conn.nc.Publish(subject, data)
}

func (m *Management) KickChannel(channelID string) error {
	if !m.conn.isConnected() {
		return fmt.Errorf("not connected to NATS")
	}

	kick := map[string]string{
		"action":  "kick_channel",
		"channel": channelID,
		"from":    m.conn.config.NodeID,
	}

	data, err := json.Marshal(kick)
	if err != nil {
		return err
	}

	subject := fmt.Sprintf("%smanagement.kick", m.conn.config.NATSPrefix)
	return m.conn.nc.Publish(subject, data)
}

func (m *Management) Statistic() map[string]uint64 {
	stats := m.conn.statistics.GetStats()

	dbStats, _ := m.conn.db.GetStatistics()
	if dbStats != nil {
		if pending, ok := dbStats["pending_messages"].(int); ok {
			stats["pending_messages"] = uint64(pending)
		}
		if sent, ok := dbStats["sent_messages"].(int); ok {
			stats["sent_messages"] = uint64(sent)
		}
		if acknowledged, ok := dbStats["acknowledged_messages"].(int); ok {
			stats["acknowledged_messages"] = uint64(acknowledged)
		}
	}

	stats["nats_connected"] = 0
	if m.conn.getNATSStatus() {
		stats["nats_connected"] = 1
	}

	return stats
}

func (m *Management) PurgeAll(target ...string) error {
	if len(target) > 0 && target[0] == Self {
		return m.conn.db.PurgeAll()
	}

	if !m.conn.isConnected() {
		return fmt.Errorf("not connected to NATS")
	}

	purge := map[string]string{
		"action": "purge_all",
		"from":   m.conn.config.NodeID,
	}

	data, err := json.Marshal(purge)
	if err != nil {
		return err
	}

	subject := fmt.Sprintf("%smanagement.purge", m.conn.config.NATSPrefix)

	if err := m.conn.nc.Publish(subject, data); err != nil {
		return err
	}

	return m.conn.db.PurgeAll()
}

func (m *Management) PurgeSent(target ...string) error {
	if len(target) > 0 && target[0] == Self {
		return m.conn.db.PurgeSent()
	}

	if !m.conn.isConnected() {
		return fmt.Errorf("not connected to NATS")
	}

	purge := map[string]string{
		"action": "purge_sent",
		"from":   m.conn.config.NodeID,
	}

	data, err := json.Marshal(purge)
	if err != nil {
		return err
	}

	subject := fmt.Sprintf("%smanagement.purge", m.conn.config.NATSPrefix)

	if err := m.conn.nc.Publish(subject, data); err != nil {
		return err
	}

	return m.conn.db.PurgeSent()
}

func (m *Management) PurgeQueue(target ...string) error {
	if len(target) > 0 && target[0] == Self {
		return m.conn.db.PurgeQueue()
	}

	if !m.conn.isConnected() {
		return fmt.Errorf("not connected to NATS")
	}

	purge := map[string]string{
		"action": "purge_queue",
		"from":   m.conn.config.NodeID,
	}

	data, err := json.Marshal(purge)
	if err != nil {
		return err
	}

	subject := fmt.Sprintf("%smanagement.purge", m.conn.config.NATSPrefix)

	if err := m.conn.nc.Publish(subject, data); err != nil {
		return err
	}

	return m.conn.db.PurgeQueue()
}
