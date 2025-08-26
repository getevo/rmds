package rmds

import (
	"fmt"
	"strings"
	"time"
)

type Config struct {
	NodeID                     string        // Unique identifier for this node instance
	NATSServers                []string      // List of NATS server URLs for clustering
	NATSPrefix                 string        // Prefix for all NATS subjects used by RMDS
	EnableCompression          bool          // Enable Snappy compression for message payloads
	StoragePath                string        // Path to SQLite database file (supports {nodeID} placeholder)
	KeepaliveInterval          time.Duration // How often to send keepalive messages
	MessageExpiry              time.Duration // Default expiry time for messages if not specified
	NodeOfflineTimeout         time.Duration // Time to wait before considering a node offline
	DiscoveryBroadcastInterval time.Duration // How often to broadcast node discovery information
	DeliveryRetryInterval      time.Duration // Time between delivery retry attempts
	ACKTimeout                 time.Duration // Maximum time to wait for message acknowledgment
	ManagementChannel          string        // NATS channel for management operations
	DiscoveryChannel           string        // NATS channel for node discovery
	KeepaliveChannel           string        // NATS channel for keepalive messages
	MaxMessagesPerReceiver     int           // Maximum queued messages per receiver
	MessageCleanupInterval     time.Duration // How often to clean up old messages
	DeliveredMessageRetention  time.Duration // How long to keep delivered messages
	ExpiredNodeCleanupInterval time.Duration // How often to clean up expired node information
	MaxRetryAttempts           int           // Maximum number of retry attempts per message
}

func DefaultConfig() *Config {
	return &Config{
		NodeID:                     "",
		NATSServers:                []string{"nats://localhost:4222"},
		NATSPrefix:                 "rmds.",
		EnableCompression:          true,
		StoragePath:                "./rmds-{nodeID}.db",
		KeepaliveInterval:          10 * time.Second,
		MessageExpiry:              24 * time.Hour,
		NodeOfflineTimeout:         30 * time.Second,
		DiscoveryBroadcastInterval: 30 * time.Second,
		DeliveryRetryInterval:      1 * time.Second,
		ACKTimeout:                 10 * time.Second,
		ManagementChannel:          "management",
		DiscoveryChannel:           "discovery",
		KeepaliveChannel:           "keepalive",
		MaxMessagesPerReceiver:     10000,
		MessageCleanupInterval:     1 * time.Hour,
		DeliveredMessageRetention:  3 * 24 * time.Hour,
		ExpiredNodeCleanupInterval: 24 * time.Hour,
		MaxRetryAttempts:           10,
	}
}

func (c *Config) GetDatabasePath() string {
	if c.NodeID != "" {
		// Handle both {nodeID} placeholder and %s format specifier
		if strings.Contains(c.StoragePath, "{nodeID}") {
			return strings.ReplaceAll(c.StoragePath, "{nodeID}", c.NodeID)
		} else if strings.Contains(c.StoragePath, "%s") {
			return fmt.Sprintf(c.StoragePath, c.NodeID)
		}
	}
	return c.StoragePath
}
