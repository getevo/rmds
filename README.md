# RMDS - Reliable Message Delivery System

RMDS is a resilient message delivery system for Go applications that ensures reliable communication even in unreliable network conditions. Built on top of NATS pub/sub, it provides message persistence, automatic retries, and guaranteed delivery without requiring NATS JetStream.

## Features

### Core Messaging
- **Zero Message Loss**: Messages are persisted in SQLite and retried until acknowledged
- **FIFO Ordering**: Strict first-in-first-out message delivery per receiver
- **Channel-based Communication**: Publisher-subscriber pattern with role-based access control
- **Reliable Delivery**: ACK-based confirmation with automatic retry mechanisms
- **Network Optimization**: Snappy compression to minimize bandwidth usage

### Network Resilience
- **NATS Transport**: Uses NATS pub/sub for high-performance messaging
- **Message Queuing**: SQLite persistence during NATS outages
- **NATS Auto-Recovery**: Automatically retries all pending messages when NATS reconnects
- **Keepalive System**: 10-second heartbeats for node health monitoring
- **Automatic Reconnection**: Handles multi-day network outages gracefully
- **Queue Management**: Configurable message limits with overflow handling

### Architecture
- **Node Discovery**: Automatic peer discovery via NATS broadcast messages
- **Database Isolation**: Each node maintains separate SQLite database
- **Concurrent Delivery**: 1-message-at-a-time delivery with configurable ACK timeout
- **Role-based Access**: Nodes can join channels as reader, writer, or both
- **Real-time Statistics**: Message delivery tracking and performance metrics

## Installation

```bash
go get github.com/getevo/rmds
```

## Quick Start

```go
package main

import (
    "fmt"
    "time"
    "github.com/getevo/rmds"
)

func main() {
    // Create configuration
    config := rmds.DefaultConfig()
    config.NodeID = "node-1"
    config.NATSServers = []string{"nats://localhost:4222"}
    
    // Create connection
    connection, err := rmds.New(config)
    if err != nil {
        panic(err)
    }
    defer connection.Unsubscribe()
    
    // Join a channel
    channel := connection.Join("my-channel", rmds.RW)
    
    // Set up message handler
    channel.OnMessage(func(message *rmds.Message) {
        fmt.Printf("Received: %s from %s\n", string(message.Data), message.Sender)
        // Manually acknowledge the message
        message.Ack()
    })
    
    // Send a message
    err = channel.SendMessage([]byte("Hello, World!"))
    if err != nil {
        fmt.Printf("Error sending message: %v\n", err)
    }
    
    // Keep the program running
    select {}
}
```

## Configuration

```go
config := &rmds.Config{
    NodeID:                     "node-12345678",        // Unique identifier for this node
    NATSServers:                []string{                // NATS cluster URLs
        "nats://nats1:4222",
        "nats://nats2:4222",
        "nats://nats3:4222",
    },
    NATSPrefix:                 "rmds.",                 // Prefix for all NATS subjects
    EnableCompression:          true,                    // Enable Snappy compression
    StoragePath:                "./rmds-{nodeID}.db",    // SQLite database path
    KeepaliveInterval:          10 * time.Second,        // Heartbeat interval
    MessageExpiry:              24 * time.Hour,          // Default message TTL
    NodeOfflineTimeout:         30 * time.Second,        // Node offline detection
    DiscoveryBroadcastInterval: 30 * time.Second,        // Discovery broadcast rate
    DeliveryRetryInterval:      5 * time.Second,         // Retry interval
    ACKTimeout:                 10 * time.Second,        // ACK wait timeout
    ManagementChannel:          "management",            // Management channel name
    DiscoveryChannel:           "discovery",             // Discovery channel name
    KeepaliveChannel:           "keepalive",             // Keepalive channel name
    MaxMessagesPerReceiver:     10000,                   // Max queued messages
    MessageCleanupInterval:     1 * time.Hour,           // Cleanup frequency
    DeliveredMessageRetention:  3 * 24 * time.Hour,      // Delivered message retention
    ExpiredNodeCleanupInterval: 24 * time.Hour,          // Expired node cleanup
    MaxRetryAttempts:           10,                      // Max retry attempts
}
```

## API Usage

### Creating a Connection

```go
// With default config
connection, err := rmds.New(nil)

// With custom config
config := rmds.DefaultConfig()
config.NodeID = "my-node"
connection, err := rmds.New(config)

// With options
connection, err := rmds.New(config,
    rmds.SetID("my-node"),
    rmds.SetNatsPrefix("custom."),
    rmds.SetNATS("nats://server1:4222,nats://server2:4222"),
)
```

### Joining Channels

```go
// Join as writer only
writeChannel := connection.Join("notifications", rmds.WriteOnly)

// Join as reader only
readChannel := connection.Join("events", rmds.ReadOnly)

// Join as both reader and writer
rwChannel := connection.Join("chat", rmds.RW)
```

### Sending Messages

```go
// Send raw bytes
channel.SendMessage([]byte("Hello"))

// Send string
channel.SendMessage("Hello, World!")

// Send struct (will be JSON encoded)
type User struct {
    Username string `json:"username"`
    Name     string `json:"name"`
}

user := User{Username: "john", Name: "John Doe"}
channel.SendMessage(user)

// Send with timeout
channel.SendMessage("Urgent message", rmds.Timeout(10*time.Minute))
```

### Receiving Messages

```go
channel.OnMessage(func(message *rmds.Message) {
    fmt.Printf("Channel: %s\n", message.Channel)
    fmt.Printf("Sender: %s\n", message.Sender)
    fmt.Printf("Created: %v\n", message.CreatedAt)
    fmt.Printf("Data: %s\n", string(message.Data))
    
    // Parse JSON if needed
    var user User
    if err := json.Unmarshal(message.Data, &user); err == nil {
        fmt.Printf("User: %+v\n", user)
    }
    
    // Manually acknowledge the message
    message.Ack()
})
```

### Management APIs

```go
// Get network topology
topology := connection.Mgmt.Topology()

// Get all nodes
nodes := connection.Mgmt.Nodes()

// Get all channels
channels := connection.Mgmt.Channels()

// Get channel subscribers
subscribers := connection.Mgmt.GetSubscribers("chat")

// Get statistics
stats := connection.Mgmt.Statistic()
fmt.Printf("Delivered: %d\n", stats["delivered"])
fmt.Printf("Queued: %d\n", stats["queued"])
fmt.Printf("Dropped: %d\n", stats["dropped"])

// Kick a node
connection.Mgmt.Kick("node-4")

// Kick from channel
connection.Mgmt.KickChannel("node-4")

// Purge messages
connection.Mgmt.PurgeAll()           // All messages for all nodes
connection.Mgmt.PurgeSent()          // Sent messages for all nodes
connection.Mgmt.PurgeQueue()         // Queued messages for all nodes

connection.Mgmt.PurgeAll(rmds.Self)  // All messages for current node
connection.Mgmt.PurgeSent(rmds.Self) // Sent messages for current node
connection.Mgmt.PurgeQueue(rmds.Self)// Queued messages for current node
```

### Cleanup

```go
// Unsubscribe from a channel
channel.Unsubscribe()

// Disconnect and cleanup everything
connection.Unsubscribe()
```

## Demo Chat Application

The repository includes a demo chat application that showcases RMDS capabilities with both CLI and Web UI interfaces.

### Running the Demo

```bash
# Download and run NATS server
docker run -p 4222:4222 nats:latest

# Build the chat application
cd cmd/chat
go build -o chat

# Run multiple instances with different node IDs

# Terminal 1: Alice (writer)
./chat -node_id alice -mode writer -channel my_channel -nats localhost:4222 -web 8081

# Terminal 2: Bob (reader/writer)
./chat -node_id bob -mode rw -channel my_channel -nats localhost:4222 -web 8082

# Terminal 3: Lisa (reader)
./chat -node_id lisa -mode reader -channel my_channel -nats localhost:4222 -web 8083

# Terminal 4: Andy (reader)
./chat -node_id andy -mode reader -channel my_channel -nats localhost:4222 -web 8084
```

### Web UI

Open your browser and navigate to:
- Alice: http://localhost:8081
- Bob: http://localhost:8082
- Lisa: http://localhost:8083
- Andy: http://localhost:8084

### CLI Commands

- Type messages to send them to the channel
- `/stats` - Show statistics
- `/nodes` - Show connected nodes
- `/topology` - Show network topology
- `/quit` - Exit the application

## Testing Scenarios

### Scenario 1: Normal Operation
1. Run 4 agents: alice (writer), bob, lisa, andy (receivers)
2. Send messages from alice
3. All receivers should receive messages

### Scenario 2: NATS Outage During Operation
1. Run 4 agents
2. Send message 1 from alice (bob receives)
3. Stop NATS server
4. Send messages 2 and 3 from alice
5. Start NATS server after 30 seconds
6. Bob should receive messages 2 and 3

### Scenario 3: Starting Without NATS
1. Stop NATS server
2. Run 4 agents
3. Send messages 1 and 2 from alice
4. Start NATS server after 30 seconds
5. Bob should receive messages 1 and 2

### Scenario 4: High Volume
1. Send 1000 messages in sequence
2. Receivers should receive all messages in order

## Architecture Details

### Message Flow
1. Sender creates a message and assigns it a unique ID
2. Message is persisted to SQLite with "pending" status
3. Message is queued for delivery to each receiver
4. Delivery worker picks up the message and sends via NATS
5. Receiver processes the message and sends ACK
6. Sender marks message as "acknowledged" upon receiving ACK
7. If no ACK is received within timeout, message is requeued

### Persistence Layer
- Each node maintains its own SQLite database
- Messages are stored with metadata (sender, receiver, timestamp, etc.)
- Automatic cleanup of old delivered messages
- Indexes on status, receiver, and expiry for fast queries

### Node Discovery
- Nodes broadcast keepalive messages every 10 seconds
- Each keepalive contains node ID and subscribed channels
- Nodes are marked offline after missing 3 keepalives (30 seconds)
- Discovery information is used for routing messages

### Compression
- Optional Snappy compression for message payloads
- Reduces bandwidth usage for large messages
- Transparent compression/decompression

## Performance Considerations

- Messages are processed one at a time per receiver to maintain order
- SQLite WAL mode for better concurrent access
- Connection pooling for NATS connections
- Batch processing of pending messages during recovery
- Configurable limits to prevent memory exhaustion

## License

MIT License - see LICENSE file for details

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.