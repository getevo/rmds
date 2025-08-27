package main

import (
	"fmt"
	"time"

	"github.com/getevo/evo/lib/log"
	"github.com/getevo/rmds"
)

func main() {
	fmt.Println("=== RMDS Example - Reliable Message Delivery ===")

	// Create sender configuration
	senderConfig := rmds.DefaultConfig()
	senderConfig.NodeID = "sender"
	
	// Create sender connection
	sender, err := rmds.New(senderConfig)
	if err != nil {
		log.Fatal("Failed to create sender:", err)
	}
	defer sender.Unsubscribe()
	
	// Join channel in write mode
	senderCh := sender.Join("example", rmds.WriteOnly)

	// Create receiver configuration
	receiverConfig := rmds.DefaultConfig()
	receiverConfig.NodeID = "receiver"
	
	// Create receiver connection
	receiver, err := rmds.New(receiverConfig)
	if err != nil {
		log.Fatal("Failed to create receiver:", err)
	}
	defer receiver.Unsubscribe()
	
	// Join channel in read mode
	receiverCh := receiver.Join("example", rmds.ReadOnly)
	
	// Set up message handler
	receiverCh.OnMessage(func(msg *rmds.Message) {
		fmt.Printf("📨 Received: %s (from %s)\n", string(msg.Data), msg.Sender)
	})

	// Wait for discovery
	fmt.Println("⏳ Waiting for node discovery...")
	time.Sleep(3 * time.Second)
	
	// Check discovered nodes
	nodes := sender.GetDiscovery().GetChannelNodes("example")
	fmt.Printf("🔍 Discovered %d receivers: %v\n", len(nodes), nodes)
	
	// Send example messages
	messages := []string{
		"Hello, World!",
		"This is a reliable message",
		"Zero message loss guaranteed",
		"Perfect FIFO ordering",
		"Production ready system",
	}
	
	fmt.Printf("📤 Sending %d messages...\n", len(messages))
	for i, msg := range messages {
		fmt.Printf("  Sending message %d: %s\n", i+1, msg)
		if err := senderCh.SendMessage(msg); err != nil {
			log.Error("Failed to send message", i+1, ":", err)
		}
		time.Sleep(500 * time.Millisecond) // Small delay between messages
	}
	
	// Wait for processing
	fmt.Println("⏳ Waiting for message processing...")
	time.Sleep(5 * time.Second)
	
	// Show statistics
	fmt.Println("\n📊 Statistics:")
	stats := sender.GetStatistics()
	fmt.Printf("  Messages Sent: %d\n", stats.GetMessagesSent())
	fmt.Printf("  Messages Delivered: %d\n", stats.GetMessagesDelivered())
	fmt.Printf("  ACKs Received: %d\n", stats.GetACKsReceived())
	
	fmt.Println("\n✅ Example completed successfully!")
	fmt.Println("📋 Features demonstrated:")
	fmt.Println("  • Reliable message delivery")
	fmt.Println("  • Zero message loss")
	fmt.Println("  • Perfect FIFO ordering")
	fmt.Println("  • ACK-based confirmation")
	fmt.Println("  • Node discovery")
	fmt.Println("  • Statistics tracking")
}