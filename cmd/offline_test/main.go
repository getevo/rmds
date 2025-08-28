package main

import (
	"fmt"
	"time"

	"github.com/getevo/evo/lib/log"
	"github.com/getevo/rmds"
)

func main() {
	fmt.Println("=== OFFLINE RECEIVER TEST ===")
	fmt.Println("Testing message delivery when receiver goes offline and comes back")

	// Create Writer A
	writerConfig := rmds.DefaultConfig()
	writerConfig.NodeID = "writer_A"
	writerConfig.EnableDebugLogging = true
	
	writerA, err := rmds.New(writerConfig)
	if err != nil {
		log.Fatal("Failed to create writer A:", err)
	}
	defer writerA.Unsubscribe()
	
	writerCh := writerA.Join("test_offline", rmds.WriteOnly)

	// Create Reader B  
	readerConfig := rmds.DefaultConfig()
	readerConfig.NodeID = "reader_B"
	readerConfig.EnableDebugLogging = true
	
	var readerB *rmds.Connection
	var readerCh *rmds.Channel
	var receivedMessages []string
	
	startReaderB := func() {
		fmt.Println("📱 Starting Reader B...")
		readerB, err = rmds.New(readerConfig)
		if err != nil {
			log.Fatal("Failed to create reader B:", err)
		}
		
		readerCh = readerB.Join("test_offline", rmds.ReadOnly)
		readerCh.OnMessage(func(msg *rmds.Message) {
			receivedMessages = append(receivedMessages, string(msg.Data))
			fmt.Printf("📨 Reader B received: %s\n", string(msg.Data))
			msg.Ack()
		})
	}
	
	stopReaderB := func() {
		if readerB != nil {
			fmt.Println("🛑 Stopping Reader B...")
			readerB.Unsubscribe()
			readerB = nil
		}
	}

	// Phase 1: Both online, send initial messages
	fmt.Println("\n=== Phase 1: Both Writer A and Reader B online ===")
	startReaderB()
	
	// Wait for discovery
	fmt.Println("⏳ Waiting for node discovery...")
	time.Sleep(5 * time.Second)
	
	// Check discovered nodes
	nodes := writerA.GetDiscovery().GetChannelNodes("test_offline")
	fmt.Printf("🔍 Writer A discovered %d readers: %v\n", len(nodes), nodes)
	
	// Send initial messages (should be received immediately)
	fmt.Println("📤 Sending initial messages (Reader B online)...")
	for i := 1; i <= 3; i++ {
		msg := fmt.Sprintf("Initial message %d", i)
		fmt.Printf("  Sending: %s\n", msg)
		if err := writerCh.SendMessage(msg); err != nil {
			log.Error("Failed to send message:", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	
	// Wait for delivery
	time.Sleep(2 * time.Second)
	fmt.Printf("✅ Phase 1 complete. Reader B received %d messages: %v\n", len(receivedMessages), receivedMessages)

	// Phase 2: Kill Reader B, send messages while offline
	fmt.Println("\n=== Phase 2: Reader B goes offline, Writer A continues sending ===")
	stopReaderB()
	
	// Wait a moment for disconnection to be detected
	time.Sleep(2 * time.Second)
	
	// Send messages while Reader B is offline (these should be queued)
	fmt.Println("📤 Sending messages while Reader B is OFFLINE...")
	offlineMessages := []string{}
	for i := 4; i <= 8; i++ {
		msg := fmt.Sprintf("Offline message %d", i)
		offlineMessages = append(offlineMessages, msg)
		fmt.Printf("  Sending: %s\n", msg)
		if err := writerCh.SendMessage(msg); err != nil {
			log.Error("Failed to send message:", err)
		}
		time.Sleep(1 * time.Second)
	}
	
	fmt.Printf("📦 Sent %d messages while Reader B was offline: %v\n", len(offlineMessages), offlineMessages)
	
	// Check database for queued messages
	fmt.Println("\n🗄️  Checking database for queued messages...")
	db, _ := rmds.NewDatabase(writerConfig.GetDatabasePath())
	defer db.Close()
	
	pendingReceivers, _ := db.GetPendingReceivers()
	fmt.Printf("📋 Found %d receivers with pending messages: %v\n", len(pendingReceivers), pendingReceivers)
	
	for _, receiver := range pendingReceivers {
		pendingMsgs, _ := db.GetPendingMessages(receiver)
		fmt.Printf("📬 Receiver %s has %d pending messages\n", receiver, len(pendingMsgs))
	}

	// Phase 3: Restart Reader B, it should receive all offline messages
	fmt.Println("\n=== Phase 3: Restart Reader B - should receive ALL offline messages ===")
	
	beforeCount := len(receivedMessages)
	fmt.Printf("📊 Reader B had received %d messages before going offline\n", beforeCount)
	
	// Restart Reader B
	startReaderB()
	defer stopReaderB()
	
	// Wait for reconnection and message delivery
	fmt.Println("⏳ Waiting for Reader B to reconnect and receive offline messages...")
	time.Sleep(10 * time.Second)
	
	// Check results
	afterCount := len(receivedMessages)
	newMessages := afterCount - beforeCount
	
	fmt.Println("\n=== RESULTS ===")
	fmt.Printf("📊 Reader B received %d new messages after reconnecting\n", newMessages)
	fmt.Printf("📊 Expected: %d offline messages\n", len(offlineMessages))
	fmt.Printf("📋 All received messages: %v\n", receivedMessages)
	
	if newMessages == len(offlineMessages) {
		fmt.Println("✅ SUCCESS: Reader B received ALL offline messages!")
	} else {
		fmt.Printf("❌ FAILED: Reader B only received %d/%d offline messages\n", newMessages, len(offlineMessages))
		
		// Show detailed database state for debugging
		fmt.Println("\n🔍 DEBUG: Final database state...")
		stats, _ := db.GetStatistics()
		for key, value := range stats {
			fmt.Printf("  %s: %v\n", key, value)
		}
	}
	
	fmt.Println("\n🏁 Test completed")
}