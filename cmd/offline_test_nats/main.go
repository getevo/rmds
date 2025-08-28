package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/getevo/evo/lib/log"
	"github.com/getevo/rmds"
)

func main() {
	fmt.Println("=== OFFLINE RECEIVER TEST WITH NATS ===")
	fmt.Println("This test requires NATS server running on localhost:4222")
	fmt.Println("Run: docker run -p 4222:4222 nats:latest")
	fmt.Println("")

	// Create Writer A
	writerConfig := rmds.DefaultConfig()
	writerConfig.NodeID = "writer_A"
	writerConfig.EnableDebugLogging = true
	
	fmt.Println("🚀 Starting Writer A...")
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
	
	var receivedMessages []string
	
	fmt.Println("🚀 Starting Reader B...")
	readerB, err := rmds.New(readerConfig)
	if err != nil {
		log.Fatal("Failed to create reader B:", err)
	}
	
	readerCh := readerB.Join("test_offline", rmds.ReadOnly)
	readerCh.OnMessage(func(msg *rmds.Message) {
		receivedMessages = append(receivedMessages, string(msg.Data))
		fmt.Printf("📨 Reader B received: %s\n", string(msg.Data))
		msg.Ack()
	})

	// Phase 1: Wait for discovery
	fmt.Println("\n=== Phase 1: Waiting for node discovery ===")
	fmt.Println("⏳ Waiting 15 seconds for nodes to discover each other...")
	time.Sleep(15 * time.Second)
	
	// Check discovered nodes
	nodes := writerA.GetDiscovery().GetChannelNodes("test_offline")
	fmt.Printf("🔍 Writer A discovered %d readers: %v\n", len(nodes), nodes)
	
	if len(nodes) == 0 {
		fmt.Println("❌ ERROR: Writer A didn't discover Reader B. Check NATS connection.")
		return
	}
	
	// Send initial messages
	fmt.Println("📤 Sending initial messages while Reader B is online...")
	for i := 1; i <= 2; i++ {
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

	// Phase 2: Stop Reader B and send messages
	fmt.Println("\n=== Phase 2: Reader B goes offline ===")
	fmt.Println("🛑 Stopping Reader B (simulating network outage)...")
	readerB.Unsubscribe()
	
	// Wait for disconnection to be processed
	fmt.Println("⏳ Waiting 3 seconds for disconnection...")
	time.Sleep(3 * time.Second)
	
	// Send messages while Reader B is offline
	fmt.Println("📤 Sending messages while Reader B is OFFLINE...")
	offlineMessages := []string{}
	for i := 3; i <= 7; i++ {
		msg := fmt.Sprintf("Offline message %d", i)
		offlineMessages = append(offlineMessages, msg)
		fmt.Printf("  Sending: %s\n", msg)
		if err := writerCh.SendMessage(msg); err != nil {
			log.Error("Failed to send message:", err)
		}
		time.Sleep(1 * time.Second)
	}
	
	fmt.Printf("📦 Sent %d messages while Reader B was offline\n", len(offlineMessages))
	
	// Check what nodes Writer A can see now
	nodes = writerA.GetDiscovery().GetChannelNodes("test_offline")
	fmt.Printf("🔍 Writer A now sees %d readers: %v\n", len(nodes), nodes)

	// Check database for pending messages
	fmt.Println("\n🗄️  Checking Writer A's database for queued messages...")
	db, _ := rmds.NewDatabase(writerConfig.GetDatabasePath())
	defer db.Close()
	
	pendingReceivers, _ := db.GetPendingReceivers()
	fmt.Printf("📋 Found %d receivers with pending messages: %v\n", len(pendingReceivers), pendingReceivers)
	
	for _, receiver := range pendingReceivers {
		pendingMsgs, _ := db.GetPendingMessages(receiver)
		fmt.Printf("📬 Receiver %s has %d pending messages\n", receiver, len(pendingMsgs))
		
		// Show first few pending messages
		for i, pmsg := range pendingMsgs {
			if i >= 3 { break }
			fmt.Printf("  - Message %d: %s (Status: %s)\n", i+1, string(pmsg.Data), pmsg.Status)
		}
	}

	// Phase 3: Restart Reader B
	fmt.Println("\n=== Phase 3: Reader B comes back online ===")
	beforeCount := len(receivedMessages)
	fmt.Printf("📊 Reader B had %d messages before going offline\n", beforeCount)
	
	fmt.Println("🚀 Restarting Reader B...")
	readerB, err = rmds.New(readerConfig)
	if err != nil {
		log.Fatal("Failed to restart reader B:", err)
	}
	defer readerB.Unsubscribe()
	
	readerCh = readerB.Join("test_offline", rmds.ReadOnly)
	readerCh.OnMessage(func(msg *rmds.Message) {
		receivedMessages = append(receivedMessages, string(msg.Data))
		fmt.Printf("📨 Reader B received: %s\n", string(msg.Data))
		msg.Ack()
	})
	
	// Wait for reconnection and message delivery
	fmt.Println("⏳ Waiting 15 seconds for Reader B to reconnect and receive offline messages...")
	time.Sleep(15 * time.Second)
	
	// Final results
	afterCount := len(receivedMessages)
	newMessages := afterCount - beforeCount
	
	fmt.Println("\n=== FINAL RESULTS ===")
	fmt.Printf("📊 Reader B received %d new messages after reconnecting\n", newMessages)
	fmt.Printf("📊 Expected: %d offline messages\n", len(offlineMessages))
	fmt.Printf("📋 All received messages: %v\n", receivedMessages)
	
	// Check final database state
	finalStats, _ := db.GetStatistics()
	fmt.Println("\n📊 Final database statistics:")
	for key, value := range finalStats {
		fmt.Printf("  %s: %v\n", key, value)
	}
	
	if newMessages == len(offlineMessages) {
		fmt.Println("\n✅ SUCCESS: Reader B received ALL offline messages!")
	} else {
		fmt.Printf("\n❌ FAILED: Reader B only received %d/%d offline messages\n", newMessages, len(offlineMessages))
		
		// Show what's still pending
		finalPending, _ := db.GetPendingReceivers()
		fmt.Printf("🔍 Receivers with still-pending messages: %v\n", finalPending)
	}
	
	fmt.Println("\n🏁 Test completed")
	fmt.Println("Press Ctrl+C to exit...")
	
	// Wait for interrupt
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
}