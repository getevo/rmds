package main

import (
	"fmt"
	"gith
	"log"
	"time"
)

func main() {
	fmt.Println("=== TESTING FIXED MESSAGE STORAGE ===")

	// Start Alice, Bob, Andy receivers
	fmt.Println("Starting receivers...")
	aliceConfig := rmds.DefaultConfig()
	aliceConfig.NodeID = "alice"
	alice, _ := rmds.New(aliceConfig)
	defer alice.Unsubscribe()
	aliceCh := alice.Join("test", rmds.RW)

	bobConfig := rmds.DefaultConfig()
	bobConfig.NodeID = "bob"
	bob, _ := rmds.New(bobConfig)
	defer bob.Unsubscribe()
	bobCh := bob.Join("test", rmds.RW)

	andyConfig := rmds.DefaultConfig()
	andyConfig.NodeID = "andy"
	andy, _ := rmds.New(andyConfig)
	defer andy.Unsubscribe()
	andyCh := andy.Join("test", rmds.RW)

	// Set up message handlers to track deliveries
	var aliceMessages, bobMessages, andyMessages []string

	aliceCh.OnMessage(func(msg *rmds.Message) {
		aliceMessages = append(aliceMessages, string(msg.Data))
		fmt.Printf("ALICE received: %s\n", string(msg.Data))
		msg.Ack()
	})

	bobCh.OnMessage(func(msg *rmds.Message) {
		bobMessages = append(bobMessages, string(msg.Data))
		fmt.Printf("BOB received: %s\n", string(msg.Data))
		msg.Ack()
	})

	andyCh.OnMessage(func(msg *rmds.Message) {
		andyMessages = append(andyMessages, string(msg.Data))
		fmt.Printf("ANDY received: %s\n", string(msg.Data))
		msg.Ack()
	})

	// Start sender
	senderConfig := rmds.DefaultConfig()
	senderConfig.NodeID = "sender"
	sender, _ := rmds.New(senderConfig)
	defer sender.Unsubscribe()
	senderCh := sender.Join("test", rmds.WriteOnly)

	// Wait for discovery
	fmt.Println("Waiting for discovery...")
	time.Sleep(8 * time.Second)

	// Check discovered nodes
	nodes := sender.GetDiscovery().GetChannelNodes("test")
	log.Printf("DEBUG: sender discovered %d nodes: %v", len(nodes), nodes)
	fmt.Printf("Sender discovered %d nodes: %v\n", len(nodes), nodes)

	if len(nodes) != 3 {
		log.Printf("WARNING: Expected 3 nodes, got %d. Some receivers may not be discovered.\n", len(nodes))
	}

	// Send test messages
	testMessages := []string{
		"Test message 1 - ALL RECEIVERS MUST GET THIS",
		"Test message 2 - EVERY RECEIVER SHOULD SEE THIS",
		"Test message 3 - NO RECEIVER SHOULD MISS THIS",
	}

	log.Printf("DEBUG: sending %d test messages", len(testMessages))
	fmt.Printf("Sending %d test messages...\n", len(testMessages))
	for i, msgText := range testMessages {
		log.Printf("DEBUG: sending message %d: %s", i+1, msgText)
		fmt.Printf("Sending message %d: %s\n", i+1, msgText)
		if err := senderCh.SendMessage(msgText); err != nil {
			log.Fatal("failed to send test message %d (%s): %v", i+1, msgText, err)
		}
		time.Sleep(1 * time.Second) // Small delay between messages
	}

	// Wait for processing
	fmt.Println("Waiting for message processing...")
	time.Sleep(10 * time.Second)

	// Check results
	fmt.Println("\n=== DELIVERY RESULTS ===")
	fmt.Printf("Alice received %d messages: %v\n", len(aliceMessages), aliceMessages)
	fmt.Printf("Bob received %d messages: %v\n", len(bobMessages), bobMessages)
	fmt.Printf("Andy received %d messages: %v\n", len(andyMessages), andyMessages)

	// Verify correctness
	expectedCount := len(testMessages)
	success := true

	if len(aliceMessages) != expectedCount {
		fmt.Printf("❌ ALICE: Expected %d messages, got %d\n", expectedCount, len(aliceMessages))
		success = false
	} else {
		fmt.Printf("✅ ALICE: Received all %d messages\n", expectedCount)
	}

	if len(bobMessages) != expectedCount {
		fmt.Printf("❌ BOB: Expected %d messages, got %d\n", expectedCount, len(bobMessages))
		success = false
	} else {
		fmt.Printf("✅ BOB: Received all %d messages\n", expectedCount)
	}

	if len(andyMessages) != expectedCount {
		fmt.Printf("❌ ANDY: Expected %d messages, got %d\n", expectedCount, len(andyMessages))
		success = false
	} else {
		fmt.Printf("✅ ANDY: Received all %d messages\n", expectedCount)
	}

	// Check database state
	db, _ := rmds.NewDatabase(senderConfig.GetDatabasePath())
	defer db.Close()
	stats, _ := db.GetStatistics()

	fmt.Println("\n=== SENDER DATABASE STATS ===")
	for key, value := range stats {
		fmt.Printf("%s: %v\n", key, value)
	}

	expectedTotal := len(nodes) * len(testMessages) // 3 nodes * 3 messages = 9 total
	actualTotal := stats["pending_messages"].(int) + stats["sent_messages"].(int) + stats["acknowledged_messages"].(int)

	if actualTotal == expectedTotal {
		fmt.Printf("✅ DATABASE: Total messages = %d (expected %d)\n", actualTotal, expectedTotal)
	} else {
		fmt.Printf("❌ DATABASE: Total messages = %d (expected %d)\n", actualTotal, expectedTotal)
		success = false
	}

	if success {
		fmt.Println("\n🎉 ALL TESTS PASSED! Message delivery is working flawlessly!")
	} else {
		fmt.Println("\n💥 TESTS FAILED! There are still message delivery issues!")
	}
}
