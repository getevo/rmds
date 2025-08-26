package main

import (
	"fmt"
	"log"
	"time"
	"rmds"
)

func main() {
	config := rmds.DefaultConfig()
	config.NodeID = "debug_sender"
	
	conn, err := rmds.New(config)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Unsubscribe()
	
	// Join channel in write mode
	ch := conn.Join("test", rmds.WriteOnly)
	
	// Wait for discovery
	fmt.Println("Waiting 5 seconds for node discovery...")
	time.Sleep(5 * time.Second)
	
	// Check discovered nodes
	nodes := conn.GetDiscovery().GetChannelNodes("test")
	fmt.Printf("Discovered %d nodes: %v\n", len(nodes), nodes)
	
	// Send one message and track it carefully
	fmt.Println("\n=== SENDING SINGLE MESSAGE ===")
	err = ch.SendMessage("CRITICAL TEST MESSAGE - must reach ALL receivers")
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println("Message sent! Checking database immediately...")
	
	// Check database RIGHT after sending
	db, err := rmds.NewDatabase(config.GetDatabasePath())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Show all messages in database
	fmt.Println("\n=== DATABASE STATE AFTER SENDING ===")
	stats, _ := db.GetStatistics()
	for key, value := range stats {
		fmt.Printf("%s: %v\n", key, value)
	}
	
	// Show pending receivers
	receivers, _ := db.GetPendingReceivers()
	fmt.Printf("\nPending receivers: %v\n", receivers)
	
	for _, receiver := range receivers {
		messages, _ := db.GetPendingMessages(receiver)
		fmt.Printf("Receiver %s has %d pending messages:\n", receiver, len(messages))
		for i, msg := range messages {
			fmt.Printf("  %d. ID: %s, Status: %s, Data: %s\n", i+1, msg.ID, msg.Status, string(msg.Data))
		}
	}
	
	// Wait and monitor changes
	fmt.Println("\n=== MONITORING PROCESSING (10 seconds) ===")
	for i := 0; i < 10; i++ {
		time.Sleep(1 * time.Second)
		
		newStats, _ := db.GetStatistics()
		fmt.Printf("After %ds - Pending: %v, Sent: %v, Acked: %v\n", 
			i+1, newStats["pending_messages"], newStats["sent_messages"], newStats["acknowledged_messages"])
	}
	
	fmt.Println("\n=== FINAL STATE ===")
	finalStats, _ := db.GetStatistics()
	for key, value := range finalStats {
		fmt.Printf("%s: %v\n", key, value)
	}
}