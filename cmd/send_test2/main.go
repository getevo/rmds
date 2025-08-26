package main

import (
	"fmt"
	"log"
	"time"
	"rmds"
)

func main() {
	config := rmds.DefaultConfig()
	config.NodeID = "test_sender2"
	
	conn, err := rmds.New(config)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Unsubscribe()
	
	// Join channel in write mode
	ch := conn.Join("test", rmds.WriteOnly)
	
	// Wait longer for discovery
	fmt.Println("Waiting 10 seconds for node discovery...")
	for i := 0; i < 10; i++ {
		time.Sleep(1 * time.Second)
		
		nodes := conn.GetDiscovery().GetChannelNodes("test")
		fmt.Printf("After %d seconds - Found %d nodes on channel 'test': %v\n", i+1, len(nodes), nodes)
		
		if len(nodes) > 1 { // More than just ourselves
			break
		}
	}
	
	// Send a message
	fmt.Println("Sending test message...")
	err = ch.SendMessage("Test message for Bob consumer debugging - attempt 2")
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println("Message sent! Waiting 2 seconds then checking database...")
	time.Sleep(2 * time.Second)
	
	// Check what's in the database
	db, err := rmds.NewDatabase(config.GetDatabasePath())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fmt.Println("=== Database Statistics ===")
	stats, err := db.GetStatistics()
	if err != nil {
		log.Fatal(err)
	}
	
	for key, value := range stats {
		fmt.Printf("%s: %v\n", key, value)
	}
	
	fmt.Println("\n=== Pending Receivers ===")
	receivers, err := db.GetPendingReceivers()
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Printf("Found %d receivers with pending messages:\n", len(receivers))
	for _, receiver := range receivers {
		messages, err := db.GetPendingMessages(receiver)
		if err != nil {
			continue
		}
		fmt.Printf("- %s: %d pending messages\n", receiver, len(messages))
	}
}