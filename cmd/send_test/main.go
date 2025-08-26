package main

import (
	"fmt"
	"log"
	"time"
	"rmds"
)

func main() {
	config := rmds.DefaultConfig()
	config.NodeID = "test_sender"
	
	conn, err := rmds.New(config)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Unsubscribe()
	
	// Join channel in write mode
	ch := conn.Join("test", rmds.WriteOnly)
	
	// Wait for discovery
	fmt.Println("Waiting 3 seconds for node discovery...")
	time.Sleep(3 * time.Second)
	
	// Send a message
	fmt.Println("Sending test message...")
	err = ch.SendMessage("Test message for Bob consumer debugging")
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println("Message sent! Checking database...")
	time.Sleep(1 * time.Second)
	
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
		
		for i, msg := range messages {
			if i >= 3 {
				break
			}
			fmt.Printf("  %d. ID: %s, Data: %s\n", i+1, msg.ID, string(msg.Data))
		}
	}
}