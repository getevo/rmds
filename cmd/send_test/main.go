package main

import (
	"fmt"
	"log"
	"time"
	"github.com/getevo/rmds"
)

func main() {
	config := rmds.DefaultConfig()
	config.NodeID = "test_sender"
	
	conn, err := rmds.New(config)
	if err != nil {
		log.Fatal("failed to create RMDS connection for send test: %v", err)
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
		log.Fatal("failed to send test message: %v", err)
	}
	
	fmt.Println("Message sent! Checking database...")
	time.Sleep(1 * time.Second)
	
	// Check what's in the database
	db, err := rmds.NewDatabase(config.GetDatabasePath())
	if err != nil {
		log.Fatal("failed to open database for send test: %v", err)
	}
	defer db.Close()

	fmt.Println("=== Database Statistics ===")
	stats, err := db.GetStatistics()
	if err != nil {
		log.Fatal("failed to get database statistics: %v", err)
	}
	
	for key, value := range stats {
		log.Printf("DEBUG: database stat - %s: %v", key, value)
		fmt.Printf("%s: %v\n", key, value)
	}
	
	fmt.Println("\n=== Pending Receivers ===")
	receivers, err := db.GetPendingReceivers()
	if err != nil {
		log.Fatal("failed to get pending receivers: %v", err)
	}
	
	log.Printf("DEBUG: found %d receivers with pending messages", len(receivers))
	fmt.Printf("Found %d receivers with pending messages:\n", len(receivers))
	for _, receiver := range receivers {
		messages, err := db.GetPendingMessages(receiver)
		if err != nil {
			continue
		}
		log.Printf("DEBUG: receiver %s has %d pending messages", receiver, len(messages))
		fmt.Printf("- %s: %d pending messages\n", receiver, len(messages))
		
		for i, msg := range messages {
			if i >= 3 {
				break
			}
			log.Printf("DEBUG: message %d - ID: %s, Data: %s", i+1, msg.ID, string(msg.Data))
			fmt.Printf("  %d. ID: %s, Data: %s\n", i+1, msg.ID, string(msg.Data))
		}
	}
}