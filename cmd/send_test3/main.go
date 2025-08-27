package main

import (
	"fmt"
	"log"
	"time"
	"github.com/getevo/rmds"
)

func main() {
	config := rmds.DefaultConfig()
	config.NodeID = "test_sender3"
	
	conn, err := rmds.New(config)
	if err != nil {
		log.Fatal("failed to create RMDS connection for send test 3: %v", err)
	}
	defer conn.Unsubscribe()
	
	// Join channel in write mode
	ch := conn.Join("test", rmds.WriteOnly)
	
	// Wait for discovery
	fmt.Println("Waiting 5 seconds for node discovery...")
	time.Sleep(5 * time.Second)
	
	// Send multiple messages
	for i := 1; i <= 5; i++ {
		log.Printf("DEBUG: sending test message %d", i)
		fmt.Printf("Sending test message %d...\n", i)
		err = ch.SendMessage(fmt.Sprintf("Test message #%d for consumer debugging", i))
		if err != nil {
			log.Fatal("failed to send test message #%d: %v", i, err)
		}
		time.Sleep(1 * time.Second) // Small delay between messages
	}
	
	fmt.Println("All messages sent! Waiting 5 seconds for processing...")
	time.Sleep(5 * time.Second)
	
	// Check final database state
	db, err := rmds.NewDatabase(config.GetDatabasePath())
	if err != nil {
		log.Fatal("failed to open database for send test 3: %v", err)
	}
	defer db.Close()

	fmt.Println("=== Final Database Statistics ===")
	stats, err := db.GetStatistics()
	if err != nil {
		log.Fatal("failed to get database statistics: %v", err)
	}
	
	for key, value := range stats {
		log.Printf("DEBUG: database stat - %s: %v", key, value)
		fmt.Printf("%s: %v\n", key, value)
	}
}