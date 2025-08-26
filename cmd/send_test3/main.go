package main

import (
	"fmt"
	"log"
	"time"
	"rmds"
)

func main() {
	config := rmds.DefaultConfig()
	config.NodeID = "test_sender3"
	
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
	
	// Send multiple messages
	for i := 1; i <= 5; i++ {
		fmt.Printf("Sending test message %d...\n", i)
		err = ch.SendMessage(fmt.Sprintf("Test message #%d for consumer debugging", i))
		if err != nil {
			log.Fatal(err)
		}
		time.Sleep(1 * time.Second) // Small delay between messages
	}
	
	fmt.Println("All messages sent! Waiting 5 seconds for processing...")
	time.Sleep(5 * time.Second)
	
	// Check final database state
	db, err := rmds.NewDatabase(config.GetDatabasePath())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fmt.Println("=== Final Database Statistics ===")
	stats, err := db.GetStatistics()
	if err != nil {
		log.Fatal(err)
	}
	
	for key, value := range stats {
		fmt.Printf("%s: %v\n", key, value)
	}
}