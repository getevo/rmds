package main

import (
	"fmt"
	"log"
	"os"
	"rmds"
)

func main() {
	var dbPath string
	if len(os.Args) > 1 {
		dbPath = os.Args[1]
	} else {
		dbPath = "../../alice.db"
	}
	
	db, err := rmds.NewDatabase(dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fmt.Println("=== Pending Messages by Receiver ===")
	
	// Get all pending receivers
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
		
		// Show first few messages
		for i, msg := range messages {
			if i >= 3 {
				fmt.Printf("  ... and %d more\n", len(messages)-3)
				break
			}
			fmt.Printf("  %d. ID: %s, Created: %s\n", i+1, msg.ID, msg.CreatedAt.Format("15:04:05"))
		}
	}
	
	fmt.Println("\n=== All Message Statistics ===")
	stats, err := db.GetStatistics()
	if err != nil {
		log.Fatal(err)
	}
	
	for key, value := range stats {
		fmt.Printf("%s: %v\n", key, value)
	}
}