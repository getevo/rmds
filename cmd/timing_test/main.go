package main

import (
	"fmt"
	"time"

	"github.com/getevo/evo/lib/log"
	"github.com/getevo/rmds"
)

func main() {
	config := rmds.DefaultConfig()
	config.NodeID = "timing_test"
	
	conn, err := rmds.New(config)
	if err != nil {
		log.Fatal("Failed to create connection:", err)
	}
	defer conn.Unsubscribe()
	
	// Join channel in write mode
	ch := conn.Join("test", rmds.WriteOnly)
	
	fmt.Println("=== DISCOVERY TIMING TEST ===")
	
	// Monitor discovery over time
	for i := 0; i < 15; i++ {
		time.Sleep(1 * time.Second)
		
		nodes := conn.GetDiscovery().GetChannelNodes("test")
		fmt.Printf("After %2ds - Discovered %d nodes: %v\n", i+1, len(nodes), nodes)
		
		// Send message at different times to see when it starts working
		if i == 2 { // 3 seconds
			fmt.Println("  -> Sending message at 3s...")
			err = ch.SendMessage(fmt.Sprintf("Message sent at %ds", i+1))
			if err != nil {
				fmt.Printf("  -> Error: %v\n", err)
			}
		}
		
		if i == 5 { // 6 seconds  
			fmt.Println("  -> Sending message at 6s...")
			err = ch.SendMessage(fmt.Sprintf("Message sent at %ds", i+1))
			if err != nil {
				fmt.Printf("  -> Error: %v\n", err)
			}
		}
		
		if i == 10 { // 11 seconds
			fmt.Println("  -> Sending message at 11s...")
			err = ch.SendMessage(fmt.Sprintf("Message sent at %ds", i+1))
			if err != nil {
				fmt.Printf("  -> Error: %v\n", err)
			}
		}
	}
	
	// Final database check
	db, err := rmds.NewDatabase(config.GetDatabasePath())
	if err != nil {
		log.Fatal("Failed to create connection:", err)
	}
	defer db.Close()

	fmt.Println("\n=== FINAL DATABASE STATE ===")
	stats, _ := db.GetStatistics()
	for key, value := range stats {
		fmt.Printf("%s: %v\n", key, value)
	}
	
	receivers, _ := db.GetPendingReceivers()
	fmt.Printf("Pending receivers: %v\n", receivers)
}