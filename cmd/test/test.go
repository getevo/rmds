package main

import (
	"fmt"
	"log"
	"time"

	"github.com/getevo/rmds"
)

func main() {
	// Create Alice (writer)
	config := rmds.DefaultConfig()
	config.NodeID = "alice"
	config.StoragePath = "./alice.db"
	config.NATSServers = []string{"nats://localhost:4222"}

	conn, err := rmds.New(config)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Unsubscribe()

	ch := conn.Join("test", rmds.WriteOnly)

	fmt.Println("Alice connected. Waiting 2 seconds for topology to load...")
	time.Sleep(2 * time.Second)

	// Send messages while NATS is offline
	messages := []string{
		"Hello from Alice offline!",
		"Message 2 while NATS is down",
		"Message 3 - should be queued in SQLite",
	}

	for i, msg := range messages {
		fmt.Printf("Sending message %d: %s\n", i+1, msg)
		err = ch.SendMessage([]byte(msg))
		if err != nil {
			fmt.Printf("Error sending message: %v\n", err)
		} else {
			fmt.Printf("Message %d queued successfully\n", i+1)
		}
		time.Sleep(1 * time.Second)
	}

	fmt.Println("\nMessages sent. They are now stored in SQLite.")
	fmt.Println("When NATS comes back online, they will be delivered to Bob and David.")
	fmt.Println("\nPress Enter to exit...")
	fmt.Scanln()
}