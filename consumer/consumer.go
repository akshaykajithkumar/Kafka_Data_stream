package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"

	"github.com/IBM/sarama"
)

func main() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Error creating consumer: %v", err)
	}
	defer consumer.Close()

	// Example values for userID and videoID
	userID := "user123"
	videoID := "video456"

	topic := fmt.Sprintf("%s_%s_%s", "user_topic", userID, videoID)

	partitions, err := consumer.Partitions(topic)
	if err != nil {
		log.Fatalf("Error getting partitions: %v", err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	wg := &sync.WaitGroup{}

	for _, partition := range partitions {
		partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
		if err != nil {
			log.Fatalf("Error creating partition consumer: %v", err)
		}
		defer partitionConsumer.Close()

		wg.Add(1)
		go func(pc sarama.PartitionConsumer, p int32) {
			defer wg.Done()
			for {
				select {
				case msg := <-pc.Messages():
					// Process the received message (video URL in this case)
					fmt.Printf("Received message from partition %d: %s\n", p, msg.Value)
				case err := <-pc.Errors():
					log.Printf("Error: %v\n", err)
				case <-signals:
					return
				}
			}
		}(partitionConsumer, partition)
	}

	wg.Wait()
}
