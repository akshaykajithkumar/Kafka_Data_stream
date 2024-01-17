package main

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func createTopicIfNotExists(admin sarama.ClusterAdmin, topic string) error {
	// Check if the topic exists
	topics, err := admin.ListTopics()
	if err != nil {
		return err
	}

	if _, exists := topics[topic]; exists {
		// Topic already exists
		return nil
	}

	// Topic doesn't exist, create it
	config := sarama.NewConfig()
	config.Admin.Retry.Max = 5

	err = admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	if err != nil {
		return err
	}

	// Wait for the topic to be created
	for i := 0; i < 30; i++ {
		topics, err := admin.ListTopics()
		if err != nil {
			return err
		}

		if _, exists := topics[topic]; exists {
			// Topic created successfully
			return nil
		}

		time.Sleep(1 * time.Second)
	}

	return fmt.Errorf("timeout waiting for topic creation")
}

func main() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	admin, err := sarama.NewClusterAdmin([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatal(err)
	}
	defer admin.Close()

	// Example values for userID, videoID, and videoURL
	userID := "user123"
	videoID := "video456"
	videoURL := "http://example.com/video456"

	topic := fmt.Sprintf("%s_%s_%s", "user_topic", userID, videoID)

	// Check if the topic exists, create it if not
	if err := createTopicIfNotExists(admin, topic); err != nil {
		log.Fatalf("Error creating topic: %v", err)
	}

	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(videoURL),
	}

	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Produced message to topic %s (partition %d, offset %d)\n", topic, partition, offset)
}
