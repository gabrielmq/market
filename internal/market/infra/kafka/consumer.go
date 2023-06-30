package kafka

import ckafka "github.com/confluentinc/confluent-kafka-go/kafka"

type KafkaConsumer struct {
	ConfigMap *ckafka.ConfigMap
	Topics    []string
}

func NewKafkaConsumer(configMap *ckafka.ConfigMap, topics []string) *KafkaConsumer {
	return &KafkaConsumer{
		ConfigMap: configMap,
		Topics:    topics,
	}
}

func (k *KafkaConsumer) Consume(msgChan chan *ckafka.Message) {
	consumer, err := ckafka.NewConsumer(k.ConfigMap)
	if err != nil {
		panic(err)
	}

	if err := consumer.SubscribeTopics(k.Topics, nil); err != nil {
		panic(err)
	}

	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			msgChan <- msg
		}
	}
}
