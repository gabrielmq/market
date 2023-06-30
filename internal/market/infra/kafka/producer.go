package kafka

import ckafka "github.com/confluentinc/confluent-kafka-go/kafka"

type KafkaProducer struct {
	ConfigMap *ckafka.ConfigMap
}

func NewKafkaProducer(configMap *ckafka.ConfigMap) *KafkaProducer {
	return &KafkaProducer{
		ConfigMap: configMap,
	}
}

func (k *KafkaProducer) Publish(msg interface{}, key []byte, topic string) error {
	producer, err := ckafka.NewProducer(k.ConfigMap)
	if err != nil {
		return err
	}

	message := &ckafka.Message{
		TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: ckafka.PartitionAny},
		Key:            key,
		Value:          msg.([]byte),
	}

	if err := producer.Produce(message, nil); err != nil {
		return err
	}
	return nil
}
