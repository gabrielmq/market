package main

import (
	"encoding/json"
	"fmt"
	"sync"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gabrielmq/market/internal/market/dto"
	"github.com/gabrielmq/market/internal/market/entity"
	"github.com/gabrielmq/market/internal/market/infra/kafka"
	"github.com/gabrielmq/market/internal/market/transformer"
)

func main() {
	ordersIn := make(chan *entity.Order)
	ordersOut := make(chan *entity.Order)

	wg := &sync.WaitGroup{}
	defer wg.Wait()

	kafkaMsgCh := make(chan *ckafka.Message)
	configMapConsumer := &ckafka.ConfigMap{
		"bootstrap.servers": "host.docker.internal:9094",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	}

	consumer := kafka.NewKafkaConsumer(configMapConsumer, []string{"input"})
	producer := kafka.NewKafkaProducer(&ckafka.ConfigMap{
		"bootstrap.servers": "host.docker.internal:9094",
	})

	go consumer.Consume(kafkaMsgCh)

	book := entity.NewBook(ordersIn, ordersOut, wg)
	go book.Trade()

	go func() {
		for msg := range kafkaMsgCh {
			wg.Add(1)
			tradeInput := dto.TradeInput{}
			if err := json.Unmarshal(msg.Value, &tradeInput); err != nil {
				panic(err)
			}
			ordersIn <- transformer.TransformInput(tradeInput)
		}
	}()

	for res := range ordersOut {
		output, err := json.Marshal(transformer.TransformOutput(res))
		if err != nil {
			panic(err)
		}

		err = producer.Publish(output, []byte("orders"), "output")
		if err != nil {
			fmt.Println(err)
		}
	}
}
