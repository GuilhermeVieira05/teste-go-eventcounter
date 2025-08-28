package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

var (
	size         int
	amqpUrl      string
	amqpExchange string
	declareQueue bool
)

func init() {
	flag.IntVar(&size, "size", 20, "Quantidade de mensagens geradas")
	flag.StringVar(&amqpUrl, "amqp-url", "amqp://guest:guest@localhost:5672", "URL do RabbitMQ")
	flag.StringVar(&amqpExchange, "amqp-exchange", "user-events", "Exchange do RabbitMQ")
	flag.BoolVar(&declareQueue, "amqp-declare-queue", false, "Declare fila no RabbitMQ")
	flag.Parse()
}

var (
	eventCounter = NewEventCounter()
	msgCh = make(chan string, size)
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	createdCh := make(chan amqp091.Delivery, 10)
    updatedCh := make(chan amqp091.Delivery, 10)
    deletedCh := make(chan amqp091.Delivery, 10)

	var wg sync.WaitGroup

	wg.Add(1)
	go startHandler(ctx, "created", createdCh, &wg)

	wg.Add(1)
	go startHandler(ctx, "updated", updatedCh, &wg)

	wg.Add(1)
	go startHandler(ctx, "deleted", deletedCh, &wg)

	wg.Add(1)
	go func(){
		defer wg.Done()
		go ConsumeMessage(ctx, eventCounter, createdCh, updatedCh, deletedCh)
	}()

	go func(){
		inactivity := 5 * time.Second
		timer := time.NewTimer(inactivity)
		for {
			select {
			case <- msgCh:
				timer.Reset(inactivity)
			case <- timer.C:
				log.Println("Inatividade detectada, encerrando consumer")
				cancel()
				return
			case <- ctx.Done():
				return
			}
		}
	}()

	wg.Wait()

	if err := WriteCountersToJSON(eventCounter); err != nil {
		log.Fatal(err)
	}
	log.Println("Consumer finalizado")
}


func WriteCountersToJSON(counter *EventCounter) error {
    data := counter.Snapshot()
    for eventType, users := range data {
        filename := eventType + ".json"
        content, err := json.MarshalIndent(users, "", "  ")
        if err != nil {
            return err
        }
        if err := os.WriteFile(filename, content, 0644); err != nil {
            return err
        }
        log.Printf("Arquivo salvo: %s", filename)
    }
    return nil
}


