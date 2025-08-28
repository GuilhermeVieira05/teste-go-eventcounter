package main

import (
	"context"
	"log"
	"sync"

	amqp091 "github.com/rabbitmq/amqp091-go"
)


func routeDelivery(delivery amqp091.Delivery, createdCh, updatedCh, deletedCh chan amqp091.Delivery) {
    switch {
    case matchRoutingKey(delivery.RoutingKey, "*.event.created"):
        createdCh <- delivery
    case matchRoutingKey(delivery.RoutingKey, "*.event.updated"):
        updatedCh <- delivery
    case matchRoutingKey(delivery.RoutingKey, "*.event.deleted"):
        deletedCh <- delivery
    default:
        log.Printf("RoutingKey %s não reconhecida, evento descartado", delivery.RoutingKey)
    }
}

func matchRoutingKey(routing, pattern string) bool {
    return (pattern == "*.event.created" && len(routing) > len(".event.created") && routing[len(routing)-len(".event.created"):] == ".event.created") ||
        (pattern == "*.event.updated" && len(routing) > len(".event.updated") && routing[len(routing)-len(".event.updated"):] == ".event.updated") ||
        (pattern == "*.event.deleted" && len(routing) > len(".event.deleted") && routing[len(routing)-len(".event.deleted"):] == ".event.deleted")
}

func ConsumeMessage(ctx context.Context, counter *EventCounter, 
    createdCh, updatedCh, deletedCh chan amqp091.Delivery) {
    conn, ch, err := Connection(amqpUrl)
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()
    defer ch.Close()

    if err := ch.ExchangeDeclare(
        amqpExchange, "topic", true, false, false, false, nil,
    ); err != nil {
        log.Fatal(err)
    }

    queueName := "eventcountertest" 
    if _, err := ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
        log.Fatal(err)
    }
    if err := ch.QueueBind(queueName, "#", amqpExchange, false, nil); err != nil {
        log.Fatal(err)
    }

    msgs, err := ch.Consume(
        queueName, "", true, false, false, false, nil,
    )
    if err != nil {
        log.Fatal(err)
    }

    for {
        select {
        case <-ctx.Done():
            return
        case d, ok := <-msgs:
            if !ok {
                return
            }
            routeDelivery(d, createdCh, updatedCh, deletedCh)
        }
    }
}



func Connection(ampqUrl string) (*amqp091.Connection, *amqp091.Channel, error) {
	conn, err := amqp091.Dial(amqpUrl)
	if err != nil {
		return nil, nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}

	return conn, ch, nil
}


func startHandler(ctx context.Context, eventType string, ch <-chan amqp091.Delivery, wg *sync.WaitGroup) {
    defer wg.Done()
    for {
        select {
        case <-ctx.Done():
            return
        case d, ok := <-ch:
            if !ok {
                return
            }
            userID := ParseUserID([]byte(d.RoutingKey))
            
			if userID == "" {
				continue
			}

            eventCounter.Increment(eventType, userID)
            log.Printf("Evento %s para user %s", eventType, userID)
        }
    }
}