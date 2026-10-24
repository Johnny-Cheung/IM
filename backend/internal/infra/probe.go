package infra

import (
	"context"
	"fmt"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/config"
	"github.com/rabbitmq/amqp091-go"
)

func RedisRoundTrip(ctx context.Context, config config.RedisConfig) error {
	client, err := newRedisClient(config)
	if err != nil {
		return err
	}
	defer client.Close()

	key := fmt.Sprintf("im:v11:infra-check:%d", time.Now().UnixNano())
	const value = "ok"
	if err := client.Set(ctx, key, value, time.Minute).Err(); err != nil {
		return fmt.Errorf("Redis SET: %w", err)
	}
	got, err := client.Get(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("Redis GET: %w", err)
	}
	if got != value {
		return fmt.Errorf("Redis GET returned %q", got)
	}
	if err := client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("Redis DEL: %w", err)
	}
	return nil
}

func RabbitRoundTrip(ctx context.Context, config config.RabbitMQConfig) error {
	connection, err := dialRabbit(config)
	if err != nil {
		return fmt.Errorf("RabbitMQ connect: %w", err)
	}
	defer connection.Close()

	channel, err := connection.Channel()
	if err != nil {
		return fmt.Errorf("RabbitMQ channel: %w", err)
	}
	defer channel.Close()

	queue, err := channel.QueueDeclare(
		"",
		false,
		true,
		true,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("RabbitMQ queue: %w", err)
	}
	if err := channel.Confirm(false); err != nil {
		return fmt.Errorf("RabbitMQ confirm: %w", err)
	}
	confirms := channel.NotifyPublish(make(chan amqp091.Confirmation, 1))
	body := []byte("im-v11-infra-check")
	if err := channel.PublishWithContext(ctx, "", queue.Name, false, false, amqp091.Publishing{
		ContentType: "text/plain",
		Body:        body,
	}); err != nil {
		return fmt.Errorf("RabbitMQ publish: %w", err)
	}
	select {
	case confirmation := <-confirms:
		if !confirmation.Ack {
			return fmt.Errorf("RabbitMQ publish was rejected")
		}
	case <-ctx.Done():
		return ctx.Err()
	}

	consumer, err := channel.Consume(queue.Name, "", false, true, false, false, nil)
	if err != nil {
		return fmt.Errorf("RabbitMQ consume: %w", err)
	}
	select {
	case delivery := <-consumer:
		if string(delivery.Body) != string(body) {
			return fmt.Errorf("RabbitMQ body mismatch")
		}
		if err := delivery.Ack(false); err != nil {
			return fmt.Errorf("RabbitMQ ack: %w", err)
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
