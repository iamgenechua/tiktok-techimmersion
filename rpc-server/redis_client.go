package main

import (
	"context"
	"encoding/json"
	"github.com/go-redis/redis/v8"
)

type redisClient struct {
	cli *redis.Client
}

type Message struct {
	Sender    string `json:"sender"`
	Message   string `json:"message"`
	Timestamp int64  `json:"timestamp"`
}

func (c *redisClient) InitClient(ctx context.Context, address, password string) error {
	r := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password,
		DB:       0,
	})
	_, err := r.Ping(ctx).Result()

	if err != nil {
		return err
	}

	c.cli = r
	return nil
}

func (c *redisClient) SaveMessage(ctx context.Context, chatID string, message *Message) error {
	// Marshal message to json bytes
	text, err := json.Marshal(message)
	if err != nil {
		return err
	}

	// Save message to redis
	msg := redis.Z{
		Score:  float64(message.Timestamp),
		Member: text,
	}

	_, err = c.cli.ZAdd(ctx, chatID, &msg).Result()
	if err != nil {
		return err
	}

	return nil
}

func (c *redisClient) PullMessage(ctx context.Context, chatID string, start, end int64, reverse bool) ([]*Message, error) {
	var rawMessages []string
	var deserializedMessages []*Message
	var err error

	if reverse {
		rawMessages, err = c.cli.ZRevRange(ctx, chatID, start, end).Result()
		if err != nil {
			return nil, err
		}
	} else {
		rawMessages, err = c.cli.ZRange(ctx, chatID, start, end).Result()
		if err != nil {
			return nil, err
		}
	}

	for _, rawMessage := range rawMessages {
		var message Message
		err = json.Unmarshal([]byte(rawMessage), &message)
		if err != nil {
			return nil, err
		}
		deserializedMessages = append(deserializedMessages, &message)
	}

	return deserializedMessages, nil
}
