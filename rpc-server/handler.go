package main

import (
	"context"
	"strings"
	"time"

	"github.com/TikTokTechImmersion/assignment_demo_2023/rpc-server/kitex_gen/rpc"
)

// IMServiceImpl implements the last service interface defined in the IDL.
type IMServiceImpl struct{}

func (s *IMServiceImpl) Send(ctx context.Context, req *rpc.SendRequest) (*rpc.SendResponse, error) {
	timestamp := time.Now().Unix()

	message := &Message{
		Sender:    req.Message.GetSender(),
		Message:   req.Message.GetText(),
		Timestamp: timestamp,
	}

	chatId, err := getChatId(req.Message.GetChat())

	if err != nil {
		return nil, err
	}

	err = rdb.SaveMessage(ctx, chatId, message)

	resp := rpc.NewSendResponse()
	resp.Code = 0
	resp.Msg = "success"

	return resp, nil
}

func (s *IMServiceImpl) Pull(ctx context.Context, req *rpc.PullRequest) (*rpc.PullResponse, error) {
	chatId, err := getChatId(req.GetChat())
	if err != nil {
		return nil, err
	}

	start := req.GetCursor()
	end := start + int64(req.GetLimit())

	messages, err := rdb.PullMessage(ctx, chatId, start, end, req.GetReverse())

	if err != nil {
		return nil, err
	}

	respMessages := make([]*rpc.Message, 0)
	var counter int32 = 0
	var nextCursor int64 = 0
	hasMore := false
	for _, msg := range messages {
		if counter+1 > req.GetLimit() {
			// having extra value here means it has more data
			hasMore = true
			nextCursor = end
			break // do not return the last message
		}
		temp := &rpc.Message{
			Chat:     req.GetChat(),
			Text:     msg.Message,
			Sender:   msg.Sender,
			SendTime: msg.Timestamp,
		}
		respMessages = append(respMessages, temp)
		counter += 1
	}

	resp := rpc.NewPullResponse()
	resp.Messages = respMessages
	resp.Code = 0
	resp.Msg = "success"
	resp.HasMore = &hasMore
	resp.NextCursor = &nextCursor

	return resp, nil
}

func getChatId(chat string) (string, error) {
	processedString := strings.ToLower(chat)
	parties := strings.Split(processedString, ":")
	//if len(parties) != 2 {
	//	return "", errors.New(parties[0])
	//}

	// sort parties[0] and parties[1] lexico-graphically
	if parties[0] > parties[1] {
		return parties[1] + ":" + parties[0], nil
	} else {
		return parties[0] + ":" + parties[1], nil
	}
}
