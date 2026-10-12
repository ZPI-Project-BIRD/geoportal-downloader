package download

import (
	"context"
	"encoding/json"
	"fmt"
)

const CreateDownloadQueue = "download.create"

func EnqueueCreateDownload(
	ctx context.Context,
	publish func(context.Context, string, []byte) error,
	request CreateRequest,
) error {
	body, err := json.Marshal(newCreateDownloadMessage(request))
	if err != nil {
		return fmt.Errorf("encode create download request: %w", err)
	}
	return publish(ctx, CreateDownloadQueue, body)
}

func HandleCreateDownload(
	process func(context.Context, CreateRequest) error,
) func(context.Context, []byte) error {
	return func(ctx context.Context, body []byte) error {
		var message createDownloadMessage
		if err := json.Unmarshal(body, &message); err != nil {
			return fmt.Errorf("decode create download request: %w", err)
		}
		return process(ctx, message.request())
	}
}

type createDownloadMessage struct {
	Layer       string       `json:"layer"`
	TopLeft     pointMessage `json:"top_left"`
	BottomRight pointMessage `json:"bottom_right"`
}

type pointMessage struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

func newCreateDownloadMessage(request CreateRequest) createDownloadMessage {
	return createDownloadMessage{
		Layer:       request.Layer,
		TopLeft:     pointMessage(request.TopLeft),
		BottomRight: pointMessage(request.BottomRight),
	}
}

func (m createDownloadMessage) request() CreateRequest {
	return CreateRequest{
		Layer:       m.Layer,
		TopLeft:     Point(m.TopLeft),
		BottomRight: Point(m.BottomRight),
	}
}
