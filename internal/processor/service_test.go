package processor

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

type sourceStorage struct{}

func (sourceStorage) Download(_ context.Context, _ string, writer io.Writer) (int64, error) {
	written, err := io.WriteString(writer, "geotiff")
	return int64(written), err
}

func (sourceStorage) Upload(context.Context, string, io.Reader, int64, string) (int64, error) {
	panic("unexpected upload to source storage")
}

type uploadedTile struct {
	name        string
	contentType string
	body        string
}

type tileStorage struct {
	tiles []uploadedTile
}

func (tileStorage) Download(context.Context, string, io.Writer) (int64, error) {
	panic("unexpected download from tile storage")
}

func (s *tileStorage) Upload(_ context.Context, name string, reader io.Reader, _ int64, contentType string) (int64, error) {
	body, err := io.ReadAll(reader)
	if err != nil {
		return 0, err
	}
	s.tiles = append(s.tiles, uploadedTile{name: name, contentType: contentType, body: string(body)})
	return int64(len(body)), nil
}

type testTiler struct{}

func (testTiler) Tile(_ context.Context, _ string, outputDirectory string) error {
	directory := filepath.Join(outputDirectory, "12", "2175")
	if err := os.MkdirAll(directory, 0750); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(directory, "1367.png"), []byte("png"), 0640)
}

type testAcknowledger struct {
	acknowledged bool
}

func (a *testAcknowledger) Ack(_ uint64, _ bool) error {
	a.acknowledged = true
	return nil
}

func (*testAcknowledger) Nack(_ uint64, _, _ bool) error { return nil }
func (*testAcknowledger) Reject(_ uint64, _ bool) error  { return nil }

func TestHandleProcessFeatureUploadsXYZTiles(t *testing.T) {
	message, err := json.Marshal(Job{FeatureID: "feature-1", ObjectName: "feature-1.tif"})
	if err != nil {
		t.Fatalf("encode job: %v", err)
	}

	tiles := &tileStorage{}
	acknowledger := &testAcknowledger{}
	service := NewService(sourceStorage{}, tiles, testTiler{})
	err = service.HandleProcessFeature(context.Background(), amqp.Delivery{
		Acknowledger: acknowledger,
		DeliveryTag:  1,
		Body:         message,
	})
	if err != nil {
		t.Fatalf("process feature: %v", err)
	}

	if got, want := len(tiles.tiles), 1; got != want {
		t.Fatalf("uploaded tile count = %d, want %d", got, want)
	}
	if got, want := tiles.tiles[0].name, "feature-1/12/2175/1367.png"; got != want {
		t.Errorf("tile object name = %q, want %q", got, want)
	}
	if got, want := tiles.tiles[0].contentType, "image/png"; got != want {
		t.Errorf("tile content type = %q, want %q", got, want)
	}
	if !acknowledger.acknowledged {
		t.Error("delivery was not acknowledged")
	}
}
