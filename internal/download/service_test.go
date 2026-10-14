package download

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"bird-zpi/downloader/internal/database"
	"bird-zpi/downloader/internal/gugik"
	"bird-zpi/downloader/internal/processor"

	amqp "github.com/rabbitmq/amqp091-go"
)

type recordingDatabase struct {
	feature database.Feature
}

func (d *recordingDatabase) InsertFeature(_ context.Context, feature database.Feature) error {
	d.feature = feature
	return nil
}

type recordingStorage struct {
	objectName  string
	contentType string
	body        []byte
}

func (s *recordingStorage) Upload(_ context.Context, objectName string, reader io.Reader, _ int64, contentType string) (int64, error) {
	body, err := io.ReadAll(reader)
	if err != nil {
		return 0, err
	}
	s.objectName = objectName
	s.contentType = contentType
	s.body = body
	return int64(len(body)), nil
}

type recordingAcknowledger struct {
	acknowledged bool
}

func (a *recordingAcknowledger) Ack(_ uint64, _ bool) error {
	a.acknowledged = true
	return nil
}

func (*recordingAcknowledger) Nack(_ uint64, _, _ bool) error { return nil }
func (*recordingAcknowledger) Reject(_ uint64, _ bool) error  { return nil }

type recordingProcessorPublisher struct {
	job processor.Job
}

func (p *recordingProcessorPublisher) ProcessFeature(_ context.Context, job processor.Job) error {
	p.job = job
	return nil
}

func TestHandleDownloadFeatureStoresAndRecordsFeature(t *testing.T) {
	const fileContents = "test geotiff contents"
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "image/tiff")
		_, _ = io.WriteString(response, fileContents)
	}))
	defer server.Close()

	datasetDate := time.Date(2026, time.March, 20, 0, 0, 0, 0, time.UTC)
	message, err := json.Marshal(gugik.FeatureMember{OrthophotoIndex: gugik.OrthophotoIndex{
		ID:          "feature-1",
		Color:       "RGB",
		PixelSize:   0.25,
		DatasetDate: gugik.Date(datasetDate),
		DownloadURL: server.URL,
	}})
	if err != nil {
		t.Fatalf("encode feature: %v", err)
	}

	featureDatabase := &recordingDatabase{}
	objectStorage := &recordingStorage{}
	processorPublisher := &recordingProcessorPublisher{}
	acknowledger := &recordingAcknowledger{}
	service := NewDownloadWorker(featureDatabase, objectStorage, processorPublisher)

	err = service.HandleDownloadFeature(context.Background(), amqp.Delivery{
		Acknowledger: acknowledger,
		DeliveryTag:  1,
		Body:         message,
	})
	if err != nil {
		t.Fatalf("handle download feature: %v", err)
	}

	if got, want := objectStorage.objectName, "feature-1.tif"; got != want {
		t.Errorf("object name = %q, want %q", got, want)
	}
	if got, want := string(objectStorage.body), fileContents; got != want {
		t.Errorf("stored contents = %q, want %q", got, want)
	}
	if got, want := objectStorage.contentType, "image/tiff"; got != want {
		t.Errorf("content type = %q, want %q", got, want)
	}
	if got, want := featureDatabase.feature.ID, "feature-1"; got != want {
		t.Errorf("database feature ID = %q, want %q", got, want)
	}
	if got, want := featureDatabase.feature.FileSize, int64(len(fileContents)); got != want {
		t.Errorf("database file size = %d, want %d", got, want)
	}
	if !featureDatabase.feature.DatasetDate.Equal(datasetDate) {
		t.Errorf("database dataset date = %s, want %s", featureDatabase.feature.DatasetDate, datasetDate)
	}
	if got, want := processorPublisher.job, (processor.Job{FeatureID: "feature-1", ObjectName: "feature-1.tif"}); got != want {
		t.Errorf("processor job = %#v, want %#v", got, want)
	}
	if !acknowledger.acknowledged {
		t.Error("delivery was not acknowledged")
	}
}
