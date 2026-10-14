package download

import (
	"context"

	"bird-zpi/downloader/internal/gugik"
)

const DownloadFeatureQueue = "download.feature"

type Publisher interface {
	DownloadFeature(context.Context, Job) error
}

type Job struct {
	ProjectID string              `json:"projectId"`
	LayerID   string              `json:"layerId"`
	Feature   gugik.FeatureMember `json:"feature"`
}

type RetryPublisher interface {
	Publisher
	ProcessorPublisher
}
