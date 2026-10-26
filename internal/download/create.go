package download

import (
	"bird-zpi/downloader/internal/gugik"
)

type Job struct {
	ProjectID string              `json:"projectId"`
	Feature   gugik.FeatureMember `json:"feature"`
}

// type RetryPublisher interface {
// 	ProcessorPublisher
// }
