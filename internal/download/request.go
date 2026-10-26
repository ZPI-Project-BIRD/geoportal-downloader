package download

import "errors"

var ErrFeatureNotFound = errors.New("feature not found")

var ErrProjectNotFound = errors.New("project not found")

type RetryAction string

const (
	RetryDownloadQueued  RetryAction = "download_queued"
	RetryTilingQueued    RetryAction = "tiling_queued"
	RetryAlreadyComplete RetryAction = "already_complete"
)

type CreateRequest struct {
	ProjectID string
	Layer     string
}
