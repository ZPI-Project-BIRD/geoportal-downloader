package download

import "errors"

var ErrFeatureNotFound = errors.New("feature not found")

type RetryAction string

const (
	RetryDownloadQueued  RetryAction = "download_queued"
	RetryTilingQueued    RetryAction = "tiling_queued"
	RetryAlreadyComplete RetryAction = "already_complete"
)

type Point struct {
	X float64
	Y float64
}

type CreateRequest struct {
	Layer       string
	TopLeft     Point
	BottomRight Point
}
