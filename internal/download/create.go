package download

import (
	"context"

	"bird-zpi/downloader/internal/gugik"
)

const DownloadFeatureQueue = "download.feature"

type Publisher interface {
	DownloadFeature(context.Context, gugik.FeatureMember) error
}
