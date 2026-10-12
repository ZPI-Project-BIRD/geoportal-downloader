package download

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"time"

	"bird-zpi/downloader/internal/gugik"
)

type Service struct {
	client    *http.Client
	publisher Publisher
}

func NewService(publisher Publisher) *Service {
	return &Service{
		client:    &http.Client{},
		publisher: publisher,
	}
}

func (s *Service) CreateDownload(ctx context.Context, req CreateRequest) error {
	bbox := fmt.Sprintf("%.2f,%.2f,%.2f,%.2f,EPSG:2180", req.TopLeft.X, req.TopLeft.Y, req.BottomRight.X, req.BottomRight.Y)
	url := fmt.Sprintf("https://mapy.geoportal.gov.pl/wss/service/PZGIK/ORTO/WFS/Skorowidze?SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature&TYPENAME=gugik:SkorowidzOrtofomapy2025&SRSNAME=EPSG:2180&BBOX=%s", bbox)

	res, err := retryableRequest(s.client, url)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	var collection gugik.FeatureCollection
	if err := xml.NewDecoder(res.Body).Decode(&collection); err != nil {
		return err
	}

	for _, feature := range collection.FeatureMembers {
		if err := s.publisher.DownloadFeature(ctx, feature); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) HandleDownloadFeature(ctx context.Context, body []byte) error {
	var feature gugik.FeatureMember
	if err := json.Unmarshal(body, &feature); err != nil {
		return fmt.Errorf("decode download feature job: %w", err)
	}

	// Download feature processing goes here.
	return nil
}

func retryableRequest(client *http.Client, url string) (resp *http.Response, err error) {
	var lastError error
	for retry := range 8 {
		if retry > 0 {
			time.Sleep(time.Duration(retry) * time.Second)
		}

		res, err := client.Get(url)
		if err != nil {
			lastError = err
			continue
		}

		if res.StatusCode >= 400 {
			_ = res.Body.Close()
			lastError = fmt.Errorf("download request failed with error code %d", res.StatusCode)
			continue
		}

		return res, nil
	}

	return nil, lastError
}
