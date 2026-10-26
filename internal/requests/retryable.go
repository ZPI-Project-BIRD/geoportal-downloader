package requests

import (
	"fmt"
	"net/http"
	"time"
)

func RetryableRequest(client *http.Client, url string) (resp *http.Response, err error) {
	var lastError error
	for retry := range 4 {
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
