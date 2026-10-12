package internal

import (
	"fmt"
	"testing"
)

func TestCoordinatesConversion(t *testing.T) {
	x, y, err := Convert2180to4326(500_000, 500_000)
	if err != nil {
		t.Fatalf("unexpected error: %v\n", err)
	}
	fmt.Println(x, y)
}
