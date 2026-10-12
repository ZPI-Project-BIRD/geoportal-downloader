package internal

import (
	"github.com/twpayne/go-proj/v11"
)

func Convert2180to4326(x float64, y float64) (xr float64, yr float64, err error) {
	transformer, err := proj.NewCRSToCRS(
		"EPSG:2180",
		"EPSG:4326",
		nil,
	)
	if err != nil {
		return 0, 0, err
	}

	point := proj.NewCoord(x, y, 0, 0)

	result, err := transformer.Forward(point)
	if err != nil {
		return 0, 0, err
	}

	return result.X(), result.Y(), nil
}

func Convert4326to2180(x float64, y float64) (xr float64, yr float64, err error) {
	transformer, err := proj.NewCRSToCRS(
		"EPSG:4326",
		"EPSG:2180",
		nil,
	)
	if err != nil {
		return 0, 0, err
	}

	point := proj.NewCoord(x, y, 0, 0)

	result, err := transformer.Forward(point)
	if err != nil {
		return 0, 0, err
	}

	return result.X(), result.Y(), nil
}
