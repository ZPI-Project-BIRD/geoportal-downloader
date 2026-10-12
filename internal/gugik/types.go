// Package gugik contains the XML representation of resources exposed by the
// GUGiK (Head Office of Geodesy and Cartography) services.
package gugik

import (
	"encoding/xml"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	GMLNamespace   = "http://www.opengis.net/gml"
	GUGiKNamespace = "http://www.gugik.gov.pl"
	WFSNamespace   = "http://www.opengis.net/wfs"
	XSINamespace   = "http://www.w3.org/2001/XMLSchema-instance"
)

// FeatureCollection is the response returned for the
// gugik:SkorowidzOrtofomapy2025 WFS feature type.
type FeatureCollection struct {
	XMLName        xml.Name        `xml:"http://www.opengis.net/wfs FeatureCollection"`
	SchemaLocation string          `xml:"http://www.w3.org/2001/XMLSchema-instance schemaLocation,attr"`
	BoundedBy      BoundedBy       `xml:"http://www.opengis.net/gml boundedBy"`
	FeatureMembers []FeatureMember `xml:"http://www.opengis.net/gml featureMember"`
}

type FeatureMember struct {
	OrthophotoIndex OrthophotoIndex `xml:"http://www.gugik.gov.pl SkorowidzOrtofomapy2025"`
}

// OrthophotoIndex describes one orthophoto sheet available for download.
type OrthophotoIndex struct {
	XMLName xml.Name `xml:"http://www.gugik.gov.pl SkorowidzOrtofomapy2025"`
	ID      string   `xml:"http://www.opengis.net/gml id,attr"`

	BoundedBy BoundedBy `xml:"http://www.opengis.net/gml boundedBy"`
	Geometry  Geometry  `xml:"http://www.gugik.gov.pl msGeometry"`

	SheetDesignation string  `xml:"http://www.gugik.gov.pl godlo"`
	Year             int     `xml:"http://www.gugik.gov.pl akt_rok"`
	PixelSize        float64 `xml:"http://www.gugik.gov.pl piksel"`
	Color            string  `xml:"http://www.gugik.gov.pl kolor"`
	DataSource       string  `xml:"http://www.gugik.gov.pl zrodlo_danych"`
	CoordinateSystem string  `xml:"http://www.gugik.gov.pl uklad_xy"`
	ArchivalScale    string  `xml:"http://www.gugik.gov.pl modul_archiwizacji"`
	SubmissionNumber string  `xml:"http://www.gugik.gov.pl nr_zglosz"`
	UpdateDate       Date    `xml:"http://www.gugik.gov.pl akt_data"`
	SheetFilled      string  `xml:"http://www.gugik.gov.pl czy_ark_wypelniony"`
	DownloadURL      string  `xml:"http://www.gugik.gov.pl url_do_pobrania"`
	DatasetDate      Date    `xml:"http://www.gugik.gov.pl dt_pzgik"`
	FileSizeMB       float64 `xml:"http://www.gugik.gov.pl wlk_pliku_mb"`
}

type BoundedBy struct {
	Envelope Envelope `xml:"http://www.opengis.net/gml Envelope"`
}

type Envelope struct {
	SRSName     string     `xml:"srsName,attr"`
	LowerCorner Coordinate `xml:"http://www.opengis.net/gml lowerCorner"`
	UpperCorner Coordinate `xml:"http://www.opengis.net/gml upperCorner"`
}

type Geometry struct {
	Polygon Polygon `xml:"http://www.opengis.net/gml Polygon"`
}

type Polygon struct {
	SRSName  string   `xml:"srsName,attr"`
	Exterior Exterior `xml:"http://www.opengis.net/gml exterior"`
}

type Exterior struct {
	LinearRing LinearRing `xml:"http://www.opengis.net/gml LinearRing"`
}

type LinearRing struct {
	Positions PositionList `xml:"http://www.opengis.net/gml posList"`
}

type PositionList struct {
	Dimension int        `xml:"srsDimension,attr"`
	Ordinates Coordinate `xml:",chardata"`
}

// Coordinate holds whitespace-separated coordinate ordinates. For a point it
// contains one tuple; for a GML posList it contains consecutive tuples whose
// width is given by PositionList.Dimension.
type Coordinate []float64

func (c *Coordinate) UnmarshalText(text []byte) error {
	fields := strings.Fields(string(text))
	coordinates := make(Coordinate, len(fields))
	for i, field := range fields {
		value, err := strconv.ParseFloat(field, 64)
		if err != nil {
			return fmt.Errorf("parse coordinate %q: %w", field, err)
		}
		coordinates[i] = value
	}
	*c = coordinates
	return nil
}

// Date represents the date-only values used by the GUGiK service.
type Date time.Time

func (d *Date) UnmarshalText(text []byte) error {
	value, err := time.Parse(time.DateOnly, string(text))
	if err != nil {
		return fmt.Errorf("parse GUGiK date %q: %w", text, err)
	}
	*d = Date(value)
	return nil
}

func (d Date) Time() time.Time {
	return time.Time(d)
}

func (d Date) String() string {
	return d.Time().Format(time.DateOnly)
}
