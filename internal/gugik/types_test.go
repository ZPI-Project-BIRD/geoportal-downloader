package gugik_test

import (
	"encoding/json"
	"encoding/xml"
	"testing"
	"time"

	"bird-zpi/downloader/internal/gugik"
)

func TestDecodeFeatureCollection(t *testing.T) {
	const document = `<wfs:FeatureCollection
 xmlns:wfs="http://www.opengis.net/wfs"
 xmlns:gml="http://www.opengis.net/gml"
 xmlns:gugik="http://www.gugik.gov.pl"
 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 xsi:schemaLocation="http://www.gugik.gov.pl schema.xsd">
 <gml:boundedBy><gml:Envelope srsName="EPSG:2180">
  <gml:lowerCorner>498689.93 497872.28</gml:lowerCorner>
  <gml:upperCorner>501007.05 502127.72</gml:upperCorner>
 </gml:Envelope></gml:boundedBy>
 <gml:featureMember>
  <gugik:SkorowidzOrtofomapy2025 gml:id="SkorowidzOrtofomapy2025.1542831">
   <gml:boundedBy><gml:Envelope srsName="EPSG:2180">
    <gml:lowerCorner>498689.93 497872.28</gml:lowerCorner>
    <gml:upperCorner>501007.05 500000</gml:upperCorner>
   </gml:Envelope></gml:boundedBy>
   <gugik:msGeometry><gml:Polygon srsName="EPSG:2180"><gml:exterior>
    <gml:LinearRing><gml:posList srsDimension="2">498689.93 497872.28 501007.05 500000 498689.93 497872.28</gml:posList></gml:LinearRing>
   </gml:exterior></gml:Polygon></gugik:msGeometry>
   <gugik:godlo>N-34-122-D-d-4-2</gugik:godlo>
   <gugik:akt_rok>2025</gugik:akt_rok>
   <gugik:piksel>0.25</gugik:piksel>
   <gugik:kolor>CIR</gugik:kolor>
   <gugik:zrodlo_danych>Zdj. cyfrowe</gugik:zrodlo_danych>
   <gugik:uklad_xy>PL-1992</gugik:uklad_xy>
   <gugik:modul_archiwizacji>1:5000</gugik:modul_archiwizacji>
   <gugik:nr_zglosz>DFT.7201.009.2025</gugik:nr_zglosz>
   <gugik:akt_data>2025-04-04</gugik:akt_data>
   <gugik:czy_ark_wypelniony>TAK</gugik:czy_ark_wypelniony>
   <gugik:url_do_pobrania>https://example.com/orthophoto.tif</gugik:url_do_pobrania>
   <gugik:dt_pzgik>2026-03-20</gugik:dt_pzgik>
   <gugik:wlk_pliku_mb>32</gugik:wlk_pliku_mb>
  </gugik:SkorowidzOrtofomapy2025>
 </gml:featureMember>
</wfs:FeatureCollection>`

	var collection gugik.FeatureCollection
	if err := xml.Unmarshal([]byte(document), &collection); err != nil {
		t.Fatalf("decode feature collection: %v", err)
	}

	if got, want := collection.SchemaLocation, "http://www.gugik.gov.pl schema.xsd"; got != want {
		t.Errorf("SchemaLocation = %q, want %q", got, want)
	}
	wantLowerCorner := gugik.Coordinate{498689.93, 497872.28}
	if got := collection.BoundedBy.Envelope.LowerCorner; !coordinatesEqual(got, wantLowerCorner) {
		t.Errorf("collection lower corner = %v, want %v", got, wantLowerCorner)
	}
	if got, want := len(collection.FeatureMembers), 1; got != want {
		t.Fatalf("feature count = %d, want %d", got, want)
	}

	feature := collection.FeatureMembers[0].OrthophotoIndex
	if got, want := feature.ID, "SkorowidzOrtofomapy2025.1542831"; got != want {
		t.Errorf("feature ID = %q, want %q", got, want)
	}
	if got, want := feature.Year, 2025; got != want {
		t.Errorf("feature year = %d, want %d", got, want)
	}
	if got, want := feature.UpdateDate.Time(), time.Date(2025, time.April, 4, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("feature update date = %s, want %s", got, want)
	}
	if got, want := feature.Geometry.Polygon.Exterior.LinearRing.Positions.Dimension, 2; got != want {
		t.Errorf("position dimension = %d, want %d", got, want)
	}
	if got, want := len(feature.Geometry.Polygon.Exterior.LinearRing.Positions.Ordinates), 6; got != want {
		t.Errorf("ordinate count = %d, want %d", got, want)
	}
}

func TestFeatureMemberJSONRoundTrip(t *testing.T) {
	want := gugik.FeatureMember{
		OrthophotoIndex: gugik.OrthophotoIndex{
			ID: "feature-1",
			BoundedBy: gugik.BoundedBy{Envelope: gugik.Envelope{
				LowerCorner: gugik.Coordinate{498689.93, 497872.28},
				UpperCorner: gugik.Coordinate{501007.05, 500000},
			}},
			UpdateDate:  gugik.Date(time.Date(2025, time.April, 4, 0, 0, 0, 0, time.UTC)),
			DatasetDate: gugik.Date(time.Date(2026, time.March, 20, 0, 0, 0, 0, time.UTC)),
		},
	}

	data, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("encode feature member: %v", err)
	}

	var got gugik.FeatureMember
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("decode feature member: %v", err)
	}

	if !coordinatesEqual(got.OrthophotoIndex.BoundedBy.Envelope.LowerCorner, want.OrthophotoIndex.BoundedBy.Envelope.LowerCorner) {
		t.Errorf("lower corner = %v, want %v", got.OrthophotoIndex.BoundedBy.Envelope.LowerCorner, want.OrthophotoIndex.BoundedBy.Envelope.LowerCorner)
	}
	if got.OrthophotoIndex.UpdateDate.String() != want.OrthophotoIndex.UpdateDate.String() {
		t.Errorf("update date = %s, want %s", got.OrthophotoIndex.UpdateDate, want.OrthophotoIndex.UpdateDate)
	}
}

func TestDateUnmarshalLegacyEmptyObject(t *testing.T) {
	var date gugik.Date
	if err := json.Unmarshal([]byte(`{}`), &date); err != nil {
		t.Fatalf("decode legacy date: %v", err)
	}
	if !date.Time().IsZero() {
		t.Errorf("legacy date = %s, want zero value", date)
	}
}

func coordinatesEqual(left, right gugik.Coordinate) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
