package download

type Point struct {
	X float64
	Y float64
}

type CreateRequest struct {
	Layer       string
	TopLeft     Point
	BottomRight Point
}
