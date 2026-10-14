package processor

const Queue = "process.feature"

type Job struct {
	FeatureID  string `json:"featureId"`
	LayerID    string `json:"layerId"`
	ObjectName string `json:"objectName"`
}
