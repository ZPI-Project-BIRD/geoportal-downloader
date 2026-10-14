package detection

const Queue = "detect.feature"

type Job struct {
	ProjectID  string `json:"projectId"`
	FeatureID  string `json:"featureId"`
	LayerID    string `json:"layerId"`
	ObjectName string `json:"objectName"`
}
