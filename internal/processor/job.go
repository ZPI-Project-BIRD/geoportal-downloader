package processor

const Queue = "process.feature"

type Job struct {
	FeatureID  string `json:"featureId"`
	ObjectName string `json:"objectName"`
}
