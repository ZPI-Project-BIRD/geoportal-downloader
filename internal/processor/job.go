package processor

type Job struct {
	ProjectID  string `json:"projectId"`
	FeatureID  string `json:"featureId"`
	ObjectName string `json:"objectName"`
}
