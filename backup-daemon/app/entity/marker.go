package entity

type MarkerRequest struct {
	Marker string `json:"marker" binding:"required"`
}

type MarkerResponse struct {
	Marker string `json:"marker"`
}
