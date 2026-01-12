package entity

type S3Client struct {
	Url                string `json:"url"`
	AccessKeyID        string `json:"access_key_id"`
	AccessKeySecret    string `json:"access_key_secret"`
	BucketName         string `json:"bucket_name"`
	RegionName         string `json:"region_name"`
	MaxPoolConnections int    `json:"max_pool_connections"`
	SSLVerify          bool   `json:"ssl_verify"`
}
