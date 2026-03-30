package upload

// XorbUploadResponse represents the response from uploading an xorb
type XorbUploadResponse struct {
	WasInserted bool `json:"was_inserted"`
}

// ShardUploadResponse represents the response from uploading a shard
type ShardUploadResponse struct {
	Result int `json:"result"` // 0 = already exists, 1 = was registered
}
