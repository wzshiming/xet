package upload

// XorbUploadResponse represents the response from uploading an xorb
type XorbUploadResponse struct {
	WasInserted bool `json:"was_inserted"`
}

// ShardUploadResponse represents the response from uploading a shard
type ShardUploadResponse struct {
	Result int `json:"result"` // 0 = already exists, 1 = was registered
}

// HeaderDirectUpload is the xorb upload POST header a client sends to
// advertise it can follow a 307 redirect to a direct-upload PUT URL.
// Servers must not redirect without it: xet-core auto-follows the redirect
// as a re-POST, which presigned PUT URLs reject.
const HeaderDirectUpload = "X-Xet-Direct-Upload"

// DirectUploadAccept is the HeaderDirectUpload value sent by capable clients.
const DirectUploadAccept = "accept"
