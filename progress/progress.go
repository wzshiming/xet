package progress

// ProgressFunc is a callback function type for tracking progress of uploads or downloads.
type ProgressFunc func(name string, current, total int64)
