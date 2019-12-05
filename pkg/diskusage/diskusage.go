package diskusage

// Usage holds information about total and available storage on a volume.
type Usage struct {
	TotalBytes uint64 // Size of volume
	FreeBytes  uint64 // Unused size
	AvailBytes uint64 // Available bytes to a non-privileged user
}
