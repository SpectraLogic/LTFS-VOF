package utils

import (
	"github.com/oklog/ulid/v2"
	"strings"
)

// get time from a file with a ulid name
func GetTimeFromID(filename string, logger *Logger) (ulid.ULID, uint64) {
	// need to remove suffix from filename
	name := strings.TrimSuffix(filename, ".blk")
	// if not .blk suffix then trim .ver
	if len(name) == len(filename) {
		name = strings.TrimSuffix(filename, ".ver")
	}
	// now create ULID from name
	ulid, err := ulid.Parse(name)
	if err != nil {
		logger.Fatal("Unable to ulid parse: ", name)
	}
	return ulid, ulid.Time()
}
