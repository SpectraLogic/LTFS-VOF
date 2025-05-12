package main

import (
	"github.com/oklog/ulid/v2"
	"strings"
	. "ltfs-vof/logger"
)
// get time from a file with a ulid name followed
func getTimeFromID(filename string, logger *Logger) (ulid.ULID, uint64) {
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
