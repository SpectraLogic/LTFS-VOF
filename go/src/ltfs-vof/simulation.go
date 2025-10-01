// These is the startup program
package main

import (
	"crypto/rand"
	"fmt"
	"github.com/oklog/ulid/v2"
	"io"
	. "ltfs-vof/utils"
	"os"
)

const SIMULATION_FILES string = "tapehardware/tapes/"

func createSimulatedTapes(numberOfTapes int, s3Enabled bool, buckets []string, blocksPerObject int, versioning, inDB, packList bool, logger *Logger) {
	objectCount := 0
	// remove all the tapes first
	os.RemoveAll(SIMULATION_FILES)

	// create the s3 simulation buckets
	s3Buckets := make(map[string]*S3Simulator)
	if s3Enabled {
		for _, bucket := range buckets {
			// create the s3 simulation buckets
			logger.Event("Creating simulated S3 bucket: ", bucket)
			s3Buckets[bucket] = NewS3Simulator(DEFAULT_REGION, bucket+"simulat", versioning, logger)
		}
	}

	for tape := 0; tape < numberOfTapes; tape++ {
		fmt.Println("Creating simulated tape", tape)
		// make the tape directory
		err := os.MkdirAll(fmt.Sprintf("%stape%02d", SIMULATION_FILES, tape), 0755)
		if err != nil {
			logger.Fatal("Unable to create simulated tape directory")
		}
		// one version file per tape
		versionName := ulid.Make().String()
		versionfd, err := os.Create(fmt.Sprintf("%stape%02d/%s.ver", SIMULATION_FILES, tape, versionName))
		defer versionfd.Close()

		// one block file per bucket per tape
		for _, bucket := range buckets {
			// create the block file
			blockFileName := ulid.Make().String()
			fd, err := os.Create(fmt.Sprintf("%stape%02d/%s.blk", SIMULATION_FILES, tape, blockFileName))
			defer fd.Close()

			// put a 10 objects in each block file
			for objects := 0; objects < 10; objects++ {
				// create a unique version ulid
				vid := ulid.Make().String()

				// right now a single block per object
				randomData := make([]byte, 500)
				_, err = rand.Read(randomData)
				if err != nil {
					logger.Fatal("Unable to create random data")
				}
				// make object name
				objectName := fmt.Sprintf("Object%06d", objectCount)
				objectCount++

				// write the object to the proper simulation bucket
				if s3Enabled {
					s3Buckets[bucket].Put(objectName, randomData)
				}

				//create the block
				block := NewBlock("", bucket, objectName, vid, randomData, int64(0), int64(len(randomData)))

				// record start of TLV and write a TLV block header
				startRange, err := fd.Seek(0, io.SeekCurrent)
				if err != nil {
					logger.Fatal("Unable to get start range")
				}

				// write a TLV for the block
				WriteTLV(fd, BLOCK, block.data, logger)

				// write the block to the block file
				WriteBlock(fd, block, logger)

				// create a single packentry for this block both the physical and logical
				packEntries := make([]*PackEntry, 1)

				packEntries[0] = NewPackEntry(blockFileName, 0, int64(len(block.data)))
				packEntries[0].SetPhysicalLocation(blockFileName, startRange, startRange+int64(len(block.data)))

				// create the version record
				vr, vrEncoded := NewVersionRecord(bucket, objectName, vid, packEntries, nil, logger)

				WriteTLV(versionfd, VERSION, vrEncoded, logger)

				// write the version data
				vr.WriteVersionRecord(versionfd, logger)
			}
		}
	}
}
