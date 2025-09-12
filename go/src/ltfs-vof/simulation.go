// These is the startup program
package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"github.com/oklog/ulid/v2"
	"io"
	. "ltfs-vof/utils"
	"os"
)

const SIMULATION_FILES string = "tapehardware/tapes/"

func createSimulatedTapes(numberOfTapes int, bucket string, logger *Logger) {
	objectCount := 0
	for tape := 0; tape < numberOfTapes; tape++ {
		// remove existing directory
		err := os.RemoveAll(fmt.Sprintf("%stape%02d", SIMULATION_FILES, tape))

		// create 10 block files of random data
		err = os.MkdirAll(fmt.Sprintf("%stape%02d", SIMULATION_FILES, tape), 0755)
		if err != nil {
			logger.Fatal("Unable to create simulated tape directory")
		}
		// one version file per tape
		versionName := ulid.Make().String()
		versionfd, err := os.Create(fmt.Sprintf("%stape%02d/%s.ver", SIMULATION_FILES, tape, versionName))
		defer versionfd.Close()

		// 10 block files per tape
		for blockFile := 0; blockFile < 1; blockFile += 1 {
			// create the block file
			blockFileName := ulid.Make().String()
			fd, err := os.Create(fmt.Sprintf("%stape%02d/%s.blk", SIMULATION_FILES, tape, blockFileName))
			defer fd.Close()

			// put a 10 objects in each file
			for blocks := 0; blocks < 10; blocks++ {
				// create a unique version ulid
				vid := ulid.Make().String()

				// create a random 500 bytes of data to be a block
				randomData := make([]byte, 500)
				_, err = rand.Read(randomData)
				if err != nil {
					logger.Fatal("Unable to create random data")
				}
				// make object name
				objectName := fmt.Sprintf("Object%06d", objectCount)
				objectCount++

				// write object to s3
				if bucket != "" {
					PutObject(bucket, objectName, DEFAULT_REGION, randomData)
				}

				//create the block
				block := NewBlock("", bucket, objectName, vid, randomData, int64(0), int64(len(randomData)))

				// json marshal the block
				blockData, _ := json.Marshal(block)

				// write a TLV for the block
				WriteTLV(fd, BLOCK, blockData, logger)
				fmt.Println("Block Data Length: ", len(blockData))

				// record start of TLV and write a TLV block header
				startRange, err := fd.Seek(0, io.SeekCurrent)
				if err != nil {
					logger.Fatal("Unable to get start range")
				}

				// write the block to the block file
				_, err = fd.Write(blockData)
				if err != nil {
					logger.Fatal("Unable to write block data")
				}

				// create the packentry for this block both the physical and logical
				packEntry := NewPackEntry(blockFileName, 0, int64(len(blockData)))
				packEntry.SetPhysicalLocation(blockFileName, startRange, startRange+int64(len(blockData)))

				// create the version data
				vd := NewVersionRecord(bucket, objectName, vid, packEntry)
				// encode the structure using json
				ud, err := json.Marshal(vd)
				if err != nil {
					logger.Fatal("Unable to marshal version data")
				}

				// write a version TLV in the version file
				fmt.Println("version file: ", versionName, " size: ", len(ud))
				WriteTLV(versionfd, VERSION, ud, logger)
				// write the version data
				_, err = versionfd.Write(ud)
				if err != nil {
					logger.Fatal("Unable to write version data")
				}

			}
		}
	}
}
