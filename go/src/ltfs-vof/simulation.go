// These is the startup program
package main

import (
	"crypto/rand"
	"fmt"
	"github.com/oklog/ulid/v2"
	"io"
	. "ltfs-vof/utils"
	"math"
	"os"
)

const SIMULATION_FILES string = "tapehardware/tapes/"

func getBlockRanges(objectSize int, blockSize int) [][2]int {
	blockCount := int(math.Ceil(float64(objectSize) / float64(blockSize)))
	blockRanges := make([][2]int, blockCount)
	for i := 0; i < blockCount; i++ {
		start := i * blockSize
		end := start + blockSize
		if end > objectSize {
			end = objectSize
		}
		blockRanges[i] = [2]int{start, end}
	}
	return blockRanges
}

// create a simulated object of specified size, spanning blocksPerObject blocks, add it to s3 bucket if enabled
func createSimulatedObject(name string, blockSize int, s3sim *S3Simulator, bucket string, objectSize int, logger *Logger, packFile *os.File, packName string, versionFile *os.File, packing bool, backwards bool, usesPackList bool) {
	var randomData []byte
	randomData = make([]byte, objectSize)
	_, err := rand.Read(randomData)
	if err != nil {
		logger.Fatal("Unable to create random data")
	}

	if s3sim != nil {
		s3sim.Put(name, randomData)
	}

	blockCount := int(math.Ceil(float64(objectSize) / float64(blockSize)))
	versionName := ulid.Make().String()

	blockRanges := getBlockRanges(objectSize, blockSize)

	var packEntries Packs
	var packReference *PackReference = nil
	var startRange int64

	if packing {
		for blockIter := 0; blockIter < blockCount; blockIter++ {
			currBlockRange := blockRanges[blockIter]
			if backwards {
				currBlockRange = blockRanges[blockCount-blockIter-1]
			}

			startRange, err = packFile.Seek(0, io.SeekCurrent)
			if err != nil {
				logger.Fatal("Unable to get start range")
			}

			currBlockData := randomData[currBlockRange[0]:currBlockRange[1]]
			currBlock := NewBlock("", bucket, name, versionName, currBlockData, int64(currBlockRange[0]), int64(currBlockRange[1]))

			WriteTLV(packFile, BLOCK, currBlockData, logger)
			WriteBlock(packFile, currBlock, logger)

			endRange, err := packFile.Seek(0, io.SeekCurrent)
			if err != nil {
				logger.Fatal("Unable to get end range")
			}
			packEntry := NewPackEntry(packName, int64(currBlockRange[0]), int64(currBlockRange[1]))
			packEntry.SetPhysicalLocation(packName, startRange, endRange)
			// add to pack entries if backwards or first block, else append it to the first pack entry (only pack entry needed for sequential blocks)
			if backwards || blockIter == 0 {
				packEntries = append(packEntries, packEntry)
			} else {
				packEntries[0].AddSequentialPacks(packEntry)
			}
		}
	} else {
		packEntries = nil
	}
	randomData = nil
	if usesPackList {
		startRange, err = packFile.Seek(0, io.SeekCurrent)
		if err != nil {
			logger.Fatal("Unable to get start range for packlist")
		}
		packListRecord := NewPackListRecord(versionName, packEntries, logger)
		WriteTLV(packFile, PACKLIST, packListRecord.GetPackListEncoded(logger), logger)
		packListRecord.WritePackListRecord(packFile, logger)
		endRange, err := packFile.Seek(0, io.SeekCurrent)
		if err != nil {
			logger.Fatal("Unable to get end range for packlist")
		}
		packReference = NewPackReference(packName, startRange, endRange-startRange)

		packEntries = nil
	}

	vr, vrEncoded := NewVersionRecord(bucket, name, versionName, packEntries, randomData, packReference, false, false, logger)

	WriteTLV(versionFile, VERSION, vrEncoded, logger)
	vr.WriteVersionRecord(versionFile, logger)

}

func createSimulatedTapes(numberOfTapes int, s3Enabled bool, buckets []string, blocksPerObject int, versioning bool, inDB, packList bool, logger *Logger) {
	objectCount := 0
	// remove all the tapes first
	err := os.RemoveAll(SIMULATION_FILES)
	if err != nil {
		logger.Fatal("Unable to remove existing simulation files")
	}

	// create the s3 simulation buckets
	s3Buckets := make(map[string]*S3Simulator)
	if s3Enabled {
		for _, bucket := range buckets {
			// create the s3 simulation buckets
			logger.Event("Creating simulated S3 bucket: ", bucket)
			s3Buckets[bucket] = NewS3Simulator(DEFAULT_REGION, bucket, versioning, logger)
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
			if err != nil {
				logger.Fatal("Unable to create simulated block file", err)
			}
			defer fd.Close()

			// create simulated objects and write to block and version files
			// 2 objects 500 bytes, single block, in pack
			// for objects := 0; objects < 2; objects++ {
			//	objectName := fmt.Sprintf("Object%06d", objectCount)
			//	objectCount++
			//	createSimulatedObject(objectName, 500, s3Buckets[bucket], bucket, 500, logger, fd, blockFileName, versionfd, true, false, false)
			//}
			//// 2 objects 100 bytes, in version record
			//for objects := 0; objects < 2; objects++ {
			//	objectName := fmt.Sprintf("Object%06d", objectCount)
			//	objectCount++
			//	createSimulatedObject(objectName, 100, s3Buckets[bucket], bucket, 100, logger, fd, blockFileName, versionfd, false, false, false)
			//}
			//// 2 objects 1800 bytes, 500 byte blocks, in pack
			//for objects := 0; objects < 2; objects++ {
			//	objectName := fmt.Sprintf("Object%06d", objectCount)
			//	objectCount++
			//	createSimulatedObject(objectName, 500, s3Buckets[bucket], bucket, 1800, logger, fd, blockFileName, versionfd, true, false, false)
			//}
			//// 2 objects 1801 bytes, 500 byte blocks, in pack, 3 versions each
			//for objects := 0; objects < 2; objects++ {
			//	objectName := fmt.Sprintf("Object%06d", objectCount)
			//	objectCount++
			//	for versions := 0; versions < 3; versions++ {
			//		createSimulatedObject(objectName, 500, s3Buckets[bucket], bucket, 1801, logger, fd, blockFileName, versionfd, true, false, false)
			//	}
			//}
			//// 2 objects 1802 bytes, 500 byte blocks, in pack, written backwards in the pack file
			//for objects := 0; objects < 2; objects++ {
			//	objectName := fmt.Sprintf("Object%06d", objectCount)
			//	objectCount++
			//	createSimulatedObject(objectName, 500, s3Buckets[bucket], bucket, 1802, logger, fd, blockFileName, versionfd, true, true, false)
			//}
			// 2 objects 1803 bytes, 500 byte blocks, in pack, with pack list
			for objects := 0; objects < 3; objects++ {
				objectName := fmt.Sprintf("Object%06d", objectCount)
				objectCount++
				createSimulatedObject(objectName, 500, s3Buckets[bucket], bucket, 1805, logger, fd, blockFileName, versionfd, true, false, true)
			}
		}
	}
}
