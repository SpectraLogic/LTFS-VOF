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

func writeSimBlock(packFile *os.File, packName string, blockData []byte, bucket string, objectName string, versionName string, blockRange [2]int, logger *Logger) *PackEntry {
	startRange, err := packFile.Seek(0, io.SeekCurrent)
	if err != nil {
		logger.Fatal("Unable to get start range for packFile: ", packFile.Name())
	}
	currBlock := NewBlock("", bucket, objectName, versionName, blockData, int64(blockRange[0]), int64(blockRange[1]))
	WriteTLV(packFile, BLOCK, blockData, logger)
	WriteBlock(packFile, currBlock, logger)
	logger.Event("Wrote Block to Pack File: ", packFile.Name(), " Object: ", objectName, " Version: ", versionName, " Block Range: ", blockRange)
	endRange, err := packFile.Seek(0, io.SeekCurrent)
	if err != nil {
		logger.Fatal("Unable to get end range for packFile: ", packFile.Name())
	}

	packEntry := NewPackEntry(packName, int64(blockRange[0]), int64(blockRange[1]))
	packEntry.SetPhysicalLocation(packName, startRange, endRange)
	return packEntry
}

func writeSimPackList(packFile *os.File, packName string, packEntries Packs, objectName string, versionName string, logger *Logger) *PackReference {
	startRange, err := packFile.Seek(0, io.SeekCurrent)
	if err != nil {
		logger.Fatal("Unable to get start range for packlist")
	}
	packListRecord := NewPackListRecord(versionName, packEntries, logger)
	WriteTLV(packFile, PACKLIST, packListRecord.GetPackListEncoded(logger), logger)
	packListRecord.WritePackListRecord(packFile, logger)
	logger.Event("Wrote PackList to Pack File: ", packName, " Object: ", objectName, " Version: ", versionName)
	endRange, err := packFile.Seek(0, io.SeekCurrent)
	if err != nil {
		logger.Fatal("Unable to get end range for packlist")
	}
	packReference := NewPackReference(packName, startRange, endRange-startRange)

	return packReference
}

// create a simulated object of specified size, spanning blocksPerObject blocks, add it to s3 bucket if enabled
func createSimulatedObject(name string, blockSize int, s3sim *S3Simulator, bucket string, objectSize int, logger *Logger, packFile *os.File, packName string, versionFile *os.File, packing bool, backwards bool, usesPackList bool, deleted bool) {
	var randomData []byte
	randomData = make([]byte, objectSize)
	_, err := rand.Read(randomData)
	if err != nil {
		logger.Fatal("Unable to create random data")
	}

	if s3sim != nil {
		s3sim.Put(name, randomData)
		if deleted {
			s3sim.Delete(name)
		}
	}

	blockCount := int(math.Ceil(float64(objectSize) / float64(blockSize)))
	versionName := ulid.Make().String()

	blockRanges := getBlockRanges(objectSize, blockSize)

	var packEntries Packs
	var packReference *PackReference = nil
	//var startRange int64

	if packing {
		for blockIter := 0; blockIter < blockCount; blockIter++ {
			currBlockRange := blockRanges[blockIter]
			if backwards {
				currBlockRange = blockRanges[blockCount-blockIter-1]
			}

			currBlockData := randomData[currBlockRange[0]:currBlockRange[1]]
			packEntry := writeSimBlock(packFile, packName, currBlockData, bucket, name, versionName, currBlockRange, logger)

			if backwards || blockIter == 0 {
				packEntries = append(packEntries, packEntry)
			} else {
				packEntries[0].AddSequentialPacks(packEntry)
			}
		}
		randomData = nil // if the data is packed then it won't be in the version record

		if usesPackList {
			packReference = writeSimPackList(packFile, packName, packEntries, name, versionName, logger)
			packEntries = nil // if the packlist is referenced in the version record then the individual pack entries won't be
		}
	} else {
		packEntries = nil
	}

	vr, vrEncoded := NewVersionRecord(bucket, name, versionName, packEntries, randomData, packReference, deleted, false, logger)

	WriteTLV(versionFile, VERSION, vrEncoded, logger)
	vr.WriteVersionRecord(versionFile, logger)
	logger.Event("Wrote Version Record to Version File Object: ", name, " Version: ", versionName)
}

/*
create simulated blocks and version record for 2 objects, the packlist for each will be in a separate pack file from the blocks
Example: 700 byte objects, 500 byte blocks
Object1 consists of block1_1, block1_2, and packlist1
Object2 consists of block2_1, block2_2, and packlist2
first .blk file:
block1_1, block1_2, packlist2
second .blk file:
block2_1, block2_2, packlist1

When reading either .blk file first, we will get a case of reading blocks before the packlist that references them,
and we will also get a case of reading a packlist that references blocks in a different .blk file that has not been read yet.
*/
func createSimulatedObjectWithPacklistSeparate(objectName1, objectName2 string, blockSize int, s3sim *S3Simulator, bucket string, objectSize int, packName1, packName2 string, packFile1, packFile2, versionFile *os.File, logger *Logger) {
	var randomData [2][]byte
	randomData[0] = make([]byte, objectSize)
	randomData[1] = make([]byte, objectSize)

	versionName1 := ulid.Make().String()
	versionName2 := ulid.Make().String()

	_, err := rand.Read(randomData[0])
	if err != nil {
		logger.Fatal("Unable to create random data")
	}
	_, err = rand.Read(randomData[1])
	if err != nil {
		logger.Fatal("Unable to create random data")
	}

	if s3sim != nil {
		s3sim.Put(objectName1, randomData[0])
		s3sim.Put(objectName2, randomData[1])
	}

	blockRanges := getBlockRanges(objectSize, blockSize)

	var packEntries1 Packs
	var packEntries2 Packs

	for blockIter := 0; blockIter < len(blockRanges); blockIter++ {
		currBlockRange := blockRanges[blockIter]

		currBlockData1 := randomData[0][currBlockRange[0]:currBlockRange[1]]
		currBlockData2 := randomData[1][currBlockRange[0]:currBlockRange[1]]

		packEntry1 := writeSimBlock(packFile1, packName1, currBlockData1, bucket, objectName1, versionName1, currBlockRange, logger)
		packEntry2 := writeSimBlock(packFile2, packName2, currBlockData2, bucket, objectName2, versionName2, currBlockRange, logger)

		if blockIter == 0 {
			packEntries1 = append(packEntries1, packEntry1)
			packEntries2 = append(packEntries2, packEntry2)
		} else {
			packEntries1[0].AddSequentialPacks(packEntry1)
			packEntries2[0].AddSequentialPacks(packEntry2)
		}
	}

	packReference1 := writeSimPackList(packFile2, packName2, packEntries1, objectName1, versionName1, logger)
	packReference2 := writeSimPackList(packFile1, packName1, packEntries2, objectName2, versionName2, logger)

	vr1, vrEncoded1 := NewVersionRecord(bucket, objectName1, versionName1, nil, nil, packReference1, false, false, logger)
	vr2, vrEncoded2 := NewVersionRecord(bucket, objectName2, versionName2, nil, nil, packReference2, false, false, logger)

	WriteTLV(versionFile, VERSION, vrEncoded1, logger)
	vr1.WriteVersionRecord(versionFile, logger)
	logger.Event("Wrote Version Record to Version File Object: ", objectName1, " Version: ", versionName1)
	WriteTLV(versionFile, VERSION, vrEncoded2, logger)
	vr2.WriteVersionRecord(versionFile, logger)
	logger.Event("Wrote Version Record to Version File Object: ", objectName2, " Version: ", versionName2)
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
			// 1 object(s) 500 bytes, single block, in pack
			for objects := 0; objects < 1; objects++ {
				objectName := fmt.Sprintf("Object%06d", objectCount)
				objectCount++
				createSimulatedObject(objectName, 500, s3Buckets[bucket], bucket, 500, logger, fd, blockFileName, versionfd, true, false, false, false)
			}
			// 1 object(s) 100 bytes, in version record
			for objects := 0; objects < 1; objects++ {
				objectName := fmt.Sprintf("Object%06d", objectCount)
				objectCount++
				createSimulatedObject(objectName, 100, s3Buckets[bucket], bucket, 100, logger, fd, blockFileName, versionfd, false, false, false, false)
			}
			// 1 object(s) 1809 bytes, 500 byte blocks, in pack
			for objects := 0; objects < 1; objects++ {
				objectName := fmt.Sprintf("Object%06d", objectCount)
				objectCount++
				createSimulatedObject(objectName, 500, s3Buckets[bucket], bucket, 1809, logger, fd, blockFileName, versionfd, true, false, false, false)
			}
			// 1 object(s) 1801 bytes, 500 byte blocks, in pack, 3 versions each
			for objects := 0; objects < 1; objects++ {
				objectName := fmt.Sprintf("Object%06d", objectCount)
				objectCount++
				for versions := 0; versions < 3; versions++ {
					createSimulatedObject(objectName, 500, s3Buckets[bucket], bucket, 1801, logger, fd, blockFileName, versionfd, true, false, false, false)
				}
			}
			// 1 object(s) 1802 bytes, 500 byte blocks, in pack, written backwards in the pack file
			for objects := 0; objects < 1; objects++ {
				objectName := fmt.Sprintf("Object%06d", objectCount)
				objectCount++
				createSimulatedObject(objectName, 500, s3Buckets[bucket], bucket, 1802, logger, fd, blockFileName, versionfd, true, true, false, false)
			}
			// 1 object(s) 1803 bytes, 500 byte blocks, in pack, with pack list
			for objects := 0; objects < 1; objects++ {
				objectName := fmt.Sprintf("Object%06d", objectCount)
				objectCount++
				createSimulatedObject(objectName, 500, s3Buckets[bucket], bucket, 1803, logger, fd, blockFileName, versionfd, true, false, true, false)
			}
			// 1 object(s) 1900 bytes, 500 byte blocks, in pack, with pack list, backwards
			for objects := 0; objects < 1; objects++ {
				objectName := fmt.Sprintf("Object%06d", objectCount)
				objectCount++
				createSimulatedObject(objectName, 500, s3Buckets[bucket], bucket, 1900, logger, fd, blockFileName, versionfd, true, true, true, false)
			}
			//// 1 object(s) 1601 bytes, 500 byte blocks, in pack, deleted
			//for objects := 0; objects < 1; objects++ {
			//	objectName := fmt.Sprintf("Object%06d", objectCount)
			//	objectCount++
			//	createSimulatedObject(objectName, 500, s3Buckets[bucket], bucket, 1801, logger, fd, blockFileName, versionfd, true, false, false, true)
			//}

			// create 2 objects (2100 bytes, 500 byte blocksize) with object data in separate block files from the packlists that reference them to test reading blocks before packlist and visa versa
			blockFileName1 := ulid.Make().String()
			blockFileName2 := ulid.Make().String()
			fd1, err := os.Create(fmt.Sprintf("%stape%02d/%s.blk", SIMULATION_FILES, tape, blockFileName1))
			if err != nil {
				logger.Fatal("Unable to create simulated block file", err)
			}
			fd2, err := os.Create(fmt.Sprintf("%stape%02d/%s.blk", SIMULATION_FILES, tape, blockFileName2))
			if err != nil {
				logger.Fatal("Unable to create simulated block file", err)
			}
			defer fd1.Close()
			defer fd2.Close()
			objectName1 := fmt.Sprintf("Object%06d", objectCount)
			objectCount++
			objectName2 := fmt.Sprintf("Object%06d", objectCount)
			objectCount++
			createSimulatedObjectWithPacklistSeparate(objectName1, objectName2, 500, s3Buckets[bucket], bucket, 2100, blockFileName1, blockFileName2, fd1, fd2, versionfd, logger)
		}
	}
}
