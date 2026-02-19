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
			logger.Event("Wrote Block to Pack File: ", packName, " Object: ", name, " Version: ", versionName, " Block Range: ", currBlockRange)

			endRange, err := packFile.Seek(0, io.SeekCurrent)
			if err != nil {
				logger.Fatal("Unable to get end range")
			}
			packEntry := NewPackEntry(packName, int64(currBlockRange[0]), int64(currBlockRange[1]))
			//logger.Event("Confirming Block Range:", currBlockRange, "matches Logical Range:", packEntry.GetLogicalStart(), packEntry.GetLogicalStart()+packEntry.GetLogicalLength())
			packEntry.SetPhysicalLocation(packName, startRange, endRange)
			// add to pack entries if backwards or first block, else append it to the first pack entry (only pack entry needed for sequential blocks)
			if backwards || blockIter == 0 {
				packEntries = append(packEntries, packEntry)
			} else {
				packEntries[0].AddSequentialPacks(packEntry)
			}
		}
		randomData = nil
	} else {
		packEntries = nil
	}
	if usesPackList {
		startRange, err = packFile.Seek(0, io.SeekCurrent)
		if err != nil {
			logger.Fatal("Unable to get start range for packlist")
		}
		packListRecord := NewPackListRecord(versionName, packEntries, logger)
		WriteTLV(packFile, PACKLIST, packListRecord.GetPackListEncoded(logger), logger)
		packListRecord.WritePackListRecord(packFile, logger)
		logger.Event("Wrote PackList to Pack File: ", packName, " Object: ", name, " Version: ", versionName)
		endRange, err := packFile.Seek(0, io.SeekCurrent)
		if err != nil {
			logger.Fatal("Unable to get end range for packlist")
		}
		packReference = NewPackReference(packName, startRange, endRange-startRange)

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
func createSimulatedObjectWithPacklistSeparate(object1Name, object2Name string, blockSize int, s3sim *S3Simulator, bucket string, objectSize int, packName1, packName2 string, packFile1, packFile2, versionFile *os.File, logger *Logger) {
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
		s3sim.Put(object1Name, randomData[0])
		s3sim.Put(object2Name, randomData[1])
	}

	blockRanges := getBlockRanges(objectSize, blockSize)

	var packEntries1 Packs
	var packEntries2 Packs

	for blockIter := 0; blockIter < len(blockRanges); blockIter++ {
		currBlockRange := blockRanges[blockIter]

		startRange1, err := packFile1.Seek(0, io.SeekCurrent)
		if err != nil {
			logger.Fatal("Unable to get start range for packfile:", packFile1.Name())
		}
		startRange2, err := packFile2.Seek(0, io.SeekCurrent)
		if err != nil {
			logger.Fatal("Unable to get start range for packfile:", packFile2.Name())
		}

		currBlockData1 := randomData[0][currBlockRange[0]:currBlockRange[1]]
		currBlock1 := NewBlock("", bucket, object1Name, versionName1, currBlockData1, int64(currBlockRange[0]), int64(currBlockRange[1]))
		currBlockData2 := randomData[1][currBlockRange[0]:currBlockRange[1]]
		currBlock2 := NewBlock("", bucket, object2Name, versionName2, currBlockData2, int64(currBlockRange[0]), int64(currBlockRange[1]))

		// Write block for object1 to packfile1
		WriteTLV(packFile1, BLOCK, currBlockData1, logger)
		WriteBlock(packFile1, currBlock1, logger)
		logger.Event("Wrote Block to Pack File: ", packFile1.Name(), " Object: ", object1Name, " Block Range: ", currBlockRange)
		// Write block for object2 to packfile2
		WriteTLV(packFile2, BLOCK, currBlockData2, logger)
		WriteBlock(packFile2, currBlock2, logger)
		logger.Event("Wrote Block to Pack File: ", packFile2.Name(), " Object: ", object2Name, " Block Range: ", currBlockRange)

		endRange1, err := packFile1.Seek(0, io.SeekCurrent)
		if err != nil {
			logger.Fatal("Unable to get end range for block 1")
		}
		endRange2, err := packFile1.Seek(0, io.SeekCurrent)
		if err != nil {
			logger.Fatal("Unable to get end range for block 2")
		}

		packEntry1 := NewPackEntry(packName1, int64(currBlockRange[0]), int64(currBlockRange[1]))
		packEntry1.SetPhysicalLocation(packName1, startRange1, endRange1)

		packEntry2 := NewPackEntry(packName2, int64(currBlockRange[0]), int64(currBlockRange[1]))
		packEntry2.SetPhysicalLocation(packName2, startRange2, endRange2)

		if blockIter == 0 {
			packEntries1 = append(packEntries1, packEntry1)
			packEntries2 = append(packEntries2, packEntry2)
		} else {
			packEntries1[0].AddSequentialPacks(packEntry1)
			packEntries2[0].AddSequentialPacks(packEntry2)
		}
	}
	startRange1, err := packFile1.Seek(0, io.SeekCurrent)
	if err != nil {
		logger.Fatal("Unable to get start range for packlist:", packFile1.Name())
	}
	startRange2, err := packFile2.Seek(0, io.SeekCurrent)
	if err != nil {
		logger.Fatal("Unable to get start range for packlist:", packFile2.Name())
	}

	packListRecord1 := NewPackListRecord(versionName1, packEntries1, logger)
	packListRecord2 := NewPackListRecord(versionName2, packEntries2, logger)

	// Object1 packlist goes in packfile2 and Object2 packlist goes in packfile1 to create cross reference of packlists and blocks in different packfiles
	WriteTLV(packFile2, PACKLIST, packListRecord1.GetPackListEncoded(logger), logger)
	packListRecord1.WritePackListRecord(packFile2, logger)
	logger.Event("Wrote PackList to Pack File: ", packFile2.Name(), " Object: ", object1Name, " Version: ", versionName1)

	WriteTLV(packFile1, PACKLIST, packListRecord2.GetPackListEncoded(logger), logger)
	packListRecord2.WritePackListRecord(packFile1, logger)
	logger.Event("Wrote PackList to Pack File: ", packFile1.Name(), " Object: ", object2Name, " Version: ", versionName2)

	endRange1, err := packFile1.Seek(0, io.SeekCurrent)
	if err != nil {
		logger.Fatal("Unable to get end range for packlist:", packFile1.Name())
	}
	endRange2, err := packFile2.Seek(0, io.SeekCurrent)
	if err != nil {
		logger.Fatal("Unable to get end range for packlist:", packFile2.Name())
	}

	packReference1 := NewPackReference(packName2, startRange2, endRange2-startRange2)
	packReference2 := NewPackReference(packName1, startRange1, endRange1-startRange1)

	vr1, vrEncoded1 := NewVersionRecord(bucket, object1Name, versionName1, nil, nil, packReference1, false, false, logger)
	vr2, vrEncoded2 := NewVersionRecord(bucket, object2Name, versionName2, nil, nil, packReference2, false, false, logger)

	WriteTLV(versionFile, VERSION, vrEncoded1, logger)
	vr1.WriteVersionRecord(versionFile, logger)
	logger.Event("Wrote Version Record to Version File Object: ", object1Name, " Version: ", versionName1)
	WriteTLV(versionFile, VERSION, vrEncoded2, logger)
	vr2.WriteVersionRecord(versionFile, logger)
	logger.Event("Wrote Version Record to Version File Object: ", object2Name, " Version: ", versionName2)
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
