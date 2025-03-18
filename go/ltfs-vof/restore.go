package main

import (
	"io"
	"log"
	. "ltfs-vof/tapehardware"
	_ "modernc.org/sqlite"
	"os"
	//	"time"
)

// THERE ARE THREE PLACES DATA CAN BE LOCATED
// 1. In the version record itself sotred in the "DATA" field
// 2. In one or more block files that are pointed to by the "PACKS" field in the version record
// 3. In one or more block files that are pointed to by a PACK RECORD, that exists in a block file, that is pointed to by the "Reference" field in the version record.

// Restore all versions, deletemarkers, essentially make s3 repository look like
// original
func RestoreAll(library TapeLibrary, dbManager *DBManager) {
	// For version records that have the "DATA: stored as part of the version record they
	// need to be scannned now so if they are the only version of an object they can be
	// processed
	versionRecordsWithData := dbManager.getVersionsInRecord()
	logEvent("Processing ", len(versionRecordsWithData), " version records that contain data")
	for _, versionID := range versionRecordsWithData {
		dbManager.processVersion(versionID)
	}

	// audit the library
	drives, tapes := library.Audit()
	logEvent("Audited Tape Library #cartridges: ", len(tapes), "  #drives: ", len(drives))

	// get resource allocation for tape drives
	driveReserve := NewResource(len(drives))
	logEvent("Reserving drives")

	// create a channel for goroutines to post to when completed
	tapeCompleteChannel := make(chan bool, len(tapes))

	// get an ordered list of tapes from oldest to newest
	// also get a list of the pack order on each tape from oldest to newest
	tapeCartridgeOrder, packsOrder := dbManager.GetTapePackOrder()
	logEvent("Cartridge Order: ", tapeCartridgeOrder)
	logEvent("Pack Order: ", packsOrder)
	for _, nextTape := range tapeCartridgeOrder {
		// get the tape from the list of tapes
		var tape TapeCartridge
		for _, c := range tapes {
			if c.Name() == nextTape {
				tape = c
				break
			}
		}
		// if didn't find it then failed
		if tape == nil {
			log.Fatal("Tape not found")
		}
		// reserve a drive
		driveNumber := driveReserve.Reserve()
		drive := drives[driveNumber]
		go func(tape TapeCartridge, drive TapeDrive) {

			// load tape into drive
			logEvent("Loading and Mounting tape: ", tape.Name(), " toDrive: ", drive.Name())
			status := library.Load(tape, drive)
			// if unable to load tape
			if !status {
				log.Fatal("Failed to load tape")
			}

			// mount the tape for LTFS get the pack files witht their full paths
			_, packFilePaths, status := drive.MountLTFS()
			if !status {
				log.Fatal("Failed to load tape")
			}

			// now read each pack from oldest to newest
			for _, pack := range packsOrder[tape.Name()] {
				// open the pack file
				logEvent("Open Pack File: ", packFilePaths[pack])

				file, err := os.Open(packFilePaths[pack])
				logEvent("Reading Pack, drive: ", drive.Name(), "  tape: ", tape.Name(), " pack: ", pack)
				if err != nil {
					log.Fatal(err)
				}
				defer file.Close()
				for {
					// get current location in file
					offset := currentFileLocation(file)

					tlv := ReadTLV(file)
					if tlv == nil {
						break
					}
					switch tlv.Tag() {
					case BLOCK:
						logEvent("TLV is Block type datalength = ", tlv.DataLength)
						block := ReadBlock(file, tlv.DataLength())
						// see if there is a version record associated with this block
						// if  there  is then cache the block and
						if dbManager.doesVersionRecordExist(block.GetVersion()) {
							// cache the block and send version to s3 if version is complete
							dbManager.WriteBlock(pack, offset, currentFileLocation(file), block)
							logEvent("Read & Wrote Block Pack:", pack, " offset: ", offset)
						} else {
							logEvent("Block not associated with a version record")
						}
					case PACKLIST:
						logEvent("TLV is packlist")
						packs := ReadPackListRecord(file, tlv.DataLength())
						if packs == nil {
							log.Fatal("unable to read packlist")
						}
						logEvent("Processing Pack List", pack, " offset: ", offset)
						dbManager.ProcessPackList(pack, offset, packs)
					default:
						log.Fatal("TLV not of Version or Version Delete type")
					}
				}
			}
			// unload and dismount the tape
			logEvent("Dismounting and Unloading tape: ", tape.Name(), " toDrive: ", drive.Name())
			drive.Unmount()
			library.Unload(drive)
			// release the drive and notify the channel
			driveReserve.Release(driveNumber)
			tapeCompleteChannel <- true

		}(tape, drive)

	}
	// wait for all tapes to complete and stop the resource manager
	for i := 0; i < len(tapes); i++ {
		<-tapeCompleteChannel
	}
	close(tapeCompleteChannel)
	driveReserve.Stop()

}
func currentFileLocation(file *os.File) int64 {
	offset, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Fatal("unable to size file: ", err)
	}
	return offset
}
