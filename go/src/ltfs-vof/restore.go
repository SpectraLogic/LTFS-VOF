package main

import (
	"fmt"
	"io"
	. "ltfs-vof/tapehardware"
	. "ltfs-vof/utils"
	_ "modernc.org/sqlite"
	"os"
)

// THERE ARE THREE PLACES DATA CAN BE LOCATED
// 1. In the version record itself sotred in the "DATA" field
// 2. In one or more block files that are pointed to by the "PACKS" field in the version record
// 3. In one or more block files that are pointed to by a PACK RECORD, that exists in a block file, that is pointed to by the "Reference" field in the version record.

// Restore all versions, deletemarkers, essentially make s3 repository look like
// original
func (db *Database) RestoreAll() {
	// For version records that have the "DATA: stored as part of the version record they
	// need to be scannned now so if they are the only version of an object they can be
	// processed
	versionRecordsWithData := db.dbManager.getVersionsInRecord()
	db.logger.Event("Processing ", len(versionRecordsWithData), " version records that contain data")
	for _, versionID := range versionRecordsWithData {
		db.logger.Event("Writing in Record Data for VersionID: ", versionID)
		db.dbManager.processVersion(versionID)
	}

	// audit the library
	drives, tapes := db.library.Audit()
	db.logger.Event("Audited Tape Library #cartridges: ", len(tapes), "  #drives: ", len(drives))

	// get resource allocation for tape drives
	driveReserve := NewResource(len(drives))
	db.logger.Event("Reserving drives")

	// create a channel for goroutines to post to when completed
	tapeCompleteChannel := make(chan bool, len(tapes))

	// get an ordered list of tapes from oldest to newest
	// also get a list of the pack order on each tape from oldest to newest
	tapeCartridgeOrder, packsOrder := db.dbManager.GetTapePackOrder()
	db.logger.Event("Cartridge Order: ", tapeCartridgeOrder)
	db.logger.Event("Pack Order: ", packsOrder)
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
			db.logger.Fatal("Tape not found")
		}
		// reserve a drive
		driveNumber := driveReserve.Reserve()
		drive := drives[driveNumber]
		go func(tape TapeCartridge, drive TapeDrive) {

			// load tape into drive
			sn, exists := drive.SerialNumber()
			if !exists {
				db.logger.Fatal("Drive has not serial number:")
			}

			db.logger.Event("Loading and Mounting tape: ", tape.Name(), " toDrive: ", sn)
			status := db.library.Load(tape, drive)
			// if unable to load tape
			if !status {
				db.logger.Fatal("Failed to load tape drive: ", sn)
			}

			// mount the tape for LTFS get the pack files witht their full paths
			_, packFilePaths, status := drive.MountLTFS()
			if !status {
				db.logger.Fatal("Failed to mount LTFS tape drive: ", sn)
			}

			// now read each pack from oldest to newest
			fmt.Println("PacksOrder: ", packsOrder)
			for _, pack := range packsOrder[tape.Name()] {
				fmt.Println("Pack: ", pack, "Path: ", packFilePaths[pack])
				// open the pack file
				db.logger.Event("Open Pack File: ", packFilePaths[pack])

				fmt.Println("packFilePaths: ", packFilePaths)
				db.logger.Event("Open Pack File: ", packFilePaths[pack])

				file, err := os.Open(packFilePaths[pack])
				if err != nil {
					db.logger.Fatal("Unable to get open pack file: ", packFilePaths[pack])
				}
				db.logger.Event("Reading Pack, drive: ", sn, "  tape: ", tape.Name(), " pack: ", pack)
				defer file.Close()
				for {
					// get current location in file
					offset := db.currentFileLocation(file)

					tlv := ReadTLV(file, db.logger)
					if tlv == nil {
						break
					}
					fmt.Println("Read TLV at offset: ", offset, " tag: ", tlv.Tag(), " length: ", tlv.DataLength())
					switch tlv.Tag() {
					case BLOCK:
						db.logger.Event("TLV is Block type datalength = ", tlv.DataLength())
						block := ReadBlock(file, tlv.DataLength(), db.logger)
						// see if there is a version record associated with this block
						// if  there  is then cache the block and
						if db.dbManager.doesVersionRecordExist(block.GetVersion()) {
							// cache the block and send version to s3 if version is complete
							db.dbManager.WriteBlock(pack, offset, db.currentFileLocation(file), block)
							db.logger.Event("Read & Wrote Block Pack:", pack, " offset: ", offset)
						} else {
							db.logger.Event("Block not associated with a version record")
						}
					case PACKLIST:
						db.logger.Event("TLV is packlist")
						packs := ReadPackListRecord(file, tlv.DataLength(), db.logger)
						if packs == nil {
							db.logger.Fatal("unable to read packlist")
						}
						db.logger.Event("Processing Pack List", pack, " offset: ", offset)
						db.dbManager.ProcessPackList(pack, offset, packs)
					default:
						db.logger.Fatal("TLV not of BLOCK or PACKLIST type")
					}
				}
			}
			db.logger.Event("Dismounting and Unloading tape: ", tape.Name(), " toDrive: ", sn)
			drive.Unmount()
			db.library.Unload(drive)
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
func (db *Database) currentFileLocation(file *os.File) int64 {
	offset, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		db.logger.Fatal("unable to size file: ", err)
	}
	return offset
}
