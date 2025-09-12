package main

import (
	"fmt"
	"github.com/oklog/ulid/v2"
	"io"
	. "ltfs-vof/tapehardware"
	. "ltfs-vof/utils"
	_ "modernc.org/sqlite"
	"os"
	"slices"
	"sort"
	"strings"
)

type Database struct {
	versionCache string
	dbManager    *DBManager
	library      TapeLibrary
	logger       *Logger
}

func NewDatabase(versionCache string, dbManager *DBManager, library TapeLibrary, logger *Logger) *Database {
	return &Database{
		versionCache: versionCache,
		dbManager:    dbManager,
		library:      library,
		logger:       logger,
	}
}

func (db *Database) GetVersionFiles() {
	os.RemoveAll(db.versionCache)
	os.Mkdir(db.versionCache, 0755)

	// audit the library
	drives, carts := db.library.Audit()

	// get resource allocation for tape drives
	driveReserve := NewResource(len(drives))

	// create a channel for goroutines to post to when completed
	completeChannel := make(chan bool)

	// process all tapes that are currently in drives
	count := 0
	for number, drive := range drives {

		// continue if no cart in drive
		cart, exists := drive.GetCart()
		if !exists {
			continue
		}
		// cart in drive process it
		count++
		go db.readVersionFiles(drive, number, cart, completeChannel, nil)
		// remove cartridge from total carts list
		carts = slices.DeleteFunc(carts, func(nextCart TapeCartridge) bool {
			return nextCart == cart
		})
	}
	// wait for all carts in drive to be processed
	for i := 0; i < count; i++ {
		<-completeChannel
	}

	// process carts that are not in drives
	count = 0
	for _, cart := range carts {
		db.logger.Event("Look for Version Files From Tape ", cart.Name())

		// get a drive to mount the tape
		driveNumber := driveReserve.Reserve()
		drive := drives[driveNumber]

		// load the cart into the drive
		status := db.library.Load(cart, drive)
		if !status {
			db.logger.Fatal("Failed to load tape")
		}

		// use go routine to keep all drives busy
		count += 1
		go db.readVersionFiles(drive, driveNumber, cart, completeChannel, driveReserve)
	}
	// wait for all carts not in drive to be processed
	for i := 0; i < count; i++ {
		<-completeChannel
	}
	driveReserve.Stop()
}
func (db *Database) readVersionFiles(drive TapeDrive, driveNumber int, tape TapeCartridge, completeChannel chan bool, driveReserve *Resource) {
	// mount LTFS on the tape
	sn, exists := drive.SerialNumber()
	if !exists {
		db.logger.Fatal("Unable to get drive serial number")
	}
	db.logger.Event("Mounting Tape with LTFS", tape.Name(), " into Drive ", sn)
	versionFiles, blockFiles, status := drive.MountLTFS()
	if !status {
		db.logger.Fatal("Failed to mount tape")
	}
	for key, path := range versionFiles {

		// write the version file to the version cache directory
		db.logger.Event("Found Version File", key, " Path: ", path)
		sourceFile, err := os.Open(path)
		if err != nil {
			db.logger.Fatal("unable to open version file: ", path)
		}
		defer sourceFile.Close()
		// create the version cache file which is the base name of the version file
		cacheFileName := DEFAULT_VERSION_CACHE + "/" + key
		db.logger.Event("Cache File Name: ", key)
		cacheFile, err := os.Create(cacheFileName)
		if err != nil {
			db.logger.Fatal("unable to create version cache file", cacheFile)
		}
		defer cacheFile.Close()
		// copy the version file to the version cache file
		db.logger.Event("Copy Version File", sourceFile, " File: ", cacheFile)
		_, err = io.Copy(cacheFile, sourceFile)
		if err != nil {
			db.logger.Fatal("unable to copy version file to version cache file", err)
		}
	}
	// put all blockfiles into database
	for key, path := range blockFiles {
		// put each .blk file into tape table
		db.logger.Event("Found Block File Tape: ", tape.Name(), " Key: ", key, "  Path: ", path)
		db.dbManager.AddTapeToPack(key, tape.Name())
	}
	// unmount and unload the drive
	db.logger.Event("dismounting and unloading tape", tape.Name(), " from Drive ", sn)
	drive.Unmount()
	db.library.Unload(drive)

	// free up the resource and post to the complete channel
	if driveReserve != nil {
		driveReserve.Release(driveNumber)
	}
	completeChannel <- true
}
func (db *Database) CreateDatabase() {
	// sort the version files from oldest to newest based on the their ULIDS
	versionFileUlids := db.sortVersionFiles()

	// look for metadata records and send a list back from oldest to newest of files that need
	// to be processed
	versionFilesToProcess := db.findVersionFilesToProcess(versionFileUlids)

	for _, versionFile := range versionFilesToProcess {
		// open the oldest version file
		versionFileName := DEFAULT_VERSION_CACHE + "/" + versionFile.String()
		file, err := os.Open(versionFileName)
		if err != nil {
			db.logger.Fatal(err)
		}
		defer file.Close()
		db.logger.Event("Processing version file: ", versionFileName)

		// read TLV's followed by blocks
		for {
			db.logger.Event("Reading TLV ")
			tlv := ReadTLV(file, db.logger)
			fmt.Println("TLV: ", tlv)
			if tlv == nil {
				db.logger.Event("End of Processing version file: ", versionFileName)
				break
			}
			switch tlv.Tag() {
			case VERSION:
				v := ReadVersionRecord(file, tlv.DataLength(), db.logger)
				db.logger.Event("Reading Version Record, Object Name ", v.VersionID.Object, " File Name: ", versionFileName)
				// insert the version into the database
				db.dbManager.AddVersion(v)
			case DELETEVERSION:
				db.logger.Event("Reading Delete Version Record, version file: ", versionFileName)
				delete := ReadVersionRecord(file, tlv.DataLength(), db.logger)
				// insert the version into the database
				db.dbManager.DeleteVersion(delete.GetVersion())
			// ignore duplicate meta files
			case METAFILE:
				db.logger.Event("Ignoring already processed metafile in version file: ", versionFileName)
				ReadMetaFile(file, tlv.DataLength(), db.logger)
			default:
				db.logger.Event("Invalid TLV: ", tlv, " in version file: ", versionFileName)
			}
		}
	}
}

// sort the version files from oldest to newest
func (db *Database) sortVersionFiles() []ulid.ULID {

	// create a list of the version files
	var versionFileUlids []ulid.ULID
	// read the version cache directory
	files, err := os.ReadDir(DEFAULT_VERSION_CACHE)
	if err != nil {
		db.logger.Fatal(err)
	}
	// remove the .ver suffix from the file name
	for _, file := range files {
		versionUlid, _ := GetTimeFromID(file.Name(), db.logger)
		versionFileUlids = append(versionFileUlids, versionUlid)
	}

	// sort the version files based on the time
	sort.Slice(versionFileUlids, func(i, j int) bool {
		return versionFileUlids[i].Time() < versionFileUlids[j].Time()
	})
	db.logger.Event("Sorted Version Files: ", versionFileUlids)
	return versionFileUlids
}
func (db *Database) findVersionFilesToProcess(versionFileUlids []ulid.ULID) []ulid.ULID {
	// will need to start at the newest version file and work backwards
	// looking for a metafile that preculdes older files (i.e. full backup)
	db.logger.Event("LOOKING FOR METAFILES IN VERSION FILES STARTING FROM NEWEST TO OLDEST")
	// make a copy of the version file ulids
	originalVersionFileUlids := make([]ulid.ULID, len(versionFileUlids))
	copy(originalVersionFileUlids, versionFileUlids)
	for i := len(versionFileUlids) - 1; i >= 0; i-- {
		// open the version file
		versionFileName := DEFAULT_VERSION_CACHE + "/" + versionFileUlids[i].String()
		file, err := os.Open(versionFileName)
		if err != nil {
			db.logger.Fatal(err)
		}
		defer file.Close()
		db.logger.Event("Checking version file for Metafile: ", versionFileName)

		// only going to read first TLV to determine if metafile exists
		tlv := ReadTLV(file, db.logger)
		if tlv == nil {
			// continue to the next version file
			continue
		}
		switch tlv.Tag() {
		case METAFILE:
			// if a metafile is found then this is the first version file to process
			metaFile := ReadMetaFile(file, tlv.DataLength(), db.logger)
			// if metafile is nil then their is no metafile record to process
			if metaFile == nil {
				continue
			}
			db.logger.Event("Found Request for Newest Meta File To Process: ", metaFile.Oldest)
			oldestFileToProcess := metaFile.GetOldest()
			// find it in the ulid list
			for j, ulid := range versionFileUlids {
				if strings.HasPrefix(ulid.String()+".ver", oldestFileToProcess) {
					// found the oldest remove any that are older
					versionList := versionFileUlids[j:]
					db.logger.Event("Found Newest Meta File To Process and Created version list: ", versionList)
					return versionList
				}
			}
			// can not find the oldest version file is an error then processs entire set
			// of version files
			db.logger.Event("***NOT ABLE TO FIND NEWEST VERSION FULL FILE***")
			return originalVersionFileUlids
		default:
			continue
		}
	}
	// didn't find any metafiles so process all version files
	return originalVersionFileUlids
}
