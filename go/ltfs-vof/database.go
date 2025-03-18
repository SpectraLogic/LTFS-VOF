package main

import (
	"github.com/oklog/ulid/v2"
	"io"
	"log"
	. "ltfs-vof/tapehardware"
	_ "modernc.org/sqlite"
	"os"
	"sort"
	"strings"
)

func getVersionFiles(library TapeLibrary, dbManager *DBManager) {
	os.RemoveAll(DEFAULT_VERSION_CACHE)
	os.Mkdir(DEFAULT_VERSION_CACHE, 0755)

	// audit the library
	drives, tapes := library.Audit()

	// get resource allocation for tape drives
	driveReserve := NewResource(len(drives))

	// create a channel for goroutines to post to when completed
	tapeCompleteChannel := make(chan bool, len(tapes))

	for _, tape := range tapes {
		logEvent("Look for Version Files From Tape ", tape.Name())
		// get a drive to mount the tape
		driveNumber := driveReserve.Reserve()
		drive := drives[driveNumber]
		// mount tape into drive as a go routine such that multiple tape drives can read tapes concurrently
		go func(drive TapeDrive, driveNumber int, tape TapeCartridge) {
			logEvent("Loading Tape ", tape.Name(), " into Drive ", drive.Name())
			status := library.Load(tape, drive)
			// if unable to mount tape
			if !status {
				log.Fatal("Failed to mount tape")
			}
			// load the tape for LTFS
			logEvent("Mounting Tape with LTFS", tape.Name(), " into Drive ", drive.Name())
			versionFiles, blockFiles, status := drive.MountLTFS()
			if !status {
				log.Fatal("Failed to mount tape")
			}
			for key, path := range versionFiles {

				// write the version file to the version cache directory
				logEvent("Found Version File", key, " Path: ", path)
				sourceFile, err := os.Open(path)
				if err != nil {
					log.Fatal("unable to open version file: ", path)
				}
				defer sourceFile.Close()
				// create the version cache file which is the base name of the version file
				cacheFileName := DEFAULT_VERSION_CACHE + "/" + key
				logEvent("Cache File Name: ", key)
				cacheFile, err := os.Create(cacheFileName)
				if err != nil {
					log.Fatal("unable to create version cache file", cacheFile)
				}
				defer cacheFile.Close()
				// copy the version file to the version cache file
				logEvent("Copy Version File", sourceFile, " File: ", cacheFile)
				_, err = io.Copy(cacheFile, sourceFile)
				if err != nil {
					log.Fatal("unable to copy version file to version cache file", err)
				}
			}
			// put all blockfiles into database
			for key, path := range blockFiles {
				// put each .blk file into tape table
				logEvent("Found Block File Tape: ", tape.Name(), " Key: ", key, "  Path: ", path)
				dbManager.AddTapeToPack(key, tape.Name())
			}
			// unmount and unload the drive
			logEvent("dismounting and unloading tape", tape.Name(), " from Drive ", drive.Name())
			drive.Unmount()
			library.Unload(drive)

			// free up the resource and post to the complete channel
			driveReserve.Release(driveNumber)
			tapeCompleteChannel <- true
		}(drive, driveNumber, tape)
	}
	// wait for all tapes to be processed and stop the resource manager
	for i := 0; i < len(tapes); i++ {
		<-tapeCompleteChannel
	}
	driveReserve.Stop()
}
func createDatabase(dbManager *DBManager) {
	// sort the version files from oldest to newest based on the their ULIDS
	versionFileUlids := sortVersionFiles()

	// look for metadata records and send a list back from oldest to newest of files that need
	// to be processed
	versionFilesToProcess := findVersionFilesToProcess(versionFileUlids)

	for _, versionFile := range versionFilesToProcess {
		// open the oldest version file
		versionFileName := DEFAULT_VERSION_CACHE + "/" + versionFile.String()
		file, err := os.Open(versionFileName)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()
		logEvent("Processing version file: ", versionFileName)

		// read TLV's followed by blocks
		for {
			logEvent("Reading TLV ")
			tlv := ReadTLV(file)
			if tlv == nil {
				logEvent("End of Processing version file: ", versionFileName)
				break
			}
			switch tlv.Tag() {
			case VERSION:
				v := ReadVersionRecord(file, tlv.DataLength())
				logEvent("Reading Version Record, Object Name ", v.VersionID.Object, " File Name: ", versionFileName)
				// insert the version into the database
				dbManager.AddVersion(v)
			case DELETEVERSION:
				logEvent("Reading Delete Version Record, version file: ", versionFileName)
				delete := ReadVersionRecord(file, tlv.DataLength())
				// insert the version into the database
				dbManager.DeleteVersion(delete.GetVersion())
			// ignore duplicate meta files
			case METAFILE:
				logEvent("Ignoring already processed metafile in version file: ", versionFileName)
				ReadMetaFile(file, tlv.DataLength())
			default:
				logEvent("Invalid TLV: ", tlv, " in version file: ", versionFileName)
			}
		}
	}
	logEvent("Second END")
}

// sort the version files from oldest to newest
func sortVersionFiles() []ulid.ULID {

	// create a list of the version files
	var versionFileUlids []ulid.ULID
	// read the version cache directory
	files, err := os.ReadDir(DEFAULT_VERSION_CACHE)
	if err != nil {
		log.Fatal(err)
	}
	// remove the .ver suffix from the file name
	for _, file := range files {
		versionUlid, _ := getTimeFromID(file.Name())
		versionFileUlids = append(versionFileUlids, versionUlid)
	}

	// sort the version files based on the time
	sort.Slice(versionFileUlids, func(i, j int) bool {
		return versionFileUlids[i].Time() < versionFileUlids[j].Time()
	})
	logEvent("Sorted Version Files: ", versionFileUlids)
	return versionFileUlids
}
func findVersionFilesToProcess(versionFileUlids []ulid.ULID) []ulid.ULID {
	// will need to start at the newest version file and work backwards
	// looking for a metafile that preculdes older files (i.e. full backup)
	logEvent("LOOKING FOR METAFILES IN VERSION FILES STARTING FROM NEWEST TO OLDEST")
	// make a copy of the version file ulids
	originalVersionFileUlids := make([]ulid.ULID, len(versionFileUlids))
	copy(originalVersionFileUlids, versionFileUlids)
	for i := len(versionFileUlids) - 1; i >= 0; i-- {
		// open the version file
		versionFileName := DEFAULT_VERSION_CACHE + "/" + versionFileUlids[i].String()
		file, err := os.Open(versionFileName)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()
		logEvent("Checking version file for Metafile: ", versionFileName)

		// only going to read first TLV to determine if metafile exists
		tlv := ReadTLV(file)
		if tlv == nil {
			// continue to the next version file
			continue
		}
		switch tlv.Tag() {
		case METAFILE:
			// if a metafile is found then this is the first version file to process
			metaFile := ReadMetaFile(file, tlv.DataLength())
			// if metafile is nil then their is no metafile record to process
			if metaFile == nil {
				continue
			}
			logEvent("Found Request for Newest Meta File To Process: ", metaFile.Oldest)
			oldestFileToProcess := metaFile.GetOldest()
			// find it in the ulid list
			for j, ulid := range versionFileUlids {
				if strings.HasPrefix(ulid.String()+".ver", oldestFileToProcess) {
					// found the oldest remove any that are older
					versionList := versionFileUlids[j:]
					logEvent("Found Newest Meta File To Process and Created version list: ", versionList)
					return versionList
				}
			}
			// can not find the oldest version file is an error then processs entire set
			// of version files
			logEvent("***NOT ABLE TO FIND NEWEST VERSION FULL FILE***")
			return originalVersionFileUlids
		default:
			continue
		}
	}
	// didn't find any metafiles so process all version files
	return originalVersionFileUlids
}

// get time from a file with a ulid name followed
func getTimeFromID(filename string) (ulid.ULID, uint64) {
	// need to remove suffix from filename
	name := strings.TrimSuffix(filename, ".blk")
	// if not .blk suffix then trim .ver
	if len(name) == len(filename) {
		name = strings.TrimSuffix(filename, ".ver")
	}
	// now create ULID from name
	ulid, err := ulid.Parse(name)
	if err != nil {
		log.Fatal("Unable to ulid parse: ", name)
	}
	return ulid, ulid.Time()
}
