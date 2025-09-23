// These is the startup program
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	. "ltfs-vof/tapehardware"
	. "ltfs-vof/utils"
)

// the format of the json config file
type Config struct {
	LibraryDevice    string                   `json:"LibraryDevice"`
	TapeDriveDevices map[int]*TapeDriveDevice `json:"TapeDevices"`
}

const DEFAULT_DB string = "./db"
const DEFAULT_BLOCK_CACHE string = "cache"
const DEFAULT_VERSION_CACHE string = "versions"
const DEFAULT_REGION string = "us-east-1"
const DEFAULT_CONFIG_FILE string = "config.json"
const DEFAULT_LOG_FILE string = "ltfs-vof.log"

func main() {
	// get the command line arguments
	simulate := flag.Bool("simulate", false, "Simulate a tape library ")
	s3enabled := flag.Bool("s3", false, "Write S3 as the storage backend")
	verify := flag.Bool("verify", false, "Verify that hardware matches config file")
	version := flag.Bool("version", false, "Find and copy version files")
	database := flag.Bool("database", false, "Create the database")
	read := flag.Bool("read", false, "Read the tapes")
	clean := flag.Bool("clean", false, "Clean the log")
	region := flag.String("region", DEFAULT_REGION, "AWS region to write s3 objects")
	configFile := flag.String("config", DEFAULT_CONFIG_FILE, "JSON file that defines tape drive mapping")
	logFile := flag.String("log", DEFAULT_LOG_FILE, "Log file for this run")
	// simulation options
	simTapes := flag.Int("simtapes", 0, "Create the number of simulated tapes specified")
	simBucket := flag.String("simbucket", "", "The S3 bucket to use to write simulated objects")
	simDrives := flag.Int("simdrives", 1, "Number of simulated tape drives")
	flag.Parse()

	// create the customer logger
	logger := NewLogger(*logFile, *clean)

	// if create simulated tapes then do it and exit
	if *simTapes != 0 {
		createSimulatedTapes(*simTapes, *simBucket, logger)
		return
	}

	// read the config file
	configData, err := ioutil.ReadFile(*configFile)
	if err != nil {
		logger.Fatal("Unable to read configuration file: ", *configFile)
	}
	// unmarshal the config file
	var config Config
	err = json.Unmarshal(configData, &config)
	if err != nil {
		logger.Fatal("Unable to json unmarshal the json config file: ", *configFile)
	}

	// run a verification of the config file
	if *verify {
		library := NewRealTapeLibrary(config.LibraryDevice, config.TapeDriveDevices)
		fmt.Println("\n\nLibrary: ", config.LibraryDevice)
		tapeDrives, tapeCartridges := library.Audit()

		fmt.Println("\nCartridge\tSlot")
		for _, tc := range tapeCartridges {
			fmt.Printf("%.18s%d\n", tc.Name(), tc.GetSlot())
		}
		// check that tape drives the
		fmt.Println("\nDrive\tSerial\t\tCart")
		drivePathFailure := false
		for d, td := range tapeDrives {
			// if does not exist on data path then exit
			sn, exists := td.SerialNumber()
			if !exists {
				drivePathFailure = true
				logger.Event("Device Path did not see drive: ", d)
				continue
			}
			cart, _ := td.GetCart()
			if cart != nil {
				fmt.Printf("%02d%16s%16s\n", d, sn, cart.Name())
			} else {
				fmt.Printf("%02d%16s%16s\n", d, sn, "No Cartridge")
			}
		}
		if drivePathFailure {
			logger.Fatal("Verification of config file: ", *configFile, " failed")
		}
	}

	// log arguments
	logger.Event("****RUN PARMS **** ")
	logger.Event("\n\tSIMULATE: ", *simulate, "\n\tVERSION: ", *version, "\n\tDATABASE: ", *database, "\n\tREAD: ", *read, "\n\tS3: ", *s3enabled)

	// select the library type used
	var library TapeLibrary
	if *simulate {
		library = NewTapeLibrarySimulator(SIMULATION_FILES, *simDrives, logger)
	} else {
		library = NewRealTapeLibrary(config.LibraryDevice, config.TapeDriveDevices)
	}
	dbManager := NewDBManager(DEFAULT_DB, DEFAULT_BLOCK_CACHE, *region, *clean, *s3enabled, logger)
	db := NewDatabase(DEFAULT_VERSION_CACHE, dbManager, library, logger)
	// if version is enabled create the database manager and get the version files
	if *version {
		logger.Event("*****COPYING VERSION FILES******")
		db.GetVersionFiles()
		logger.Event("****VERSION FILES COPIED******")
	}
	if *database {
		logger.Event("******BUILDING DATABASE*******")
		db.CreateDatabase()
		logger.Event("******ENDING BUILDING DATABASE*******")
	}

	// restore all the content if specified
	if *read {
		logger.Event("******READING BLOCK FILES*******")
		db.RestoreAll()
		logger.Event("******READ ALL BLOCK FILES*******")
	}
	/*
		// compare all source and corresponding target buckets
		if *compare || (*all && *s3enabled) {
			for _, bucket := range SIMULATED_BUCKET_NAMES {
				// list the target bucket
				result := s3source[bucket].CompareBuckets(s3target[bucket])
				if result {
					fmt.Println("S3 BUCKETS ARE THE SAME !!!")
				}
			}
		}
	*/
}
