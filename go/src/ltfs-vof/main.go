// These is the startup program
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	. "ltfs-vof/tapehardware"
	. "ltfs-vof/utils"
	"strings"
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
	verify := flag.Bool("verify", false, "Verify that the config file matches the hardware")
	version := flag.Bool("version", false, "Find and copy version files")
	database := flag.Bool("database", false, "Create the database")
	read := flag.Bool("read", false, "Read the tapes")
	clean := flag.Bool("clean", false, "Clean the log and database file")
	region := flag.String("region", DEFAULT_REGION, "region or endpoint to write s3 objects")
	configFile := flag.String("config", DEFAULT_CONFIG_FILE, "JSON file that defines tape drive mapping")
	logFile := flag.String("log", DEFAULT_LOG_FILE, "Log file for this run")
	versioned := flag.Bool("versioning", true, "set to false if customer buckets are non versioned")
	s3 := flag.Bool("s3", true, "Write objects to S3 buckets ")
	// simulation options
	simulate := flag.Bool("simulate", false, "Simulate a tape library ")
	simTapes := flag.Int("simtapes", 0, "Create the number of simulated tapes specified")
	simS3 := flag.Bool("sims3", false, "Write simulated objects to S3 buckets ")
	simDrives := flag.Int("simdrives", 1, "Number of simulated tape drives")
	simBlocks := flag.Int("simblocks", 1, "Number of blocks per object")
	// simS3compare := flag.Bool("simscompare", false, "Verify that simulation bucket and the customer output bucket match")
	var simBuckets stringSlice
	flag.Var(&simBuckets, "simbucket", "simbucket may be repeated to create multiple simulation buckets")
	flag.Parse()

	// create the customer logger
	logger := NewLogger(*logFile, *clean)

	// if create simulated tapes then do it and exit
	if *simTapes != 0 {
		// the source bucket for the simulator will be prefixed with source
		logger.Event("****CREATING SIMULATED TAPES AND BUCKETS **** ")
		createSimulatedTapes(*simTapes, *simS3, simBuckets.Slice(), *simBlocks, *versioned, logger)
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
	logger.Event("\n\tSIMULATE: ", *simulate, "\n\tVERSION: ", *version, "\n\tDATABASE: ", *database, "\n\tREAD: ", *read, "\n\tS3: ", *simS3)

	// select the library type used
	var library TapeLibrary
	if *simulate {
		library = NewTapeLibrarySimulator(SIMULATION_FILES, *simDrives, logger)
	} else {
		library = NewRealTapeLibrary(config.LibraryDevice, config.TapeDriveDevices)
	}
	dbManager := NewDBManager(DEFAULT_DB, DEFAULT_BLOCK_CACHE, *region, *clean, *s3, *versioned, *simulate, logger)
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

// stringSlice is a custom type to hold a slice of strings
type stringSlice []string

// String implements the flag.Value interface's String method
func (s *stringSlice) String() string {
	return strings.Join(*s, ",")
}

// Set implements the flag.Value interface's Set method
func (s *stringSlice) Set(value string) error {
	*s = append(*s, value)
	return nil
}
func (s *stringSlice) Slice() []string {
	return []string(*s)
}
