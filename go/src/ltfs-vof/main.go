// These is the startup program
package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	. "ltfs-vof/tapehardware"
	"os"
	"runtime"
	"time"
	"fmt"
)

// the format of the json config file
type Config struct {
	LibraryDevice    string                   `json:"LibraryDevice"`
	TapeDriveDevices map[int]*TapeDriveDevice `json:"TapeDevices"`
}

const DEFAULT_DB string = "./db"
const DEFAULT_BLOCK_CACHE string = "cache"
const DEFAULT_VERSION_CACHE string = "versions"
const DEFAULT_REGION string = "us-west-1"
const DEFAULT_CONFIG_FILE string = "config.json"

func main() {
	// get the command line arguments
	all := flag.Bool("all", false, "Perform all operaions sequentially")
	simulate := flag.Bool("simulate", false, "Simulate a tape library ")
	s3enabled := flag.Bool("s3", false, "Write S3 as the storage backend")
	verify := flag.Bool("verify", false, "Verify that hardware matches config file")
	version := flag.Bool("version", false, "Find and copy version files")
	database := flag.Bool("database", false, "Create the database")
	read := flag.Bool("read", false, "Read the tapes")
	clean := flag.Bool("clean", false, "Clean the log")
	region := flag.String("region", DEFAULT_REGION, "AWS region to write s3 objects")
	configFile := flag.String("config", DEFAULT_CONFIG_FILE, "JSON file that defines tape drive mapping")
	flag.Parse()

	// read the config file
	configData, err := ioutil.ReadFile(*configFile)
	if err != nil {
		logEvent("Unable to read configuration file: ", *configFile)
		log.Fatal(err)
	}
	// unmarshal the config file
	var config Config
	err = json.Unmarshal(configData, &config)
	if err != nil {
		logEvent("Unable to json unmarshal the json config file: ", *configFile)
		log.Fatal(err)
	}

	// run a verification of the config file 
	if *verify {
		library := NewRealTapeLibrary(config.LibraryDevice, config.TapeDriveDevices)
		fmt.Println("\n\nLibrary: ",config.LibraryDevice)
		tapeDrives, tapeCartridges := library.Audit()
		fmt.Println("\n\tCartridge\tSlot")
		for _,tc := range tapeCartridges {
			fmt.Printf("\t%.18s%d", tc.Name(),tc.GetSlot())
		}
		// check that tape drives the 
		fmt.Println("\n\tDevice\tSerial\t\tCart")
		drivePathFailure := false
		for _,td := range tapeDrives {
			// if does not exist on data path then exit
			sn, exists := td.SerialNumber()
			if !exists {
				drivePathFailure = true
				logEvent("Device Path did not see drive",td.Device())
				fmt.Println("\t",td.Device(),"\tDevice Not Seen")
				continue
			}
			cart,_ := td.GetCart()
			if cart == "" {
				cart = "Empty"
			}
			fmt.Println("\t",td.Device(),"\t",sn,"\t",cart)
		}
		if drivePathFailure {
			logEvent("Verification of config file: ", *configFile, " failed")
			log.Fatal("Verification of config file: ", *configFile, " failed")
		}
	}

	// create or append the log file
	var file *os.File
	if *all || *clean {
		file, err = os.Create("ltfs-vof.log")
		os.Remove(DEFAULT_DB)
	} else {
		file, err = os.OpenFile("ltfs-vof.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	}
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	log.SetOutput(file)

	// if all set everything as enabled
	if *all {
		*version = true
		*database = true
		*read = true
	}

	// log arguments
	logEvent("****RUN PARMS **** ")
	logEvent("\n\tSIMULATE: ", *simulate, "\n\tVERSION: ", *version, "\n\tDATABASE: ", *database, "\n\tREAD: ", *read, "\n\tS3: ", *s3enabled)

	// select the library type used
	var library TapeLibrary
	if *simulate {
		library = NewTapeLibrarySimulator()
	} else {
		library = NewRealTapeLibrary(config.LibraryDevice, config.TapeDriveDevices)
	}
	dbManager := NewDBManager(DEFAULT_DB, DEFAULT_BLOCK_CACHE, *region, *clean, *s3enabled)
	db := NewDatabase(DEFAULT_VERSION_CACHE, dbManager, library)
	// if version is enabled create the database manager and get the version files
	if *version {
		logEvent("*****COPYING VERSION FILES******")
		db.GetVersionFiles()
		logEvent("****VERSION FILES COPIED******")
	}
	if *database {
		logEvent("******BUILDING DATABASE*******")
		db.CreateDatabase()
		logEvent("******ENDING BUILDING DATABASE*******")
	}

	// restore all the content if specified
	if *read {
		logEvent("******READING BLOCK FILES*******")
		db.RestoreAll()
		logEvent("******READ ALL BLOCK FILES*******")
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

// log events with a time stamp and function name as a prefix
func logEvent(args ...any) {

	// get function name
	pc, _, _, _ := runtime.Caller(1)
	fname := runtime.FuncForPC(pc).Name()

	// get current time
	currentTime := time.Now().Format("Mon Jan _2 15:04:05")

	// print line
	log.Println(currentTime, fname, ": ", args)
}
