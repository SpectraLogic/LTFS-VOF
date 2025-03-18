// program to read tapes and write to S3
package main

import (
	"flag"
	. "ltfs-vof/tapehardware"
	"os"
	"log"
	"runtime"
	"time"
)

const DEFAULT_DB string = "./db"
const DEFAULT_CACHE string = "lost+found"
const DEFAULT_VERSION_CACHE string = "./version"
const DEFAULT_REGION string = "us-west-1"

func main() {
	// get the command line arguments
	simulate := flag.Bool("simulate", false, "Simulate a tape library ")
	s3enabled := flag.Bool("s3", false, "Write S3 as the storage backend")
	version := flag.Bool("version", false, "Find and copy version files")
	database := flag.Bool("database", false, "Create the database")
	read := flag.Bool("read", false, "Read the tapes")
	clean := flag.Bool("clean", false, "Clean the log")
	//compare := flag.Bool("compare", false, "Compare a source and result S3 bucket")
	all := flag.Bool("all", false, "Perform all operaions sequentially")

	flag.Parse()

	// create or append the log file
	var file *os.File
	var err error
	if *all || *clean {
		file, err = os.Create("ltfs-vof.log")
		os.Remove(DEFAULT_DB)
		if *s3enabled {
			// list buckets and delete them all
		}
	} else {
		file, err = os.OpenFile("ltfs-vof.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	}
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	log.SetOutput(file)

	// if all set everything else
	if *all {
		*version = true
		*database = true
		*read = true
	}

	// log arguments
	logEvent("****RUN PARMS **** ")
	logEvent("\n\tSIMULATE: ", *simulate, "\n\tVERSION: ", *version, "\n\tDATABASE: ", *database, "\n\tREAD: ", *read, "\n\tS3: ", *s3enabled)

	// select the librar type used
	var library TapeLibrary
	if *simulate {
	//	library = NewTapeLibrarySimulator()
		library = NewSingleCartTapeLibrary()
	} else {
		library = NewRealTapeLibrary()
	}
	
	var dbmanager *DBManager
	dbmanager = NewDBManager(DEFAULT_DB, DEFAULT_CACHE, DEFAULT_REGION, *clean, *s3enabled)
	// if version is enabled create the database manager and get the version files
	if *version {
		logEvent("*****COPYING VERSION FILES******")
		getVersionFiles(library, dbmanager)
		logEvent("****VERSION FILES COPIED******")
	}
	if *database {
		logEvent("******BUILDING DATABASE*******")
		createDatabase(dbmanager)
		logEvent("******ENDING BUILDING DATABASE*******")
	}

	// restore all the content if specified
	if *read {
		logEvent("******READING BLOCK FILES*******")
		RestoreAll(library, dbmanager)
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
