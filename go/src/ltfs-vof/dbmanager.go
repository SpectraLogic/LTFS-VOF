package main

import (
	"database/sql"
	"encoding/json"
	"github.com/oklog/ulid/v2"
	. "ltfs-vof/utils"
	_ "modernc.org/sqlite"
	"os"
	"sort"
	"strings"
)

type DBManager struct {
	db           *sql.DB
	cacheDir     string
	region       string
	s3Enabled    bool
	lockResource *Resource
	lockValue    int
	logger       *Logger
}

func NewDBManager(dbName, cacheDir, region string, clean, s3Enabled bool, logger *Logger) *DBManager {
	var manager DBManager
	manager.region = region
	manager.s3Enabled = s3Enabled
	manager.lockResource = NewResource(1)
	manager.cacheDir = cacheDir
	manager.logger = logger

	var err error
	manager.db, err = sql.Open("sqlite", dbName)
	if manager.db == nil {
		logger.Fatal("Could not open db", err)
	}

	// remove and setup the db
	if clean {
		os.Remove(dbName)
		file, err := os.Create(dbName)
		if err != nil {
			logger.Fatal(err)
		}
		file.Close()
		// create blocks table
		_, err = manager.db.Exec(`CREATE TABLE blocks (blockid TEXT NOT NULL PRIMARY KEY, state INT default 0,blockinfo BLOB)`)
		if err != nil {
			logger.Fatal("Could not create block table", err)
		}
		// create versions table
		_, err = manager.db.Exec(`CREATE TABLE versions (versionid TEXT NOT NULL PRIMARY KEY, bucketkey string KEY, inrecord BOOL KEY, completed BOOL DEFAULT false, deletemarker BOOL default false, ispacklist BOOL default false, blocklist BLOB )`)
		if err != nil {
			logger.Fatal("Could not create version table", err)
		}
		// create the packs table
		_, err = manager.db.Exec(`CREATE TABLE packs (packid TEXT NOT NULL PRIMARY KEY,tapeid TEXT KEY, blocklist BLOB)`)
		if err != nil {
			logger.Fatal("Could not create pack table", err)
		}
		// remove everything from the cache directory
		if !s3Enabled {
			os.RemoveAll(manager.cacheDir)
			os.Mkdir(cacheDir, 0777)
		}
	}

	return &manager
}
func (dbm *DBManager) lock() {
	dbm.lockValue = dbm.lockResource.Reserve()
}
func (dbm *DBManager) unlock() {
	dbm.lockResource.Release(dbm.lockValue)
}

// add a version to the version table
func (dbm *DBManager) AddVersion(mr *MetaReference) {
	dbm.lock()
	bucketObject := mr.GetBucketObject()
	// if delete marker then just add it to version table
	if mr.GetIsDeleteMarker() {
		dbm.insertVersionTable(bucketObject, mr.GetVersion(), false, true, false, nil)
		dbm.unlock()
		return
	}
	// if version is deleted then remove it from the block table and verison table
	if mr.GetIsDeleted() {
		dbm.DeleteVersion(mr.GetVersion())
		dbm.unlock()
		return
	}
	// three cases, data is in version, version points to blocks, or version points to pack list
	var blockIDs []string
	// see if data is part of record or the packs are defined
	data := mr.GetDataInRecord()
	packs := mr.GetPacks()
	// if data is in the record then add it to the block table and the version table
	if data != nil {
		// create a block entry for the data and write it to the cache
		pentry := NewPackEntry("", 0, 0)
		blockid := dbm.insertBlocksTable(pentry)
		// write block to cache
		block := NewBlock(blockid, mr.GetBucket(), mr.GetObject(), mr.GetVersion(), data, 0, 0)
		dbm.writeBlockToCache(blockid, block)
		dbm.updateBlockRecordState(blockid, STATE_CACHED)

		// create the version record and add the block to its block list
		dbm.insertVersionTable(bucketObject, mr.GetVersion(), true, false, false, blockIDs)
		dbm.addVersionBlockID(mr.GetVersion(), blockid)
	} else if packs != nil {
		// if the packs are to be in the version record then add them to the pack table
		for i, pEntry := range packs {
			// if there are packs then add the pack list to the version table
			blockIDs = append(blockIDs, dbm.insertBlocksTable(pEntry))
			dbm.insertPackTable(pEntry.GetPackName(), pEntry.GetPhysicalStart(), mr.GetVersion(), blockIDs[i])
		}
		dbm.insertVersionTable(bucketObject, mr.GetVersion(), false, false, false, blockIDs)
	} else if mr.GetIsPackList() {
		// put the pack list entry into the version and pack table
		packList := mr.GetPackList()
		dbm.insertPackTable(packList.GetPackName(), packList.GetPhysicalStart(), mr.GetVersion(), "")
		dbm.insertVersionTable(bucketObject, mr.GetVersion(), false, false, true, nil)
	} else {
		dbm.logger.Fatal("Version added that doesn't have data in the version, packs or a packlist")
	}
	dbm.unlock()
}

func (dbm *DBManager) DeleteVersion(version string) {

	dbm.lock()

	// get the blocklist from the version table
	_, _, _, _, blockids := dbm.getVersionInfo(version)

	// set each block to deleted
	for _, blockid := range blockids {
		// remove the record from the table
		dbm.deleteBlockRecord(blockid)
	}

	// delete the version from the version table
	dbm.deleteVersionsTable(version)
	dbm.unlock()
}

// encountered a data block
func (dbm *DBManager) WriteBlock(pack string, blockStartLocation, blockEndLocation int64, block *Block) {

	dbm.lock()
	packMap := dbm.getPackMap(pack)
	packMapEntry, ok := packMap[blockStartLocation]
	if !ok {
		// get the pack map entry if it doesn't exist then this is an orphaned block
		// that might be defined in a pack list to show up later, so write the block
		// to the cache and create a block record and return

		// create a pack list entry that only knows the pack and the start location
		entry := NewPackEntry(pack, blockStartLocation, blockEndLocation)

		// insert the pack list entry into the block table, and change its state to cached
		blockID := dbm.insertBlocksTable(entry)
		dbm.updateBlockRecordState(blockID, STATE_CACHED)

		// write the data to cache
		dbm.writeBlockToCache(blockID, block)

		// insert the entry into the pack table, no version associated with it yet
		dbm.insertPackTable(pack, blockStartLocation, "", blockID)

		// done so return
		dbm.unlock()
		return
	}

	// check state of block record, if not ready then return
	// it could be deleted because the version associated with it was deleted
	state, entry := dbm.getBlockRecord(packMapEntry.BlockID)
	if state != STATE_READY {
		dbm.logger.Event("No BLock Record for : ", packMapEntry.BlockID)
		dbm.unlock()
		return
	}

	// write the block to the cache
	dbm.unlock()
	dbm.logger.Event("Write the block to cache: ", packMapEntry.BlockID)
	dbm.writeBlockToCache(packMapEntry.BlockID, block)
	dbm.lock()

	// update the block to written state
	dbm.updateBlockRecordState(packMapEntry.BlockID, STATE_CACHED)

	// check to see if this if this packlist entry extends across multiple blocks
	// if so create a new block entry, update the pack list to point to it and
	// include in the version record
	if entry.GetPhysicalEnd() > blockEndLocation && entry.GetPackName() == pack {
		// Physical end and logical end stays the same but physical start is the end of the last block

		// reuse the entry but change the physical start to the end of the last block
		// and reduce the physical length to the end of the last block
		entry.SetPhysicalLength(entry.GetPhysicalLength() - (blockEndLocation - blockStartLocation))
		entry.SetPhysicalStart(blockEndLocation)
		// little tricky, don't know logical ends so just make it one more than the start
		// so they will be in order
		entry.SetLogicalStart(entry.GetLogicalStart() + 1)
		blockid := dbm.insertBlocksTable(entry)

		// need to add the entry to the pack table
		dbm.insertPackTable(entry.GetPackName(), blockEndLocation, packMapEntry.VersionID, blockid)

		// need to update the version record to include new block
		dbm.addVersionBlockID(packMapEntry.VersionID, blockid)
	}

	// process the version in case all blocks are cached
	dbm.logger.Event("Process Version: ", packMapEntry.VersionID)
	dbm.processVersion(packMapEntry.VersionID)
	dbm.unlock()
}

// Encountered a pack list need, to create or update the blocks associated with the list,
// upate the pack map entries and update the versio to point to all blocks in pack map
func (dbm *DBManager) ProcessPackList(packName string, offset int64, packlist []*PackEntry) {

	// lock the database
	dbm.lock()
	// step 1: find the version associated with this pack list
	packMap := dbm.getPackMap(packName)
	packEntry, ok := packMap[offset]
	if !ok {
		dbm.logger.Fatal("Could not find pack entry for pack list", packName, " offset ", offset)
	}
	versionID := packEntry.VersionID
	// step 2: for block entries that don't exist create them, if they have already
	// been seen then there will a map to them and they need to be updated with
	// logical locations specified in pack map
	var blockIDs []string
	var blockID string
	for _, listentry := range packlist {
		// get the pack map entry
		packMapEntry := dbm.getPackMap(listentry.GetPackName())
		// if no pack map for this packname or no entry for this block create a block
		if packMapEntry == nil {
			blockID = dbm.insertBlocksTable(listentry)
		} else {
			entry, ok := packMapEntry[listentry.GetPhysicalStart()]
			// if no entry then create a block
			if !ok {
				blockID = dbm.insertBlocksTable(listentry)
			} else {
				// entry exists update the entry
				blockID = entry.BlockID
				dbm.updateBlocksTable(blockID, listentry)
			}
		}

		// Step 3: update the pack table with the location of the blocks in the packlist
		dbm.insertPackTable(listentry.GetPackName(), listentry.GetPhysicalStart(), versionID, blockID)
		blockIDs = append(blockIDs, blockID)
	}
	// step 4: update the version table with the location of the blocks
	dbm.updateVersionBlockIDs(versionID, blockIDs)

	// step 5: process the version in case all blocks are cahced
	dbm.processVersion(versionID)
	dbm.unlock()
}

func (dbm *DBManager) processVersion(versionID string) {
	dbm.logger.Event("versionID: ", versionID)
	// db should be locked by calling process
	// loop processing versions of this bucket and key starting with the oldest
	// two conditions are required
	// 1) all blocks in the version have been written
	// 2) This is the oldest version of the key that hasn't been written
	for {
		// get the info for this version
		bucketkey, _, deleteMarker, _, blockids := dbm.getVersionInfo(versionID)

		// if this is a not delete marker then see if all blocks are written
		if !deleteMarker {
			// check to see if all blocks associated with this version have been cached
			// if not return and wait for the next block to be written
			if blockids == nil {
				dbm.logger.Event("alls the bucketkey: ", bucketkey)
				return
			}
			for _, blockid := range blockids {
				state, _ := dbm.getBlockRecord(blockid)
				if state != STATE_CACHED {
					dbm.logger.Event("Not all blocks assosciated with verison are in cache bucketkey: ", bucketkey)
					return
				}
			}
		}
		// all blocks have been written or this is delete marker
		dbm.logger.Event("All blocks have been written")
		versions := dbm.getVersionsNotCompleted(bucketkey)
		if versions == nil {
			dbm.logger.Event("Verson completed")
			return
		}

		// if this isn't the oldest version id return and wait for the oldest version to be written
		if versions[0] != versionID {
			dbm.logger.Event("Version not oldest")
			return
		}
		bucket, key := dbm.getBucketKey(bucketkey)
		// if this is a not a delete marker send it to s3 if enabled
		if !deleteMarker {
			// sort the blocks in starting logical order
			blockids = dbm.sortBlockOrder(bucket, blockids)

			// if s3 enabled write version to S3
			if dbm.s3Enabled {
				// if single block just execute a single PUT
				if len(blockids) == 1 {
					Put(bucket, key, dbm.region, dbm.cacheDir, blockids[0])

				} else {
					// if multiple blocks do a multiple part upload where each block is a part
					PutMultipart(bucket, key, dbm.region, dbm.cacheDir, blockids)
				}
			}

			// remove the block data from the cache and delete the blockid records
			for _, blockid := range blockids {
				dbm.removeBlockFromCache(blockid, bucket)
				dbm.deleteBlockRecord(blockid)
			}
		} else {
			// delete marker
			if dbm.s3Enabled {
				dbm.logger.Event("S3, Delete Marker, bucket: ", bucket, "  key: ", key, "  Region: ", dbm.region)
				DeleteMarker(bucket, key, dbm.region)
			}
		}

		// Delete the version from the version table
		dbm.deleteVersionsTable(versionID)

		// if there is not another version then break, otherwise loop and process
		// the next version
		if len(versions) > 1 {
			versionID = versions[1]
		} else {
			break
		}
	}
}

// returns ordered list of tapes from oldest to newest and map of
// tapes to the packs on the tapes also ordered from oldest to newest
func (dbm *DBManager) GetTapePackOrder() ([]string, map[string][]string) {

	// create a map of tape id to packs
	dbm.lock()
	tapeids, packids := dbm.getTapesPacksTable()
	dbm.unlock()
	tapepacks := make(map[string][]string)
	for i, tapeid := range tapeids {
		_, ok := tapepacks[tapeid]
		if !ok {
			tapepacks[tapeid] = make([]string, 0)
		}
		// add this pack to the
		tapepacks[tapeid] = append(tapepacks[tapeid], packids[i])
	}
	var orderedList []string

	// now sort the from oldest to newest for each tape
	for tape, packids := range tapepacks {
		// convert the slice to a time based slide
		packtimes := make([]uint64, 0)
		for _, packid := range packids {
			// remove the suffix from the packid
			dbm.logger.Event("Get time from ID: ", packid)
			_, packtime := GetTimeFromID(packid, dbm.logger)
			packtimes = append(packtimes, packtime)
		}
		// sort the packids based on the time
		sort.Slice(packtimes, func(i, j int) bool {
			if packtimes[i] < packtimes[j] {
				packids[i], packids[j] = packids[j], packids[i]
				return true
			}
			return false
		})
		// create a currently unordered list of tapes
		orderedList = append(orderedList, tape)
	}
	// create the ordered list of tapes based on oldest pack time of
	// first element
	sort.Slice(orderedList, func(i, j int) bool {
		_, oldi := GetTimeFromID(tapepacks[orderedList[i]][0], dbm.logger)
		_, oldj := GetTimeFromID(tapepacks[orderedList[j]][0], dbm.logger)
		if oldi < oldj {
			return true
		}
		return false
	})
	return orderedList, tapepacks
}

// add a tape to a pack
func (dbm *DBManager) AddTapeToPack(packID string, tapeID string) {
	dbm.lock()
	dbm.insertTapePacksTable(packID, tapeID)
	dbm.unlock()
}

type blockState int

const (
	STATE_READY     blockState = 0
	STATE_WRITTEN              = 1
	STATE_CACHED               = 2
	STATE_DELETED              = 3
	STATE_COMPLETED            = 4
	STATE_ORPHANED             = 5
)

type PackMapType map[int64]PackMapEntry

type PackMapEntry struct {
	BlockID   string `json:"bid"`
	VersionID string `json:"vid"`
}

// VERSION TABLE FUNCTIONS
func (dbm *DBManager) insertVersionTable(bucketkey, versionid string, inRecord, deleteMarker, ispacklist bool, blockids []string) {

	blocklistjson, err := json.Marshal(blockids)
	if err != nil {
		dbm.logger.Fatal("Could not marshal blocklist", err)
	}
	sql := "INSERT or REPLACE INTO versions (versionid, bucketkey, inrecord, deleteMarker, ispacklist, blocklist) VALUES (?,?,?,?,?,?)"
	_, err = dbm.db.Exec(sql, versionid, bucketkey, inRecord, deleteMarker, ispacklist, blocklistjson)
	if err != nil {
		dbm.logger.Fatal("Could not insert or replace version id: ", versionid, " bucketkey: ", bucketkey, "bucketkey", " error: ", err)
	}
}
func (dbm *DBManager) deleteVersionsTable(versionid string) {
	sql := "DELETE FROM versions WHERE versionid = ?"
	_, err := dbm.db.Exec(sql, versionid)
	if err != nil {
		dbm.logger.Fatal("Could not delete version", err)
	}
}

// returns bucketkey, deleteMarker, ispacklist, blocklist
func (dbm *DBManager) getVersionInfo(versionid string) (string, bool, bool, bool, []string) {
	buckkey, inRecord, deleteMarker, ispacklist, blocklist, exist := dbm.getVersionRecord(versionid)
	if !exist {
		dbm.logger.Fatal("Version record does not exist")
	}
	return buckkey, inRecord, deleteMarker, ispacklist, blocklist
}

func (dbm *DBManager) doesVersionRecordExist(versionid string) bool {
	_, _, _, _, _, exist := dbm.getVersionRecord(versionid)
	return exist
}

func (dbm *DBManager) getVersionRecord(versionid string) (string, bool, bool, bool, []string, bool) {
	var bucketkey string
	var inRecord int
	var deleteMarker int
	var ispacklist int
	var blockinfo []byte
	var err error

	sql := "SELECT bucketkey, inrecord, deletemarker, ispacklist, blocklist FROM versions WHERE versionid = ?"
	err = dbm.db.QueryRow(sql, versionid).Scan(&bucketkey, &inRecord, &deleteMarker, &ispacklist, &blockinfo)
	if err != nil {
		return "", false, false, false, nil, false
	}
	var blocklist []string
	err = json.Unmarshal(blockinfo, &blocklist)
	if err != nil {
		dbm.logger.Fatal("Could not unmarshal blocklist for read", err)
	}
	return bucketkey, inRecord == 1, deleteMarker == 1, ispacklist == 1, blocklist, true
}

// get the versions whose data was part of version record
func (dbm *DBManager) getVersionsInRecord() []string {
	var versions []string
	var err error

	v, err := dbm.db.Query("SELECT versionid FROM versions WHERE inrecord = 1")
	if err != nil {
		dbm.logger.Fatal("Could not read versions associated with in record", err)
	}
	defer v.Close()
	for v.Next() {
		var versionid string
		err = v.Scan(&versionid)
		if err != nil {
			dbm.logger.Fatal("Could not read versionid", err)
		}
		versions = append(versions, versionid)
	}
	return versions
}

// get the versions associated with a bucket and key, that are not comlpleted
func (dbm *DBManager) getVersionsNotCompleted(bucketkey string) []string {
	var versions []string
	var err error

	sql := "SELECT versionid,completed FROM versions WHERE bucketkey = ?"
	v, err := dbm.db.Query(sql, bucketkey)
	if err != nil {
		dbm.logger.Fatal("Could not read versions associated with bucket and key", err)
	}
	defer v.Close()
	for v.Next() {
		var versionid string
		var completed int
		err = v.Scan(&versionid, &completed)
		if err != nil {
			dbm.logger.Fatal("Could not read versionid", err)
		}
		// add to the array if not completed
		if completed == 0 {
			versions = append(versions, versionid)
		}
	}

	// sort the versions by oldest to newest
	sort.Slice(versions, func(i, j int) bool {
		_, timei := GetTimeFromID(versions[i], dbm.logger)
		_, timej := GetTimeFromID(versions[j], dbm.logger)
		if timei < timej {
			return true
		}
		return false
	})
	return versions
}

// update the state of a block record in the block table
func (dbm *DBManager) updateVersionCompletedState(versionid string) {
	sql := "UPDATE versions SET completed = 1 WHERE versionid = ?"
	_, err := dbm.db.Exec(sql, versionid)
	if err != nil {
		dbm.logger.Fatal("Could not update version", err)
	}
}

// updates a versions block list with list passed
func (dbm *DBManager) updateVersionBlockIDs(versionid string, blockids []string) {
	blocklistjson, err := json.Marshal(blockids)
	if err != nil {
		dbm.logger.Fatal("Could not marshal blocklist", err)
	}
	sql := "UPDATE versions SET blocklist = ? WHERE versionid = ?"
	_, err = dbm.db.Exec(sql, blocklistjson, versionid)
	if err != nil {
		dbm.logger.Fatal("Could not update version", err)
	}
}

// adds a block to a versions block list
func (dbm *DBManager) addVersionBlockID(versionid string, blockid string) {
	// function does not need to be locked as it calls locked functions
	// get the current block list
	_, _, _, _, blocklist := dbm.getVersionInfo(versionid)
	// add the new blockid
	blocklist = append(blocklist, blockid)
	// update the version with the new block list
	dbm.updateVersionBlockIDs(versionid, blocklist)
}

// BLOCKS TABLE FUNCTIONS
func (dbm *DBManager) insertBlocksTable(entry *PackEntry) string {
	// create a unique blockid
	blockid := ulid.Make().String()
	blockinfo, err := json.Marshal(entry)
	if err != nil {
		dbm.logger.Fatal("Could not marshal block", err)
	}
	sql := "INSERT INTO blocks (blockid, state, blockinfo) VALUES (?,?,?)"
	_, err = dbm.db.Exec(sql, blockid, STATE_READY, blockinfo)
	if err != nil {
		dbm.logger.Fatal("Could not insert into blocks table: ", err)
	}
	return blockid
}

// update the state of a block record in the block table
func (dbm *DBManager) updateBlockRecordState(blockid string, state blockState) {
	sql := "UPDATE blocks SET state = ? WHERE blockid = ?"
	_, err := dbm.db.Exec(sql, state, blockid)
	if err != nil {
		dbm.logger.Fatal("Could not update block", err)
	}
}

// update the locations of a block record in the block table
// this is done when a block list is found
func (dbm *DBManager) updateBlocksTable(blockid string, entry *PackEntry) {

	blockinfo, err := json.Marshal(entry)
	if err != nil {
		dbm.logger.Fatal("Could not marshal block", err)
	}

	// update the record
	sql := "UPDATE blocks SET blockinfo = ? WHERE blockid = ?"
	_, err = dbm.db.Exec(sql, blockinfo, blockid)
	if err != nil {
		dbm.logger.Fatal("Could not update block", err)
	}
}

// read the state of a block record in the block table
func (dbm *DBManager) getBlockRecord(blockid string) (blockState, *PackEntry) {

	var state int64
	var blockinfo []byte
	var err error

	sql := "SELECT state,blockinfo FROM blocks WHERE blockid = ?"
	err = dbm.db.QueryRow(sql, blockid).Scan(&state, &blockinfo)
	if err != nil {
		dbm.logger.Fatal("Could not read block", err)
	}
	// decode json
	var entry PackEntry
	err = json.Unmarshal(blockinfo, &entry)
	return blockState(state), &entry
}

// delete a blcok record
func (dbm *DBManager) deleteBlockRecord(blockid string) {

	sql := "DELETE FROM blocks WHERE blockid = ?"
	_, err := dbm.db.Exec(sql, blockid)
	if err != nil {
		dbm.logger.Fatal("Could not read block", err)
	}
}

// PACKS TABLE FUNCTIONS
func (dbm *DBManager) insertPackTable(packid string, start int64, versionid, blockid string) {

	// get the pack
	var blocklist PackMapType

	var blockinfo []byte
	var cartid string
	var err error

	sql := "SELECT tapeid, blocklist FROM packs WHERE packid = ?"

	err = dbm.db.QueryRow(sql, packid).Scan(&cartid, &blockinfo)
	if err != nil || blockinfo == nil {
		blocklist = make(PackMapType)
	} else {
		err = json.Unmarshal(blockinfo, &blocklist)
		if err != nil {
			dbm.logger.Fatal("Could not unmarshal blocklist", err)
		}
	}
	// add the block and version id to the pack
	blocklist[start] = PackMapEntry{VersionID: versionid, BlockID: blockid}
	blocklistjson, err := json.Marshal(blocklist)
	if err != nil {
		dbm.logger.Fatal("Could not marshal blocklist", err)
	}

	sql = "INSERT OR REPLACE INTO packs (packid, tapeid, blocklist) VALUES (?,?,?)"
	_, err = dbm.db.Exec(sql, packid, cartid, blocklistjson)
	if err != nil {
		dbm.logger.Fatal("Could not insert packs", err)
	}
}

// read the block map specified
func (dbm *DBManager) getPackMap(packID string) (packMap PackMapType) {
	var packinfo []byte
	var err error

	sql := "SELECT blocklist FROM packs WHERE packid = ?"
	err = dbm.db.QueryRow(sql, packID).Scan(&packinfo)
	if err != nil {
		return nil
	}
	// the packmap might be empty because the pack is made up of a block list
	if len(packinfo) == 0 {
		return nil
	}
	err = json.Unmarshal(packinfo, &packMap)
	if err != nil {
		dbm.logger.Fatal("Could not unmarshal pack mapd", err)
	}
	return packMap
}

func (dbm *DBManager) insertTapePacksTable(packid, tapeid string) {
	// getht the blocklist, ignore errors because it may not exist
	var blockinfo []byte
	var err error

	dbm.logger.Event("pack: ", packid, "tape: ", tapeid)

	sql := "SELECT blocklist FROM packs WHERE packid = ?"
	dbm.db.QueryRow(sql, packid).Scan(&blockinfo)

	// insert or replace the tapeid into the packs table
	sql = "INSERT OR REPLACE INTO packs (packid, tapeid, blocklist) VALUES (?,?,?)"

	_, err = dbm.db.Exec(sql, packid, tapeid, blockinfo)
	if err != nil {
		dbm.logger.Fatal("Could not insert tape into packs table", err)
	}
}

// returns a slice of all entries (tapeid, packid)
func (dbm *DBManager) getTapesPacksTable() ([]string, []string) {
	tapeids := []string{}
	packids := []string{}

	sql := "SELECT packid, tapeid FROM packs"
	p, err := dbm.db.Query(sql)
	if err != nil {
		dbm.logger.Fatal("Could not read pack file ", err)
	}
	defer p.Close()
	for p.Next() {
		var tapeid string
		var packid string
		err = p.Scan(&packid, &tapeid)
		if err != nil {
			dbm.logger.Fatal("Could not read pack table", err)
		}
		tapeids = append(tapeids, tapeid)
		packids = append(packids, packid)
	}
	return tapeids, packids
}

// CACHE/S3 FUNCTIONS
func (dbm *DBManager) writeBlockToCache(blockid string, block *Block) {
	// if s3 is enabled then write the block to the S3 repository lost+found
	// if s3 is not enabled then write the block to the cache
	directory := dbm.cacheDir + "/" + block.GetBucket()
	fileName := directory + "/" + blockid
	os.Mkdir(directory, 0777)
	os.Create(fileName)
	file, err := os.OpenFile(fileName, os.O_RDWR, 0644)
	if err != nil {
		dbm.logger.Fatal("Could not open file", err)
	}
	defer file.Close()
	// write the data to the file
	_, err = file.Write(block.GetData())
	if err != nil {
		dbm.logger.Fatal("Could not write block to cache", err)
	}
}
func (dbm *DBManager) removeBlockFromCache(blockid, bucket string) {
	fileName := dbm.cacheDir + "/" + bucket + "/" + blockid
	err := os.Remove(fileName)
	if err != nil {
		dbm.logger.Fatal("Could not remove block from cache", err)
	}
}

// sort a list of blocks associated with a version based on logical address
func (dbm *DBManager) sortBlockOrder(bucket string, blockids []string) []string {

	// get all the block records associated with the version
	var listEntries []*PackEntry
	for _, blockid := range blockids {
		_, entry := dbm.getBlockRecord(blockid)
		listEntries = append(listEntries, entry)
	}
	// sort the blocks by logical starting address
	sort.Slice(listEntries, func(i, j int) bool {
		if listEntries[i].GetLogicalStart() < listEntries[j].GetLogicalStart() {
			blockids[i], blockids[j] = blockids[j], blockids[i]
			return true
		}
		return false
	})
	return blockids
}
func (dbm *DBManager) createBucketKey(bucket, Key string) string {
	return bucket + "/" + Key
}

func (dbm *DBManager) getBucketKey(bucketKey string) (string, string) {
	segments := strings.SplitN(bucketKey, "/", 2)
	if len(segments) != 2 {
		dbm.logger.Fatal("Could not split bucket key", bucketKey)
	}
	// for some reason it is putting an extra space in argument[0]
	return segments[0], segments[1]
}
