package main

import (
	"fmt"
	"github.com/spectralogic/go-core/codec/value"
	tlvcore "github.com/spectralogic/go-core/tlv"
	. "ltfs-vof/logger"
	"os"
	"strings"
)

type TagType int

const (
	BLOCK         TagType = iota
	PACKLIST              = iota
	VERSION               = iota
	DELETEVERSION         = iota
	METAFILE              = iota
)

var Tags map[TagType][]byte = map[TagType][]byte{
	BLOCK:         {'b', 'k'},
	PACKLIST:      {'o', 'l'},
	VERSION:       {'v', 'm'},
	DELETEVERSION: {'v', 'd'},
	METAFILE:      {'m', 'f'},
}

type TLV struct {
	dataLength uint64
	tag        TagType
}

func ReadTLV(file *os.File,logger *Logger) *TLV {

	var tlv TLV
	header := make([]byte, 32)
	_, err := file.Read(header)
	if err != nil {
		return nil
	}
	tagValue, size, _, err := tlvcore.DecodeHeader(header)
	if err != nil {
		return nil
	}
	tlv.dataLength = size
	// convert tag to map
	tlv.tag = TagType(-1)
	for i, tag := range Tags {
		tagLowerByte := byte(tagValue & 0xff)
		tagUpperByte := byte(tagValue >> 8 & 0xff)
		if tag[0] == tagUpperByte && tag[1] == tagLowerByte {
			tlv.tag = TagType(i)
			break
		}
	}
	if tlv.tag == -1 {
		logger.Fatal("Tag not found")
	}
	return &tlv
}

func (t *TLV) Tag() TagType {
	return t.tag
}
func (t *TLV) DataLength() uint64 {
	return t.dataLength
}

// PackEntry contains: the range of bytes of a source (version or
// multipart part), the PackID containing that data, and the matching
// byte range within the pack.
//
// When a Store saves data in native form (without packing), there
// will be only one PackEntry for the object and the SourceRange will
// be nil (since the source range matches the pack range).
//
// BlockLens and SourceLens are used to directly access a block within a pack. The information is stored for all blocks
// in the PackEntry except the last one (the last one isn't needed since we'll never skip past it). To reduce
// storage, the SourceLens only tracks packs that have a different source data length than the BlockLength. So
// normally SourceLens isn't present (since only the last block will have a size different than BlockLength). SourceLens
// is needed if any of the blocks were copied directly from a packlist with a non-default block size. In that case there
// may be a number of 0 value entries in addition to a positive or negative adjustment.
type Packs []*PackEntry
type PackEntry struct {
	Pack        string  `codec:"p" json:"pack,omitempty"`          // ID of pack containing this offset to next offset
	SourceRange *Range  `codec:"o,omitempty" json:"src,omitempty"` // Offset/len within original data (if data is packed)
	PackRange   *Range  `codec:"t,omitempty" json:"pos,omitempty"` // Offset/len within target pack.
	BlockLens   []int32 `codec:"E,omitempty" json:"bln,omitempty"` // Lengths of blocks (except the last one)
	SourceLens  []int32 `codec:"N,omitempty" json:"sln,omitempty"` // Amount source data varies from BlockLength (except the last one)
}

// StoredPack holds pack list information stored in a pack.
type StoredPack struct {
	VersionID string `codec:"I,omitempty"` // Version this pack list references
	Upload    string `codec:"U,omitempty"` // Upload used to load the data (empty if a single PUT was used)
	Packs     Packs  `codec:"P,omitempty"` // List of pack entries where data is stored
	encoder   *value.Encoder
}

type PermissionFlags int

type IDType int

type ACL struct {
	IDType      IDType          `codec:"t" json:"type"`
	ID          string          `codec:"i" json:"id"`
	Permissions PermissionFlags `codec:"p,omitempty" json:"permissions,omitempty"`
}

type ACLs []*ACL

type Timestamp int64

type VersionID struct {
	Bucket     string    `codec:"b" json:"bucket"`
	Object     string    `codec:"o" json:"object" table:"4,30,Object"`
	Version    string    `codec:"v" json:"version" table:"3,27,Version"`
	NextAction Timestamp `codec:"a,omitempty" json:"action,omitempty"`
}
type Range struct {
	Start int64 `codec:"s,omitempty" json:"start,omitempty"`
	Len   int64 `codec:"l,omitempty" json:"len,omitempty"`
}

type PackReference struct {
	Pack      string   `codec:"k" json:"pack"`                           // pack that contains the pack list
	PackRange *Range   `codec:"r,omitempty" json:"rng,omitempty"`        // range of pack that has encoded pack list
	PackIDs   []string `codec:"a,omitempty" json:"additional,omitempty"` // IDs of additional packs containing object data
	Truncated bool     `codec:"m,omitempty" json:"more,omitempty"`       // true if not all data packs are listed in PackIDs
}

type MetaReference struct {
	*VersionID `codec:"i,omitempty"`
	// Owner ID and ACLs are not supported
	OwnerID string `codec:"o,omitempty" json:"owner,omitempty"` // Canonical ID of owner
	ACLs    ACLs   `codec:"A,omitempty" json:"acls,omitempty"`  // Map of ID -> Permission

	Len          int64     `codec:"l,omitempty" json:"len,omitempty"`         // Length in bytes of uncompressed content
	ETag         string    `codec:"e,omitempty" json:"etag,omitempty"`        // Canonical hash code
	Time         Timestamp `codec:"t" json:"time,omitempty"`                  // Creation time
	Modified     Timestamp `codec:"u,omitempty" json:"modified,omitempty"`    // Last modified time
	Deleted      bool      `codec:"X,omitempty" json:"deleted,omitempty"`     // Indicates the version was deleted
	DeleteMarker bool      `codec:"d,omitempty" json:"delete,omitempty"`      // Delete marker; has no data
	NullVersion  bool      `codec:"N,omitempty" json:"nullVersion,omitempty"` // Null version (versioning disabled or suspended)
	/**** Need to deal with crypt data later
	Crypt        *crypt.Data       `codec:"C,omitempty" json:"crypt,omitempty"`
	*/
	Crypt *string `codec:"C,ignore" json:"crypt,ignore"`

	Metadata     map[string]string `codec:"s,omitempty" json:"meta,omitempty"`
	UserMetadata map[string]string `codec:"m,omitempty" json:"userMeta,omitempty"` // Object metadata for this version
	Tags         map[string]string `codec:"T,omitempty" json:"tags,omitempty"`
	External     string            `codec:"x,omitempty" json:"external,omitempty"` // The external pool this version was created on
	Data         []byte            `codec:"D,omitempty" json:"data,omitempty"`     // object data (if stored in the version record)
	Packs        Packs             `codec:"p,omitempty" json:"packs,omitempty"`    // pack list information when pack list is small
	Reference    *PackReference    `codec:"R,omitempty" json:"ref,omitempty"`      // reference to pack location where backend stored the pack list
}

// HELPER FUNCTIONS FOR RANGE
func NewRange() *Range {
	return &Range{Start: 0, Len: 0}
}
func (r *Range) SetStart(start int64) {
	r.Start = start
}
func (r *Range) SetLength(length int64) {
	r.Len = length
}
func (r *Range) GetStart() int64 {
	return r.Start
}
func (r *Range) GetLength() int64 {
	return r.Len
}
func (r *Range) Add(next *Range) {
	r.Len += next.Len
}

// returns a string so it can be used in higher level Print functions
func (r *Range) Print() string {
	return fmt.Sprintf("%d : %d", r.Start, r.Len)
}

// HELPER FUNCTIONS FOR PACKENTRY
// when we create a new pack entry we know the pack name and the logical start and end
// (SourceRange)
// the physical start and end (PackRange) are set after it is written to the pack
func NewPackEntry(packName string, logicalStart, logicalEnd int64) *PackEntry {
	var entry PackEntry
	entry.PackRange = NewRange()
	entry.SourceRange = NewRange()
	entry.SetPackName(packName)
	entry.SetLogicalStart(logicalStart)
	entry.SetLogicalLength(logicalEnd - logicalStart)
	return &entry
}
func (p *PackEntry) SetPhysicalLocation(packName string, physicalStart, physicalEnd int64) {
	p.SetPackName(packName)
	p.SetPhysicalStart(physicalStart)
	p.SetPhysicalLength(physicalEnd - physicalStart)
}

// phsyical location helper functions
func (p *PackEntry) GetPackName() string {
	return p.Pack
}
func (p *PackEntry) SetPackName(packName string) {
	p.Pack = packName
}
func (p *PackEntry) GetLogicalStart() int64 {
	return p.SourceRange.GetStart()
}
func (p *PackEntry) GetLogicalLength() int64 {
	return p.SourceRange.GetLength()
}
func (p *PackEntry) SetLogicalStart(start int64) {
	p.SourceRange.SetStart(start)
}
func (p *PackEntry) SetLogicalLength(length int64) {
	p.SourceRange.SetLength(length)
}

// phsyical location helper functions
func (p *PackEntry) GetPhysicalStart() int64 {
	return p.PackRange.GetStart()
}
func (p *PackEntry) GetPhysicalLength() int64 {
	return p.PackRange.GetLength()
}
func (p *PackEntry) GetPhysicalEnd() int64 {
	return p.GetPhysicalStart() + p.GetPhysicalLength()
}
func (p *PackEntry) SetPhysicalStart(start int64) {
	p.PackRange.SetStart(start)
}
func (p *PackEntry) SetPhysicalLength(length int64) {
	p.PackRange.SetLength(length)
}

// helper functions for making two sequential pack entries into one
func (p *PackEntry) AddSequentialPacks(nextPack *PackEntry) {
	p.SourceRange.Add(nextPack.SourceRange)
	p.PackRange.Add(nextPack.PackRange)

}

// HELPER FUNCTIONS FOR PACKREFERENCE
func NewPackReference() *PackReference {
	var pr PackReference
	pr.PackRange = NewRange()
	return &pr
}

func (pr *PackReference) GetPackName() string {
	return pr.Pack
}
func (pr *PackReference) GetPhysicalStart() int64 {
	return pr.PackRange.GetStart()
}

// There are five types of TLV records, however the last three PACKLIST, VERSION, DELETEVERSION,
// are all version records defined by the MetaReference structure.
// 1. BLOCK
// 2. PACK LIST
// 3. VERSION
// 4. DELETEVERSION
// 4. METAFILE

// Included in this file is reading of each type along wit the cration of each type
// for use by the simulator

type Block struct {
	VersionInfo string `codec:"I" json:"versionid,omitempty"` // ID of pack containing this offset to next offset
	Bucket string 
	Version string 
	Object string 
	data       []byte
	pack       *PackEntry
}

// NewBlock is used by simulator to create a new block not yet placed in a pack yet
func NewBlock(blockID, bucket, object, version string, data []byte, logicalStart, logicalEnd int64) *Block {

	var block Block

	// fill in the block information that is not encoded
	block.Bucket = bucket
	block.Object = object
	block.Version = version

	// fill in the block information that is encoded
	block.data = data

	// create the pack entry and set the source range, this does not get written to the actual pack
	block.pack = NewPackEntry("", logicalStart, logicalEnd)

	return &block
}

// Read is used by application to read a data Block out of a pack
// a read block does not include the pack information but does include the
// uploadid: versionid, objectid, and the data
func ReadBlock(file *os.File, length uint64,logger *Logger) *Block {

	var b Block
	decoder := value.NewDecoder()
	secondaryData, _, err := decoder.ReadWithBytes(file, &b)
	if err != nil {
		logger.Event("error reading block data:", err)
	}
	if secondaryData == nil {
		logger.Event("Block contains no data")
	}
	b.data = make([]byte, len(secondaryData.Bytes()))
	copy(b.data, secondaryData.Bytes())
	secondaryData.Release()
	// strip components out of versionInfo   download #: version ID: bucket/key
	segments := strings.Split(b.VersionInfo,":")
	if len(segments) != 3 {
		logger.Fatal("Invalid Version Info String From Block: ",b.VersionInfo)
	}
	b.Version = segments[1]
	// now split the bucket/key
	segments = strings.SplitN(segments[2], "/", 2)
        if len(segments) != 2 {
                logger.Fatal("Could not split bucket key", segments[2])
        }
	b.Bucket = segments[0]
	b.Object = segments[1]

	return &b
}

func (b *Block) Pack() *PackEntry {
	return b.pack
}
func (b *Block) GetBucket() string {
	return b.Bucket
}
func (b *Block) GetObject() string {
	return b.Object
}
func (b *Block) GetVersion() string {
	return b.Version
}

func (b *Block) GetData() []byte {
	return b.data
}
func (b *Block) GetLength() int {
	return len(b.data)
}

// Read is used by application to read a pack list Block out of a pack
func ReadPackListRecord(file *os.File, length uint64, logger *Logger) Packs {
	/** FOR SOME REASON ONLY DECODING A SINGLE PACKENTRY, NEED TO TALK JOE ABOUT WHY THIS IS **/
	var pack StoredPack
	decoder := value.NewDecoder()
	_, _, err := decoder.ReadWithBytes(file, &pack)
	if err != nil {
		logger.Fatal(err)
	}

	return pack.Packs
}

// VERSION - Contains a MetaReference Structure
func ReadVersionRecord(file *os.File, length uint64, logger *Logger) *MetaReference {

	var versionRecord MetaReference
	decoder := value.NewDecoder()
	_, _, err := decoder.ReadWithBytes(file, &versionRecord)
	if err != nil {
		logger.Fatal(err)
	}
	return &versionRecord
}
func (mr *MetaReference) GetBucket() string {
	return mr.Bucket
}
func (mr *MetaReference) GetVersion() string {
	return mr.Version
}
func (mr *MetaReference) GetObject() string {
	return mr.Object
}
func (mr *MetaReference) GetBucketObject() string {
	return mr.Bucket + "/" + mr.Object
}
func (mr *MetaReference) GetIsDeleteMarker() bool {
	return mr.DeleteMarker
}
func (mr *MetaReference) GetIsDeleted() bool {
	return mr.Deleted
}
func (mr *MetaReference) GetDataInRecord() []byte {
	return mr.Data
}
func (mr *MetaReference) GetIsPackList() bool {
	if mr.Reference != nil {
		return true
	}
	return false
}
func (mr *MetaReference) GetPacks() Packs {
	return mr.Packs
}
func (mr *MetaReference) GetPackList() *PackReference {
	return mr.Reference
}

// MetaFile marks the beginning of the first file of a full metadata dump.
type MetaFile struct {
	// Oldest gives the unique ID of the oldest file in the full dump of metadata.
	// Any earlier metadata files should be ignored.
	Oldest string `codec:"o" json:"oldest,omitempty"`
}

func ReadMetaFile(file *os.File, length uint64, logger *Logger) *MetaFile {

	var metaFile MetaFile
	decoder := value.NewDecoder()
	_, _, err := decoder.ReadWithBytes(file, &metaFile)
	if err != nil {
		logger.Fatal(err)
	}

	return &metaFile
}
func (mf *MetaFile) GetOldest() string {
	return mf.Oldest
}
