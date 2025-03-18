package tapehardware

import (
	"os/exec"
	"fmt"
	"log"
	"github.com/kbj/mtx"
	"path/filepath"
	"os"
	"strings"
)
type DriveInfo struct {
	slot int
	device string
	mountpoint string
}
type RealTapeLibrary struct{
	mtx    *mtx.Changer
	drives []TapeDrive
	cartridges []TapeCartridge
}
func NewRealTapeLibrary() *RealTapeLibrary {

	// slot to /dev/st* mapping, there is probably a way to do this automatically
	// by using Spectra specific command to get serial number of each drive and then 
	// using ltfs -o devicelist to map each drive to its /dev driver
	// this is hardcoded for the library that Brian provided me

	libraryDevice := "/dev/sch0"
	driveDevice := map[int]*DriveInfo{
		0: {0,"/dev/sg6","/ltfs0"},
		1: {1,"/dev/sg5","/ltfs1"},
		2: {2,"/dev/sg4","/ltfs2"},
		3: {3,"/dev/sg3","/ltfs3"},
	}

	// create the drives
	var rtl RealTapeLibrary

	// initialize mtx
	rtl.mtx = mtx.NewChanger(NewSpectraChanger(libraryDevice))

	// find cartridges in drives
	drives,err := rtl.mtx.Drives()
	if err != nil {
		log.Fatal("Unable to get drive info: ",err)
	}
	// print out drives
	for d, drive := range drives {
		var thisDrive *RealTapeDrive
		// dismount all cartridges in drives
		if (drive.Type == mtx.DataTransferSlot) && drive.Vol != nil {
			// get a home cell for this cartridge
			slot := rtl.findFreeSlot()
			cartridge := NewRealTapeCartridge(slot, mtx.DataTransferSlot, drive.Vol.Serial)

			// add cartridge to drive and unmount and  unload it 
			thisDrive = NewRealTapeDrive(d,drive.Num,cartridge, driveDevice[d])
			rtl.Print()
			thisDrive.Unmount()
			rtl.Unload(thisDrive)
		} else {
			thisDrive = NewRealTapeDrive(d,drive.Num,nil, driveDevice[d])
		}
		rtl.drives = append (rtl.drives, thisDrive)
	}

	// find cartridges in slots
	slots, err := rtl.mtx.Slots()
	rtl.Print()
	if err != nil {
		log.Fatal("Unable to get cartridge info: ",err)
	}
	for _, slot := range slots {
		if slot.Type == mtx.StorageSlot  && slot.Vol != nil {
			rtl.cartridges = append (rtl.cartridges, NewRealTapeCartridge(slot.Num,slot.Type, slot.Vol.Serial))
		}
	}
	return &rtl
}
func (rtl *RealTapeLibrary) Print() {
	drives,err := rtl.mtx.Drives()
	if err != nil {
		log.Fatal("Unable to get drive list")
	}
	for _, drive := range drives {
		fmt.Println("Drive: ",drive)
	}
	slots, err := rtl.mtx.Slots()
	if err != nil {
		log.Fatal("Unable to get slot list")
	}
	for _, slot := range slots {
		fmt.Println("Slot: ",slot)
	}
}
func (rtl *RealTapeLibrary) Audit() ([]TapeDrive, []TapeCartridge) {
	return rtl.drives,rtl.cartridges
}
func (rtl *RealTapeLibrary) Load(cart1 TapeCartridge,drive1 TapeDrive) bool {
	// change to real cart and drives
	cart := cart1.(*RealTapeCartridge)
	drive := drive1.(*RealTapeDrive)

	// get cart and drive slots and perform load
	cartSlot := cart.GetSlot()
	driveSlot := drive.GetSlot()
	rtl.mtx.Load(cartSlot,driveSlot)

	// update drive cartridge held
	drive.SetCart(cart)

	return true
}
func (rtl *RealTapeLibrary) Unload(drive1 TapeDrive) bool {
	// change to real cart and drives
	drive := drive1.(*RealTapeDrive)

	// get slot from cartridge in drive
	cart := drive.GetCart()
	if cart == nil {
		log.Fatal("Unloading a drive without a cartidge")
	}
	// get cartridge and drive slot
	cartSlot := cart.GetSlot()
	driveSlot := drive.GetSlot()

	rtl.mtx.Unload(cartSlot,driveSlot)

	// set drive to no cartridge
	drive.SetCart(nil)
	return true
}
// find first free slot
func (rtl *RealTapeLibrary) findFreeSlot() int {
	slots, err := rtl.mtx.Slots()
	if err != nil {
		log.Fatal("Unable to get cartridge info: ",err)
	}
	for _, s := range slots {
		if s.Vol == nil {
			return s.Num
		}
	}
	log.Fatal("No slots available")
	return 0
}
//**** REAL TAPE DRIVE ********
type RealTapeDrive struct {
	id int
	slot int
	driveInfo *DriveInfo
	cartridge *RealTapeCartridge
}

func NewRealTapeDrive (id, slot int, cartridge *RealTapeCartridge, info *DriveInfo) *RealTapeDrive{
	return &RealTapeDrive {
		id: id,
		slot: slot,
		driveInfo: info,
		cartridge: cartridge,
	}
}
func (rtd RealTapeDrive) Print() {
	fmt.Println("id: ",rtd.id,"  slot: ",rtd.slot,"  device: ",rtd.driveInfo.device,"  cart: ",rtd.cartridge)
}
func (rtd *RealTapeDrive) GetSlot() int {
	return rtd.slot
}
func (rtd *RealTapeDrive) GetCart()*RealTapeCartridge {
	return rtd.cartridge
}
func (rtd *RealTapeDrive) SetCart(cart *RealTapeCartridge) {
	rtd.cartridge = cart
}
// returns the mountpoint, and a map of 
func (rtd RealTapeDrive) MountLTFS() (map[string]string, map[string]string,bool) {
	devname := fmt.Sprintf("devname=%s",rtd.driveInfo.device)
	_, err := exec.Command("ltfs", "-o",devname,rtd.driveInfo.mountpoint).Output()
	if err != nil {
		return nil, nil, false
	}
	vfiles, bfiles :=  FindVersionAndBlockFiles(rtd.driveInfo.mountpoint ) 

	return vfiles, bfiles, true
}
// umounts the mount point
func (rtd RealTapeDrive) Unmount() {
	exec.Command("umount", rtd.driveInfo.mountpoint).Output()
}
func (rtd RealTapeDrive) Name() string {
	return fmt.Sprintf("Drive%d",rtd.id)
}
//**** REAL TAPE CARTRIDGE ********

type RealTapeCartridge struct {
	currentSlot int
	slotType mtx.SlotType 
	volser string
}
func NewRealTapeCartridge (slot int, slotType mtx.SlotType, volser string) *RealTapeCartridge {
	return &RealTapeCartridge {
		currentSlot: slot,
		slotType: slotType,
		volser: volser,
	}
}
func (rtc RealTapeCartridge) Print() {
	fmt.Println("slot: ",rtc.currentSlot," type: ",rtc.slotType,"  volser: ",rtc.volser) 
}
func (rtc RealTapeCartridge) Name() string {
	return rtc.volser
}
func (rtc *RealTapeCartridge) GetSlot() int {
	return rtc.currentSlot
}
func (rtc *RealTapeCartridge) UpdateSlot(slotType mtx.SlotType,slot int) {
	rtc.slotType = slotType
	rtc.currentSlot = slot
}

//**** MTX PROVIDER  ********
type Changer struct {
	device string
}
func NewSpectraChanger(device string) *Changer {

	return &Changer{
		device: device,
	}
}
func (c *Changer) Do(args ...string) ([]byte, error) {
	var returnValue []byte
	var err error
	switch len(args) {
	case 1:
	fmt.Printf("%s %s %s %s\n","mtx", "-f",c.device ,args[0])		
	returnValue, err = exec.Command("mtx", "-f",c.device ,args[0]).Output()		
	case 2:
	fmt.Printf("%s %s %s %s %s\n","mtx", "-f",c.device ,args[0],args[1])		
	returnValue, err = exec.Command("mtx", "-f",c.device,args[0],args[1]).Output()		
	case 3:
	fmt.Printf("%s %s %s %s %s %s\n","mtx", "-f",c.device ,args[0],args[1],args[2])		
	returnValue, err = exec.Command("mtx", "-f",c.device,args[0],args[1],args[2]).Output()		
case 5:
	log.Fatal("Invalid number of args")
}
return returnValue, err
}
// returns a map of version and block files with the key being the ULID and the 
// the element being the path not including the mountpoint as it can change the next
// time the tape is mounted
const VersionSuffix=".ver"
const BlockSuffix=".blk"
func FindVersionAndBlockFiles(mountPoint string) (versionFiles, blockFiles map[string]string) {
      path := mountPoint
      versionFiles = make(map[string]string)
      blockFiles = make(map[string]string)
      err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
                if err != nil {
                        return err
                }
		version, block := false,false
		var key string

                // see if it has either a version of block suffix
                if strings.HasSuffix(path, VersionSuffix) {
			version = true
			// start key by trimming suffix
			key = strings.TrimSuffix(path,VersionSuffix)
		} else if strings.HasSuffix(path, BlockSuffix) {
			block = true
			// start key by trimming suffix
			key = strings.TrimSuffix(path,BlockSuffix)
		}
		// if neither suffix found then this is a directory
		if !version && !block {
			return nil
		}
		// trim the prefix off of key
		key = filepath.Base(key)

		// add to either version or the block map
		if version {
			versionFiles[key] = path
		} else if block {
			blockFiles[key] = path
                }
                return nil
        })

        if err != nil {
                fmt.Println("Error:", err)
        }
	return versionFiles,blockFiles
}

