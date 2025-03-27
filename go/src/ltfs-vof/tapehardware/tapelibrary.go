package tapehardware

import (
	"os/exec"
	"log"
	"github.com/kbj/mtx"
	"path/filepath"
	"os"
	"strings"
	"fmt"
)
// tape drive device info read from json file
type TapeDriveDevice struct {
        Slot int `json:"slot"`
        Device string `json:"Device"`
 	MountPoint string `json:"MountPoint"`
}

type RealTapeLibrary struct{
	mtx    *mtx.Changer
	drives []TapeDrive
	cartridges []TapeCartridge
}
func NewRealTapeLibrary(libraryDevice string, tapeDevices map[int]*TapeDriveDevice) *RealTapeLibrary {

	// create the drives
	var rtl RealTapeLibrary

	// initialize mtx
	rtl.mtx = mtx.NewChanger(NewSpectraChanger(libraryDevice))

	// find cartridges in drives
	drives,err := rtl.mtx.Drives()
	if err != nil {
		log.Fatal("Unable to get drive info: ",err)
	}
	// see if drives have cartridges in them
	for d, drive := range drives {
		var thisDrive *RealTapeDrive
		// if it then create a cartridge, assign it a home cell and put it on the cartridge list
		var cartridge *RealTapeCartridge
		if drive.Type == mtx.DataTransferSlot && drive.Vol != nil {
			slot := rtl.findFreeSlot()
			rtl.cartridges = append (rtl.cartridges, NewRealTapeCartridge(slot, mtx.DataTransferSlot, drive.Vol.Serial))
		}

		// create the drive, unmount it and then put it on list
		thisDrive = NewRealTapeDrive(d,drive.Num,cartridge, tapeDevices[d])
		thisDrive.Unmount()
		rtl.drives = append (rtl.drives, thisDrive)
	}

	// find cartridges in slots
	slots, err := rtl.mtx.Slots()
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
	cart1,exists := drive.GetCart()
	if !exists {
		log.Fatal("Unloading a drive without a cartidge")
	}
	cart := cart1.(*RealTapeCartridge)
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
	driveInfo *TapeDriveDevice
	cartridge *RealTapeCartridge
	cartridgeExists bool
}

func NewRealTapeDrive (id, slot int, cartridge *RealTapeCartridge, info *TapeDriveDevice) *RealTapeDrive{
	var rtd RealTapeDrive 
	rtd.id = id
	rtd.slot = slot
	rtd.driveInfo = info
	if cartridge != nil {
		rtd.cartridgeExists = true
		rtd.cartridge =  cartridge
	} else {
		rtd.cartridgeExists = false
	}

	return &rtd
}
func (rtd RealTapeDrive) Print() {
	fmt.Println("id: ",rtd.id,"  slot: ",rtd.slot,"  device: ",rtd.driveInfo.Device,"  cart: ",rtd.cartridge)
}
func (rtd *RealTapeDrive) GetSlot() int {
	return rtd.slot
}
func (rtd *RealTapeDrive) GetCart() (TapeCartridge, bool) {
	if rtd.cartridge == nil {
		return nil, false
	}
	return rtd.cartridge, rtd.cartridgeExists
}
func (rtd *RealTapeDrive) SetCart(cart *RealTapeCartridge) {
	rtd.cartridgeExists = true
	rtd.cartridge = cart
}
func (rtd *RealTapeDrive) ClearCart() {
	rtd.cartridgeExists = false
}
	
// returns the mountpoint, and a map of 
func (rtd RealTapeDrive) MountLTFS() (map[string]string, map[string]string,bool) {
	// unmount drive prior to doing mount, if it isn't mounted unmount will fail but no big deal 
	rtd.Unmount()
	devname := fmt.Sprintf("devname=%s",rtd.driveInfo.Device)
	
	_, err := exec.Command("ltfs", "-o",devname,rtd.driveInfo.MountPoint).Output()
	if err != nil {
		return nil, nil, false
	}
	vfiles, bfiles :=  FindVersionAndBlockFiles(rtd.driveInfo.MountPoint) 
	return vfiles, bfiles, true
}
// umounts the mount point
func (rtd RealTapeDrive) Unmount() {
	exec.Command("umount", rtd.driveInfo.MountPoint).Output()
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
	returnValue, err = exec.Command("mtx", "-f",c.device ,args[0]).Output()		
	case 2:
	returnValue, err = exec.Command("mtx", "-f",c.device,args[0],args[1]).Output()		
	case 3:
	returnValue, err = exec.Command("mtx", "-f",c.device,args[0],args[1],args[2]).Output()		
	default:
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

