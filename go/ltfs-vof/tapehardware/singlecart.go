package tapehardware

import (
	"os/exec"
	"log"
	"fmt"
)
type SingleCartTapeLibrary struct{
	drives []TapeDrive
	cartridges []TapeCartridge
}
var CartridgeLabel string="700124L6"

func NewSingleCartTapeLibrary() *SingleCartTapeLibrary {

	// slot to /dev/st* mapping, there is probably a way to do this automatically
	// by using Spectra specific command to get serial number of each drive and then 
	// using ltfs -o devicelist to map each drive to its /dev driver
	// this is hardcoded for the library that Brian provided me

	driveDevice := map[int]*DriveInfo{
		0: {0,"/dev/sg6","/ltfs0"},
	}
	var rtl SingleCartTapeLibrary

	// make the cartridge
	rtl.cartridges = append (rtl.cartridges, NewSingleCartTapeCartridge(CartridgeLabel))


	// Make a single drive
	rtl.drives = append(rtl.drives, NewSingleCartTapeDrive(driveDevice[0]))

	// Unmount cartridge in case it is mounted
	rtl.drives[0].Unmount()

	return &rtl
}
func (rtl *SingleCartTapeLibrary) Audit() ([]TapeDrive, []TapeCartridge) {
	return rtl.drives,rtl.cartridges
}
func (rtl *SingleCartTapeLibrary) Load(cart1 TapeCartridge,drive1 TapeDrive) bool {
	return true
}
func (rtl *SingleCartTapeLibrary) Unload(drive1 TapeDrive) bool {
	return true
}
type SingleCartTapeDrive struct {
	driveInfo *DriveInfo
}

func NewSingleCartTapeDrive (info *DriveInfo) *SingleCartTapeDrive{
	return &SingleCartTapeDrive {
		driveInfo: info,
	}
}
func (rtd SingleCartTapeDrive) MountLTFS() (map[string]string, map[string]string,bool) {
	devname := fmt.Sprintf("devname=%s",rtd.driveInfo.device)
        _, err := exec.Command("ltfs", "-o",devname,rtd.driveInfo.mountpoint).Output()
	if err != nil {
		log.Fatal("Unable to perform LTFS mount")
	}
	vfiles, bfiles := FindVersionAndBlockFiles(rtd.driveInfo.mountpoint)
	return vfiles, bfiles, true
}
// umounts the mount point
func (rtd SingleCartTapeDrive) Unmount() {
	exec.Command("umount", rtd.driveInfo.mountpoint).Output()
}
func (rtd SingleCartTapeDrive) Name() string {
	return fmt.Sprintf("Drive0")
}
//**** REAL TAPE CARTRIDGE ********

type SingleCartTapeCartridge struct {
	volser string
}
func NewSingleCartTapeCartridge (volser string) *SingleCartTapeCartridge {
	return &SingleCartTapeCartridge {
		volser: volser,
	}
}
func (rtc SingleCartTapeCartridge) Name() string {
	return rtc.volser
}

