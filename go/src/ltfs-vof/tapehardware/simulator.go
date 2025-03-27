// used to do a quick test on code by useing realpacks
package tapehardware

import (
	"log"
	"os"
	"fmt"
)

type TapeLibrarySimulator struct {
	drives []TapeDrive
	tapes  []TapeCartridge
}
type TapeDriveSimulator struct {
	name          string
	tape          *TapeCartridgeSimulator
	number        int
	busy          bool
	tapeDirectory string
}
type TapeCartridgeSimulator struct {
	name         string
}

const NumDrives int = 1
const NumTapes int = 2
const TapeDirectory string = "tapehardware/"

func NewTapeLibrarySimulator() *TapeLibrarySimulator {

	var simulator TapeLibrarySimulator

	// create drives based on number of drives
	for i := 0; i < NumDrives; i++ {
		simulator.drives = append(simulator.drives, NewTapeDriveSimulator(i, TapeDirectory))
	}
	// create tapes based on number
	for i := 0; i < NumTapes; i++ {
		simulator.tapes = append(simulator.tapes, NewTapeCartridgeSimulator(i))
	}
	return &simulator
}

// FUNCTIONS THAT IMPLEMENT THE TAPE LIBRARY INTERFACE
func (t *TapeLibrarySimulator) Audit() ([]TapeDrive, []TapeCartridge) {
	return t.drives, t.tapes
}
func (t *TapeLibrarySimulator) Load(tape TapeCartridge, drive TapeDrive) bool {
	td := drive.(*TapeDriveSimulator)
	if td.busy {
		log.Fatal("drive busy")
	}
	td.tape = tape.(*TapeCartridgeSimulator)
	td.busy = true
	return true
}
func (t *TapeLibrarySimulator) Unload(drive TapeDrive) bool {
	td := drive.(*TapeDriveSimulator)
	if !td.busy {
		return false
	}
	td.busy = false
	return true
}
func NewTapeDriveSimulator(i int, tapeDirectory string) *TapeDriveSimulator {
	drive := TapeDriveSimulator{
		name: fmt.Sprintf("Drive-%d", i),
		busy: false,
	}
	return &drive
}
func (td *TapeDriveSimulator) Name() string {
	return td.name
}
func (td *TapeDriveSimulator) GetCart() (TapeCartridge, bool){
	if !td.busy  {
		return nil,false
	}
	return td.tape, true
}

func (td *TapeDriveSimulator) MountLTFS() ( map[string]string, map[string]string, bool) {
	if !td.busy {
		log.Fatal("drive not busy")
	}
	// get current directory
	currentDir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	// tape directory is current directory + tape
	tapeDirectory := currentDir + "/" + TapeDirectory + td.tape.Name()
	versionFiles, blockFiles := FindVersionAndBlockFiles(tapeDirectory)
	return versionFiles,blockFiles, true
}
func (td *TapeDriveSimulator) Unmount() {
}
func NewTapeCartridgeSimulator(i int) *TapeCartridgeSimulator {
	var cart TapeCartridgeSimulator
	cart.name = fmt.Sprintf("tape%d", i)
	return &cart
}
func (c *TapeCartridgeSimulator) Name() string {
	return c.name
}
