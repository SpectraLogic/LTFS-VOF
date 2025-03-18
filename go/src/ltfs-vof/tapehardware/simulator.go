package tapehardware

import (
	"fmt"
	"log"
	"os"
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
	CapacityLeft int
}

const numDrives int = 1
const numTapes int = 1
const tapeCapacity int = 100000
const tapeDirectory string = "tapehardware/tapes"

func NewTapeLibrarySimulator() *TapeLibrarySimulator {

	var simulator TapeLibrarySimulator

	// create drives based on number of drives
	for i := 0; i < numDrives; i++ {
		simulator.drives = append(simulator.drives, NewTapeDriveSimulator(i, tapeDirectory))
	}
	// create tapes based on number
	for i := 0; i < numTapes; i++ {
		simulator.tapes = append(simulator.tapes, NewTapeCartridgeSimulator(i, tapeCapacity))
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
	currentDir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	drive.tapeDirectory = currentDir + "/" + tapeDirectory
	return &drive
}
func (td *TapeDriveSimulator) Name() string {
	return td.name
}

func (td *TapeDriveSimulator) MountLTFS() ( map[string]string, map[string]string, bool) {
	if !td.busy {
		log.Fatal("drive not busy")
	}
	// need to work on this if we are going to use it
	return nil, nil, true
}
func (td *TapeDriveSimulator) Unmount() {
}
func NewTapeCartridgeSimulator(i, capacity int) *TapeCartridgeSimulator {
	var tape TapeCartridgeSimulator
	tape.name = fmt.Sprintf("tape%d", i)
	tape.CapacityLeft = capacity
	return &tape
}
func (c *TapeCartridgeSimulator) Name() string {
	return c.name
}
