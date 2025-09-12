// used to do a quick test on code by useing realpacks
package tapehardware

import (
	"fmt"
	. "ltfs-vof/utils"
	"os"
)

type TapeLibrarySimulator struct {
	drives        []TapeDrive
	tapes         []TapeCartridge
	logger        *Logger
	tapeDirectory string
}
type TapeDriveSimulator struct {
	name          string
	tape          *TapeCartridgeSimulator
	number        int
	busy          bool
	tapeDirectory string
	logger        *Logger
}
type TapeCartridgeSimulator struct {
	name string
	slot int
}

const NumDrives int = 1

func NewTapeLibrarySimulator(tapeDirectory string, logger *Logger) *TapeLibrarySimulator {

	var simulator TapeLibrarySimulator
	simulator.tapeDirectory = tapeDirectory
	simulator.logger = logger

	// create drives based on number of drives
	for i := 0; i < NumDrives; i++ {
		simulator.drives = append(simulator.drives, NewTapeDriveSimulator(i, tapeDirectory, logger))
	}
	// find the number of simulated tapes by opening up the sumlator directory
	tapes, err := os.ReadDir(tapeDirectory)
	if err != nil {
		simulator.logger.Fatal(err)
	}
	for slot, tape := range tapes {
		fmt.Println("Found tape:", tape.Name())
		simulator.tapes = append(simulator.tapes, NewTapeCartridgeSimulator(slot, tape.Name()))
		slot++
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
		t.logger.Fatal("drive busy")
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
func NewTapeDriveSimulator(i int, tapeDirectory string, logger *Logger) *TapeDriveSimulator {
	drive := TapeDriveSimulator{
		name:          fmt.Sprintf("Drive-%d", i),
		busy:          false,
		tapeDirectory: tapeDirectory,
		logger:        logger,
	}
	return &drive
}
func (td *TapeDriveSimulator) SerialNumber() (string, bool) {
	return td.name, true
}
func (td *TapeDriveSimulator) GetCart() (TapeCartridge, bool) {
	if !td.busy {
		return nil, false
	}
	return td.tape, true
}

func (td *TapeDriveSimulator) MountLTFS() (map[string]string, map[string]string, bool) {
	if !td.busy {
		td.logger.Fatal("drive not busy")
	}
	// tape directory is current directory + tape
	tapeDirectory := td.tapeDirectory + td.tape.Name()
	fmt.Println("Tape Directory:", tapeDirectory)
	versionFiles, blockFiles := FindVersionAndBlockFiles(tapeDirectory)
	fmt.Println("Version Files:", versionFiles, "Block Files:", blockFiles)
	return versionFiles, blockFiles, true
}
func (td *TapeDriveSimulator) Unmount() {
}
func NewTapeCartridgeSimulator(i int, name string) *TapeCartridgeSimulator {
	var cart TapeCartridgeSimulator
	cart.slot = i
	cart.name = name
	return &cart
}
func (c *TapeCartridgeSimulator) GetSlot() int {
	return c.slot
}
func (c *TapeCartridgeSimulator) Name() string {
	return c.name
}
