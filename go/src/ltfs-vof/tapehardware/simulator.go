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

func NewTapeLibrarySimulator(tapeDirectory string, numDrives int, logger *Logger) *TapeLibrarySimulator {

	var simulator TapeLibrarySimulator
	simulator.tapeDirectory = tapeDirectory
	simulator.logger = logger

	// create drives based on number of drives
	for i := 0; i < numDrives; i++ {
		simulator.drives = append(simulator.drives, NewTapeDriveSimulator(i, tapeDirectory, logger))
	}
	// find the number of simulated tapes by opening up the sumlator directory
	dirs, err := os.ReadDir(tapeDirectory)
	if err != nil {
		simulator.logger.Fatal(err)
	}
	var tapes []string
	for _, dir := range dirs {
		if !dir.IsDir() {
			continue
		}
		tapes = append(tapes, dir.Name())
	}
	fmt.Println("Found tapes in simulator directory: ", tapes)
	for slot, tape := range tapes {
		simulator.tapes = append(simulator.tapes, NewTapeCartridgeSimulator(slot, tape))
		slot++
	}
	return &simulator
}

// FUNCTIONS THAT IMPLEMENT THE TAPE LIBRARY INTERFACE
func (t *TapeLibrarySimulator) Audit() ([]TapeDrive, []TapeCartridge) {
	fmt.Println("Tape Cartridges in Library:", t.tapes)
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
	versionFiles, blockFiles := FindVersionAndBlockFiles(tapeDirectory)
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
