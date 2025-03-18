package tapehardware

import ()

// interfaces for tape library, tape drive and tape cartridge
type TapeLibrary interface {
	Audit() ([]TapeDrive, []TapeCartridge)
	Load(TapeCartridge, TapeDrive) bool
	Unload(TapeDrive) bool
}
type TapeDrive interface {
	MountLTFS() (map[string]string, map[string]string, bool)
	Unmount() 
	Name() string
}
type TapeCartridge interface {
	Name() string
}
