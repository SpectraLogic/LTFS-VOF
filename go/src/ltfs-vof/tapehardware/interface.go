// interface each tape library combination needs to adhere to
package tapehardware

import ()

type TapeLibrary interface {
	Audit() ([]TapeDrive, []TapeCartridge)
	Load(TapeCartridge, TapeDrive) bool
	Unload(TapeDrive) bool
}
type TapeDrive interface {
	MountLTFS() (map[string]string, map[string]string, bool)
	Unmount()
	GetCart() (TapeCartridge, bool)
	SerialNumber() (string, bool)
}
type TapeCartridge interface {
	Name() string
	GetSlot() int
}
