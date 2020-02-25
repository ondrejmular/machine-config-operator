package daemon

import (
	"path/filepath"

	igntypes "github.com/coreos/ignition/config/v2_2/types"
	"github.com/deckarep/golang-set"
)

type FileFilterEntry struct {
	Glob             string
	PostUpdateAction PostUpdateAction
	Drain            bool
}

type AvoidRebootConfig struct {
	// Files filter which do not require
	Files []*FileFilterEntry
	// List of systemd unit that do not require system reboot, but rather just unit restart
	Units []string
}

func (config *AvoidRebootConfig) GetAction(file igntypes.File) PostUpdateAction {
	for _, entry := range config.Files {
		matched, err := filepath.Match(entry.Glob, file.Path)
		if err != nil {
			// TODO: log
			continue
		}
		if matched {
			return entry.PostUpdateAction
		}
	}
	return nil
}

type PostUpdateAction interface {
	Run() error
	// TODO: add dbus connection setup
	// SetDbusConnection()
}

type RunBinaryAction struct {
	binary     string
	args       []string
	expectedRc int
}

func (action RunBinaryAction) Run() error {
	// TODO: implement
	return nil
}

type UnitOperation string

const (
	unitRestart UnitOperation = "restart"
	unitReload  UnitOperation = "reload"
)

type RunSystemctlAction struct {
	unitName  string
	operation UnitOperation
	// TODO: add systemd dbus connection
}

func (action RunSystemctlAction) Run() error {
	// TODO: implement
	// https://godoc.org/github.com/coreos/go-systemd/dbus
	return nil
}

type NoOpAction struct {
}

func (action NoOpAction) Run() error {
	return nil
}

var FilterConfig AvoidRebootConfig = AvoidRebootConfig{
	Files: []*FileFilterEntry{
		&FileFilterEntry{
			Glob: "/etc/kubernetes/kubelet.conf",
			PostUpdateAction: RunSystemctlAction{
				unitName:  "kubelet.service",
				operation: unitReload,
			},
			Drain: false,
		},
	},
	Units: []string{"chronyd.service", "sshd.service"},
}

type FileChangeType string

const (
	fileCreated FileChangeType = "created"
	fileDeleted FileChangeType = "deleted"
	fileUpdated FileChangeType = "updated"
)

type FileChanged struct {
	file       string
	changeType FileChangeType
}

func getFileNames(files []igntypes.File) []interface{} {
	names := make([]interface{}, len(files))
	for _, file := range files {
		names = append(names, file.Path)
	}
	return names
}

func getFilesDiff(oldFilesConfig, newFilesConfig []igntypes.File) []*FileChanged {
	oldFiles := mapset.NewSetFromSlice(getFileNames(oldFilesConfig))
	newFiles := mapset.NewSetFromSlice(getFileNames(newFilesConfig))
	changes := make([]*FileChanged, newFiles.Cardinality())
	for created := range oldFiles.Difference(newFiles).Iter() {
		changes = append(changes, &FileChanged{
			file:       created.(string),
			changeType: fileCreated,
		})
	}
	for deleted := range newFiles.Difference(oldFiles).Iter() {
		changes = append(changes, &FileChanged{
			file:       deleted.(string),
			changeType: fileDeleted,
		})
	}
	// for changeCandidate := range newFiles.Intersect(oldFiles).Iter() {
	//
	// }
	return changes
}
