package daemon

import (
	"fmt"
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

var FilterConfig = AvoidRebootConfig{
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
	name       string
	file       igntypes.File
	changeType FileChangeType
}

func getFileNames(files []igntypes.File) []interface{} {
	names := make([]interface{}, len(files))
	for _, file := range files {
		names = append(names, file.Path)
	}
	return names
}

func convertFiles(files []igntypes.File) map[string]igntypes.File {
	fileMap := make(map[string]igntypes.File, len(files))
	for _, file := range files {
		fileMap[file.Path] = file
	}
	return fileMap
}

func GetFilesDiff(oldFilesConfig, newFilesConfig []igntypes.File) ([]*FileChanged, error) {
	oldFiles := mapset.NewSetFromSlice(getFileNames(oldFilesConfig))
	oldFilesMap := convertFiles(oldFilesConfig)
	newFiles := mapset.NewSetFromSlice(getFileNames(newFilesConfig))
	newFilesMap := convertFiles(newFilesConfig)
	changes := make([]*FileChanged, newFiles.Cardinality())
	var ok bool
	var oldFile igntypes.File
	var newFile igntypes.File
	for created := range oldFiles.Difference(newFiles).Iter() {
		newFile, ok = newFilesMap[created.(string)]
		if !ok {
			// TODO: proper errro message, assertion
			return nil, fmt.Errorf("ERROR")
		}
		changes = append(changes, &FileChanged{
			name:       created.(string),
			file:       newFile,
			changeType: fileCreated,
		})
	}
	for deleted := range newFiles.Difference(oldFiles).Iter() {
		oldFile, ok = oldFilesMap[deleted.(string)]
		if !ok {
			// TODO: proper error message, assertion
			return nil, fmt.Errorf("ERROR")
		}
		changes = append(changes, &FileChanged{
			name:       deleted.(string),
			file:       oldFile,
			changeType: fileDeleted,
		})
	}
	for changeCandidate := range newFiles.Intersect(oldFiles).Iter() {
		// TODO: check agains what is on a filesystem
		oldFile, ok = oldFilesMap[changeCandidate.(string)]
		if !ok {
			// TODO: proper errro message, assertion
			return nil, fmt.Errorf("ERROR")
		}
		newFile, ok = newFilesMap[changeCandidate.(string)]
		if !ok {
			// TODO: proper errro message, assertion
			return nil, fmt.Errorf("ERROR")
		}
		if newFile.Contents.Source != oldFile.Contents.Source {
			changes = append(changes, &FileChanged{
				name:       changeCandidate.(string),
				file:       newFile,
				changeType: fileUpdated,
			})
		}
	}
	return changes, nil
}
