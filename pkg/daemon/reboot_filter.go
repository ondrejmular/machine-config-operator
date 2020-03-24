package daemon

import (
	"os/exec"
	"path/filepath"
	"reflect"

	igntypes "github.com/coreos/ignition/config/v2_2/types"
	"github.com/deckarep/golang-set"
	"github.com/golang/glog"
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

func (config AvoidRebootConfig) GetFileAction(filePath string) PostUpdateAction {
	for _, entry := range config.Files {
		matched, err := filepath.Match(entry.Glob, filePath)
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

func (config AvoidRebootConfig) GetUnitAction(unitName string) PostUpdateAction {
	for _, entry := range config.Units {
		if entry == unitName {
			return RunSystemctlAction{
				unitName:  unitName,
				operation: unitRestart,
			}
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
	binary string
	args   []string
}

func (action RunBinaryAction) Run() error {
	glog.Infof(
		"Running post update action: running command: %v %v", action.binary, action.args,
	)
	output, err := exec.Command(action.binary, action.args...).CombinedOutput()
	// TODO: Add some timeout?
	if err != nil {
		glog.Errorf("Running post update action (running command: '%s %s') failed: %s; command output: %s", action.binary, action.args, err, output)
		return err
	}
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
	glog.Warningf(
		"Systemd post update action not implemented! Unit: %s; Operation: %s",
		action.unitName,
		action.operation,
	)
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
		// &FileFilterEntry{
		// 	Glob: "/etc/kubernetes/kubelet.conf",
		// 	PostUpdateAction: RunSystemctlAction{
		// 		unitName:  "kubelet.service",
		// 		operation: unitReload,
		// 	},
		// 	Drain: false,
		// },
		&FileFilterEntry{
			Glob: "/home/core/testfile",
			PostUpdateAction: RunBinaryAction{
				binary: "/bin/bash",
				args: []string{
					"-c",
					"echo \"$(date)\" >> /home/core/testfile.out",
				},
			},
			Drain: false,
		},
	},
	// Units: []string{"chronyd.service", "sshd.service"},
	Units: []string{"testonly.service"},
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
	for i, file := range files {
		names[i] = file.Path
	}
	return names
}

func filesToMap(files []igntypes.File) map[string]igntypes.File {
	fileMap := make(map[string]igntypes.File, len(files))
	for _, file := range files {
		fileMap[file.Path] = file
	}
	return fileMap
}

func getFilesChanges(oldFilesConfig, newFilesConfig []igntypes.File) []*FileChanged {
	oldFiles := mapset.NewSetFromSlice(getFileNames(oldFilesConfig))
	oldFilesMap := filesToMap(oldFilesConfig)
	newFiles := mapset.NewSetFromSlice(getFileNames(newFilesConfig))
	newFilesMap := filesToMap(newFilesConfig)
	changes := make([]*FileChanged, 0, newFiles.Cardinality())
	for created := range newFiles.Difference(oldFiles).Iter() {
		changes = append(changes, &FileChanged{
			name:       created.(string),
			file:       newFilesMap[created.(string)],
			changeType: fileCreated,
		})
	}
	for deleted := range oldFiles.Difference(newFiles).Iter() {
		changes = append(changes, &FileChanged{
			name:       deleted.(string),
			file:       oldFilesMap[deleted.(string)],
			changeType: fileDeleted,
		})
	}
	for changeCandidate := range newFiles.Intersect(oldFiles).Iter() {
		newFile := newFilesMap[changeCandidate.(string)]
		// TODO: check against the state on the disk
		if !reflect.DeepEqual(newFile, oldFilesMap[changeCandidate.(string)]) {
			changes = append(changes, &FileChanged{
				name:       changeCandidate.(string),
				file:       newFile,
				changeType: fileUpdated,
			})
		}
	}
	return changes
}

func handleFilesChanges(changes []*FileChanged) (err error) {
	for _, change := range changes {
		switch change.changeType {
		case fileCreated:
			fallthrough
		case fileUpdated:
			err = writeFile(change.file)
		case fileDeleted:
			err = deleteFile(change.name)
		default:
			err = nil
		}
		if err != nil {
			return
		}
	}
	return
}

func getUnitNames(units []igntypes.Unit) []interface{} {
	names := make([]interface{}, len(units))
	for i, unit := range units {
		names[i] = unit.Name
	}
	return names
}

func unitsToMap(units []igntypes.Unit) map[string]*igntypes.Unit {
	unitMap := make(map[string]*igntypes.Unit, len(units))
	for _, unit := range units {
		unitMap[unit.Name] = &unit
	}
	return unitMap
}

type UnitChanged struct {
	name       string
	oldUnit    *igntypes.Unit
	newUnit    *igntypes.Unit
	changeType FileChangeType
}

func getUnitsChanges(oldUnitsConfig, newUnitsConfig []igntypes.Unit) []*UnitChanged {
	oldUnits := mapset.NewSetFromSlice(getUnitNames(oldUnitsConfig))
	oldUnitsMap := unitsToMap(oldUnitsConfig)
	newUnits := mapset.NewSetFromSlice(getUnitNames(newUnitsConfig))
	newUnitsMap := unitsToMap(newUnitsConfig)
	changes := make([]*UnitChanged, 0, newUnits.Cardinality())
	for created := range newUnits.Difference(oldUnits).Iter() {
		changes = append(changes, &UnitChanged{
			name:       created.(string),
			newUnit:    newUnitsMap[created.(string)],
			oldUnit:    nil,
			changeType: fileCreated,
		})
	}
	for deleted := range oldUnits.Difference(newUnits).Iter() {
		changes = append(changes, &UnitChanged{
			name:       deleted.(string),
			newUnit:    nil,
			oldUnit:    oldUnitsMap[deleted.(string)],
			changeType: fileDeleted,
		})
	}
	for changeCandidate := range newUnits.Intersect(oldUnits).Iter() {
		newUnit := newUnitsMap[changeCandidate.(string)]
		oldUnit := oldUnitsMap[changeCandidate.(string)]
		// TODO: check against the state on the disk, use checkUnits()
		if !reflect.DeepEqual(newUnit, oldUnit) {
			changes = append(changes, &UnitChanged{
				name:       changeCandidate.(string),
				newUnit:    newUnit,
				oldUnit:    oldUnit,
				changeType: fileUpdated,
			})
		}
	}
	return changes
}

func handleUnitsChanges(changes []*UnitChanged) (err error) {
	for _, change := range changes {
		switch change.changeType {
		case fileCreated:
			err = createUnit(change.newUnit)
		case fileUpdated:
			err = deleteUnit(change.oldUnit)
			if err != nil {
				// TODO: try to write it back or do it in roll-back?
				return
			}
			err = createUnit(change.newUnit)
		case fileDeleted:
			err = deleteUnit(change.oldUnit)
		default:
			err = nil
		}
		if err != nil {
			return
		}
	}
	return
}

func runPostUpdateActions(filesChanges []*FileChanged, unitsChanges []*UnitChanged) bool {
	glog.Info("Trying to check whether changes in files and unit require reboot.")
	actions := make([]PostUpdateAction, 0, len(filesChanges)+len(unitsChanges))
	for _, change := range filesChanges {
		switch change.changeType {
		case fileCreated:
			fallthrough
		case fileUpdated:
			action := FilterConfig.GetFileAction(change.name)
			if action == nil {
				glog.Infof("No action found for file %q, reboot will be required", change.name)
				return true
			}
			actions = append(actions, action)
			glog.Infof("Action found for file %q", change.name)
		default:
			glog.Infof("File %q was removed, reboot will be required", change.name)
			return true
		}
	}

	for _, change := range unitsChanges {
		switch change.changeType {
		case fileCreated:
			fallthrough
		case fileUpdated:
			action := FilterConfig.GetUnitAction(change.name)
			if action == nil {
				glog.Infof("No action found for unit %q, reboot will be required", change.name)
				return true
			}
			actions = append(actions, action)
			glog.Infof("Action found for unit %q", change.name)
		default:
			glog.Infof("Unit %q was removed, reboot will be required", change.name)
			return true
		}
	}

	glog.Infof("Running %d post update action(s)...", len(actions))
	for _, action := range actions {
		if err := action.Run(); err != nil {
			glog.Errorf("Post update action failed: %s", err)
			return true
		}
	}
	glog.Info("Running post update Actions were sucessfull")
	return false
}
