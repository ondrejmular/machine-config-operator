package daemon

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"reflect"

	igntypes "github.com/coreos/ignition/config/v2_2/types"
	"github.com/deckarep/golang-set"
	"github.com/golang/glog"
)

type FileFilterEntry struct {
	glob             string
	postUpdateAction PostUpdateAction
}

type UnitFilterEntry struct {
	name          string
	drainRequired bool
}

type AvoidRebootConfig struct {
	// Files filter which do not require reboot
	Files []*FileFilterEntry
	// List of systemd unit that do not require system reboot, but rather just unit restart
	Units []*UnitFilterEntry
}

var filterConfig = AvoidRebootConfig{
	Files: []*FileFilterEntry{
		// &FileFilterEntry{
		// 	glob: "/etc/kubernetes/kubelet.conf",
		// 	postUpdateAction: RunSystemctlAction{
		// 		unitName:  "kubelet.service",
		// 		operation: unitReload,
		// 	},
		// 	drainRequired: true,
		// },
		&FileFilterEntry{
			glob: "/home/core/testfile",
			postUpdateAction: RunBinaryAction{
				binary: "/bin/bash",
				args: []string{
					"-c",
					"echo \"$(date)\" >> /home/core/testfile.out",
				},
				DrainRequired: DrainRequired{drainRequired: false},
			},
		},
	},
	Units: []*UnitFilterEntry{
		&UnitFilterEntry{
			name:          "testonly.service",
			drainRequired: false,
		},
	},
}

func (config AvoidRebootConfig) getFileAction(filePath string) PostUpdateAction {
	for _, entry := range config.Files {
		matched, err := filepath.Match(entry.glob, filePath)
		if err != nil {
			// TODO: log
			continue
		}
		if matched {
			return entry.postUpdateAction
		}
	}
	return nil
}

func (config AvoidRebootConfig) getUnitAction(unitName string) PostUpdateAction {
	for _, entry := range config.Units {
		if entry.name == unitName {
			return RunSystemctlAction{
				unitName,
				unitRestart,
				DrainRequired{drainRequired: entry.drainRequired},
			}
		}
	}
	return nil
}

type PostUpdateAction interface {
	Run() error
	getIsDrainRequired() bool
	// TODO: add dbus connection setup
	// SetDbusConnection()
}

type DrainRequired struct {
	drainRequired bool
}

func (idr DrainRequired) getIsDrainRequired() bool {
	return idr.drainRequired
}

type RunBinaryAction struct {
	binary string
	args   []string
	// IsDrainRequired bool
	DrainRequired
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
	DrainRequired
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

type ChangeType string

const (
	changeCreated ChangeType = "created"
	changeDeleted ChangeType = "deleted"
	changeUpdated ChangeType = "updated"
)

type FileChange struct {
	name       string
	file       igntypes.File
	changeType ChangeType
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

func getFilesChanges(oldFilesConfig, newFilesConfig []igntypes.File) []*FileChange {
	oldFiles := mapset.NewSetFromSlice(getFileNames(oldFilesConfig))
	oldFilesMap := filesToMap(oldFilesConfig)
	newFiles := mapset.NewSetFromSlice(getFileNames(newFilesConfig))
	newFilesMap := filesToMap(newFilesConfig)
	changes := make([]*FileChange, 0, newFiles.Cardinality())
	for created := range newFiles.Difference(oldFiles).Iter() {
		changes = append(changes, &FileChange{
			name:       created.(string),
			file:       newFilesMap[created.(string)],
			changeType: changeCreated,
		})
	}
	for deleted := range oldFiles.Difference(newFiles).Iter() {
		changes = append(changes, &FileChange{
			name:       deleted.(string),
			file:       oldFilesMap[deleted.(string)],
			changeType: changeDeleted,
		})
	}
	for changeCandidate := range newFiles.Intersect(oldFiles).Iter() {
		newFile := newFilesMap[changeCandidate.(string)]
		// TODO: check against the state on the disk using checkFiles(...) function
		if !reflect.DeepEqual(newFile, oldFilesMap[changeCandidate.(string)]) {
			changes = append(changes, &FileChange{
				name:       changeCandidate.(string),
				file:       newFile,
				changeType: changeUpdated,
			})
		}
	}
	return changes
}

func handleFilesChanges(changes []*FileChange) (err error) {
	for _, change := range changes {
		switch change.changeType {
		case changeCreated:
			fallthrough
		case changeUpdated:
			err = writeFile(change.file)
		case changeDeleted:
			err = deleteFile(change.name)
		default:
			err = fmt.Errorf("Unknown change type %q", change.changeType)
		}
		if err != nil {
			return
		}
	}
	return
}

type UnitChange struct {
	name       string
	oldUnit    *igntypes.Unit
	newUnit    *igntypes.Unit
	changeType ChangeType
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

func getUnitsChanges(oldUnitsConfig, newUnitsConfig []igntypes.Unit) []*UnitChange {
	oldUnits := mapset.NewSetFromSlice(getUnitNames(oldUnitsConfig))
	oldUnitsMap := unitsToMap(oldUnitsConfig)
	newUnits := mapset.NewSetFromSlice(getUnitNames(newUnitsConfig))
	newUnitsMap := unitsToMap(newUnitsConfig)
	changes := make([]*UnitChange, 0, newUnits.Cardinality())
	for created := range newUnits.Difference(oldUnits).Iter() {
		changes = append(changes, &UnitChange{
			name:       created.(string),
			newUnit:    newUnitsMap[created.(string)],
			oldUnit:    nil,
			changeType: changeCreated,
		})
	}
	for deleted := range oldUnits.Difference(newUnits).Iter() {
		changes = append(changes, &UnitChange{
			name:       deleted.(string),
			newUnit:    nil,
			oldUnit:    oldUnitsMap[deleted.(string)],
			changeType: changeDeleted,
		})
	}
	for changeCandidate := range newUnits.Intersect(oldUnits).Iter() {
		newUnit := newUnitsMap[changeCandidate.(string)]
		oldUnit := oldUnitsMap[changeCandidate.(string)]
		// TODO: check against the state on the disk, use checkUnits()
		if !reflect.DeepEqual(newUnit, oldUnit) {
			changes = append(changes, &UnitChange{
				name:       changeCandidate.(string),
				newUnit:    newUnit,
				oldUnit:    oldUnit,
				changeType: changeUpdated,
			})
		}
	}
	return changes
}

func handleUnitsChanges(changes []*UnitChange) (err error) {
	for _, change := range changes {
		switch change.changeType {
		case changeCreated:
			err = createUnit(change.newUnit)
		case changeUpdated:
			err = deleteUnit(change.oldUnit)
			if err != nil {
				// TODO: try to write it back or do it in roll-back?
				return
			}
			err = createUnit(change.newUnit)
		case changeDeleted:
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

func getPostUpdateActions(filesChanges []*FileChange, unitsChanges []*UnitChange) ([]PostUpdateAction, error) {
	glog.Info("Trying to check whether changes in files and units require system reboot.")
	actions := make([]PostUpdateAction, 0, len(filesChanges)+len(unitsChanges))
	rebootRequiredMsg := ", reboot will be required"
	for _, change := range filesChanges {
		switch change.changeType {
		case changeCreated:
			fallthrough
		case changeUpdated:
			action := filterConfig.getFileAction(change.name)
			if action == nil {
				err := fmt.Errorf("No action found for file %q", change.name)
				glog.Infof("%s%s", err, rebootRequiredMsg)
				return nil, err
			}
			actions = append(actions, action)
			glog.Infof("Action found for file %q", change.name)
		default:
			err := fmt.Errorf("File %q was removed", change.name)
			glog.Infof("%s%s", err, rebootRequiredMsg)
			return nil, err
		}
	}

	for _, change := range unitsChanges {
		switch change.changeType {
		case changeCreated:
			fallthrough
		case changeUpdated:
			action := filterConfig.getUnitAction(change.name)
			if action == nil {
				err := fmt.Errorf("No action found for unit %q", change.name)
				glog.Infof("%s%s", err, rebootRequiredMsg)
				return nil, err
			}
			actions = append(actions, action)
			glog.Infof("Action found for unit %q", change.name)
		default:
			err := fmt.Errorf("Unit %q was removed", change.name)
			glog.Infof("%s%s", err, rebootRequiredMsg)
			return nil, err
		}
	}
	return actions, nil
}

func isDrainRequired(actions []PostUpdateAction) bool {
	isRequired := false
	for _, action := range actions {
		isRequired = isRequired || action.getIsDrainRequired()
	}
	return isRequired
}

func runPostUpdateActions(actions []PostUpdateAction) bool {
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
