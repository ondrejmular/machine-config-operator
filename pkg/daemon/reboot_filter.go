package daemon

import (
	"fmt"
	"path/filepath"

	igntypes "github.com/coreos/ignition/config/v2_2/types"
)

type FilterAction string

type ActionType string

const (
	NoneAction      FilterAction = "none"
	SystemctlAction FilterAction = "systemctl"
	BinaryAction    FilterAction = "binary"
	RebootAction    FilterAction = "reboot"
)

type ActionFilterEntry struct {
	File   string
	Path   string
	Action FilterAction
	Drain  bool
	Args   []string
}

func (afe *ActionFilterEntry) getPostAction(filePath string) (PostUpdateAction, error) {
	if afe.Action == BinaryAction {
		if len(afe.Args) == 0 {
			return nil, fmt.Errorf("Empty argument list")
		}
		return RunBinaryAction{
			binary:     afe.Args[0],
			args:       afe.Args[1:],
			expectedRc: 0,
		}, nil
	}
	if afe.Action == SystemctlAction {
		// TODO: properly hanlde all possible cases
		serviceName := filepath.Base(filePath)
		serviceAction := "_reload_"
		if len(afe.Args) == 1 {
			serviceAction = afe.Args[0]
		} else if len(afe.Args) == 2 {
			serviceAction = afe.Args[0]
			serviceName = afe.Args[1]
		} else if len(afe.Args) != 0 {
			// TODO: proper error
			return nil, fmt.Errorf("")
		}
		return RunSystemctlAction{
			serviceName,
			serviceAction,
		}, nil
	}
	// Noop action
	return nil, nil
}

type FileFilterConfig struct {
	entries []*ActionFilterEntry
}

func (ffc *FileFilterConfig) GetAction(file igntypes.File) (needReboot bool, action PostUpdateAction, err error) {
	needReboot = true
	action = nil
	err = nil
	var matched bool
	for _, entry := range ffc.entries {
		matched, err = filepath.Match(filepath.Join(entry.Path, entry.File), file.Path)
		if err != nil {
			return
		}
		if matched {
			if entry.Action == RebootAction {
				return
			}
			action, err = entry.getPostAction(file.Path)
			if err == nil {
				needReboot = false
			}
			return
		}
	}
	return true, nil, nil
}

var FilterConfig FileFilterConfig = FileFilterConfig{
	entries: []*ActionFilterEntry{
		&ActionFilterEntry{
			File:   "*.service",
			Path:   "/etc/systemd/system/",
			Action: SystemctlAction,
			Drain:  false,
			Args:   []string{"_restart_"},
		},
		&ActionFilterEntry{
			File:   "*",
			Path:   "/var/home/core/.ssh/",
			Action: NoneAction,
			Drain:  false,
			Args:   []string{},
		},
	},
}

type PostUpdateAction interface {
	Run() error
}

type RunBinaryAction struct {
	binary     string
	args       []string
	expectedRc int
}

func (rba RunBinaryAction) Run() error {
	// TODO: implement
	return nil
}

type RunSystemctlAction struct {
	serviceName   string
	serviceAction string
}

func (rsa RunSystemctlAction) Run() error {
	// TODO: implement
	// https://godoc.org/github.com/coreos/go-systemd/dbus
	return nil
}
