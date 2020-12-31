// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cgroup

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
)

const (
	cGroupPath      = "/proc/self/cgroup"
	cGroupMountInfo = "/proc/self/mountinfo"
	cGroupFsType    = "cgroup"

	memSubSys       = "memory"
	memLimitInBytes = "memory.limit_in_bytes"
	memUsageInBytes = "memory.usage_in_bytes"

	cpuSubSys    = "cpu"
	cpuQuota     = "cpu.cfs_quota_us"
	cpuPeriod    = "cpu.cfs_period_us"
	cpuSetSubSys = "cpuset"
	cpuSetCpus   = "cpuset.cpus"

	mountInfoSep      = " "
	optionsSep        = ","
	optionalFieldsSep = "-"

	cGroupSep = ":"
	subSysSep = ","
)

// SubSysFields
const (
	subSysFieldsId = iota
	subSysFieldsSubSystems
	subSysFieldsName

	subSysFieldsCount
)

type cGroup struct {
	path string
}

func NewCGroup(path string) *cGroup {
	return &cGroup{path: path}
}

func (cg *cGroup) readLine(param string) (string, error) {
	v, err := ioutil.ReadFile(path.Join(cg.path, param))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(v)), nil
}

func (cg *cGroup) readNum(param string) (uint64, error) {
	str, err := cg.readLine(param)
	if err != nil {
		return 0, err
	}
	return parseUint(str, 10, 64)
}

func parseUint(s string, base, bitSize int) (uint64, error) {
	v, err := strconv.ParseUint(s, base, bitSize)
	if err != nil {
		intValue, intErr := strconv.ParseInt(s, base, bitSize)
		// 1. Handle negative values greater than MinInt64 (and)
		// 2. Handle negative values lesser than MinInt64
		if intErr == nil && intValue < 0 {
			return 0, nil
		} else if intErr != nil &&
			intErr.(*strconv.NumError).Err == strconv.ErrRange &&
			intValue < 0 {
			return 0, nil
		}
		return 0, err
	}
	return v, nil
}

type cGroupSubSys struct {
	id         uint64
	subSystems []string
	name       string
}

type cGroupSys struct {
	cGroups map[string]*cGroup
}

func NewCGroupSys(cGroupPath string, mountInfoPath string) *cGroupSys {
	subSystems := make(map[string]*cGroupSubSys)

	f, err := os.Open(cGroupPath)
	if err != nil {

	}
	br := bufio.NewReader(f)
	for true {
		line, _, err := br.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil
		}
		subSyss, err := parseSubSysFromString(string(line))
		if err != nil {
			return nil
		}
		for _, subSys := range subSyss.subSystems {
			subSystems[subSys] = subSyss
		}
	}

	cGroups := make(map[string]*cGroup)
	f, err = os.Open(mountInfoPath)
	br = bufio.NewReader(f)
	for true {
		line, _, err := br.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil
		}
		mp, err := parseMountPointFromString(string(line))
		if err != nil {
			return nil
		}
		if mp.fsType == cGroupFsType {
			for _, op := range mp.superOptions {
				if sub, ok := subSystems[op]; ok {
					if subPath, err := mp.translate(sub.name); err == nil {
						cGroups[op] = NewCGroup(subPath)
					}
				}
			}
		}
	}
	return &cGroupSys{cGroups: cGroups}
}

// MountInfoFieldPart1
const (
	MountInfoFieldPart1MountId = iota
	MountInfoFieldPart1ParentId
	MountInfoFieldPart1DeviceId
	MountInfoFieldPart1Root
	MountInfoFieldPart1MountPoint
	MountInfoFieldPart1Options
	MountInfoFieldPart1OptionalFields

	MountInfoFieldPart1Count
)

// MountInfoFieldPart2
const (
	MountInfoFieldPart2FSType = iota
	MountInfoFieldPart2MountSource
	MountInfoFieldPart2SuperOptions

	MountInfoFieldPart2Count
)

type mountPoint struct {
	mountId        uint64
	parentId       uint64
	deviceId       string
	root           string
	mountPoint     string
	option         []string
	optionalFields []string
	fsType         string
	mountSource    string
	superOptions   []string
}

func (mp *mountPoint) translate(absPath string) (string, error) {
	rel, err := filepath.Rel(mp.root, absPath)
	if err != nil {
		return "", err
	}
	newPath := path.Join(mp.mountPoint, rel)
	return newPath, nil
}

func (cgs *cGroupSys) GetMemoryLimitInBytes() uint64 {
	if cg, ok := cgs.cGroups[memSubSys]; ok {
		if limit, err := cg.readNum(memLimitInBytes); err == nil {
			return limit
		}
	}
	// 0 means no limit
	return 0
}

func (cgs *cGroupSys) GetMemoryUsageInBytes() uint64 {
	if cg, ok := cgs.cGroups[memSubSys]; ok {
		if limit, err := cg.readNum(memUsageInBytes); err == nil {
			return limit
		}
	}
	// 0 means can't get result
	return 0
}

func InContainer() bool {
	v, err := ioutil.ReadFile(cGroupPath)
	if err != nil {
		return false
	}
	if strings.Contains(string(v), "docker") ||
		strings.Contains(string(v), "kubepods") ||
		strings.Contains(string(v), "containerd") {
		return true
	}
	return false
}

func parseSubSysFromString(line string) (*cGroupSubSys, error) {
	cgss := &cGroupSubSys{}
	fields := strings.Split(line, cGroupSep)
	if len(fields) != subSysFieldsCount {
		return nil, errors.New("subsystem format invalid")
	}
	id, err := parseUint(fields[subSysFieldsId], 10, 64)
	if err != nil {
		return nil, err
	}
	cgss.id = id
	cgss.subSystems = strings.Split(fields[subSysFieldsSubSystems], subSysSep)
	cgss.name = fields[subSysFieldsName]
	return cgss, nil
}

func parseMountPointFromString(line string) (*mountPoint, error) {
	fields := strings.Split(line, mountInfoSep)
	if len(fields) < MountInfoFieldPart1Count+MountInfoFieldPart2Count {
		return nil, errors.New("mount point format invalid")
	}

	sepPos := MountInfoFieldPart1OptionalFields
	foundSep := false

	for _, field := range fields[MountInfoFieldPart1OptionalFields:] {
		if field == optionalFieldsSep {
			foundSep = true
			break
		}
		sepPos++
	}
	if !foundSep {
		return nil, errors.New("mount point format invalid, doesn't found optional field separator")
	}
	fsStart := sepPos + 1

	mp := &mountPoint{}
	mountId, err := parseUint(fields[MountInfoFieldPart1MountId], 10, 64)
	if err != nil {
		return nil, err
	}
	parentId, err := parseUint(fields[MountInfoFieldPart1ParentId], 10, 64)
	if err != nil {
		return nil, err
	}
	mp.mountId = mountId
	mp.parentId = parentId
	mp.deviceId = fields[MountInfoFieldPart1DeviceId]
	mp.root = fields[MountInfoFieldPart1Root]
	mp.mountPoint = fields[MountInfoFieldPart1MountPoint]
	mp.option = strings.Split(fields[MountInfoFieldPart1Options], optionsSep)
	mp.optionalFields = fields[MountInfoFieldPart1OptionalFields : fsStart-1]
	mp.fsType = fields[fsStart+MountInfoFieldPart2FSType]
	mp.mountSource = fields[fsStart+MountInfoFieldPart2MountSource]
	mp.superOptions = strings.Split(fields[fsStart+MountInfoFieldPart2SuperOptions], optionsSep)
	return mp, nil
}

var CGroupInstance *cGroupSys

func init() {
	//	if InContainer() {
	CGroupInstance = NewCGroupSys(
		cGroupPath,
		cGroupMountInfo,
	)
	//	}
	fmt.Println(CGroupInstance.GetMemoryLimitInBytes())
}
