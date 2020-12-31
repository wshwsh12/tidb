// Copyright 2018 PingCAP, Inc.
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

package memory

import (
	"sync"
	"time"

	"github.com/pingcap/tidb/util/sys/cgroup"
	"github.com/shirou/gopsutil/mem"
)

// MemTotal returns the total amount of RAM on this system
var MemTotal func() (uint64, error)

// MemUsed returns the total used amount of RAM on this system
var MemUsed func() (uint64, error)

// MemTotalNormal returns the total amount of RAM on this system in non-container environment.
func MemTotalNormal() (uint64, error) {
	total, t := memLimit.get()
	if time.Since(t) < 60*time.Second {
		return total, nil
	}
	v, err := mem.VirtualMemory()
	if err != nil {
		return v.Total, err
	}
	memLimit.set(v.Total, time.Now())
	return v.Total, nil
}

// MemUsedNormal returns the total used amount of RAM on this system in non-container environment.
func MemUsedNormal() (uint64, error) {
	used, t := memUsage.get()
	if time.Since(t) < 500*time.Millisecond {
		return used, nil
	}
	v, err := mem.VirtualMemory()
	if err != nil {
		return v.Used, err
	}
	memUsage.set(v.Used, time.Now())
	return v.Used, nil
}

const (
	cGroupMemLimitPath = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
	cGroupMemUsagePath = "/sys/fs/cgroup/memory/memory.usage_in_bytes"
	selfCGroupPath     = "/proc/self/cgroup"
)

type memInfoCache struct {
	*sync.RWMutex
	mem        uint64
	updateTime time.Time
}

func (c *memInfoCache) get() (mem uint64, t time.Time) {
	c.RLock()
	defer c.RUnlock()
	mem, t = c.mem, c.updateTime
	return
}

func (c *memInfoCache) set(mem uint64, t time.Time) {
	c.Lock()
	defer c.Unlock()
	c.mem, c.updateTime = mem, t
}

// expiration time is 60s
var memLimit *memInfoCache

// expiration time is 500ms
var memUsage *memInfoCache

// MemTotalCGroup returns the total amount of RAM on this system in container environment.
func MemTotalCGroup() (uint64, error) {
	mem, t := memLimit.get()
	if time.Since(t) < 60*time.Second {
		return mem, nil
	}
	mem = cgroup.CGroupInstance.GetMemoryLimitInBytes()
	memLimit.set(mem, time.Now())
	return mem, nil
}

// MemUsedCGroup returns the total used amount of RAM on this system in container environment.
func MemUsedCGroup() (uint64, error) {
	mem, t := memUsage.get()
	if time.Since(t) < 500*time.Millisecond {
		return mem, nil
	}
	mem = cgroup.CGroupInstance.GetMemoryUsageInBytes()
	memUsage.set(mem, time.Now())
	return mem, nil
}

func init() {
	if cgroup.InContainer() {
		MemTotal = MemTotalCGroup
		MemUsed = MemUsedCGroup
	} else {
		MemTotal = MemTotalNormal
		MemUsed = MemUsedNormal
	}
	memLimit = &memInfoCache{
		RWMutex: &sync.RWMutex{},
	}
	memUsage = &memInfoCache{
		RWMutex: &sync.RWMutex{},
	}
	_, err := MemTotal()
	if err != nil {
	}
	_, err = MemUsed()
	if err != nil {
	}
}
