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
	"fmt"
	"sync"

	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// ActionOnExceed is the action taken when memory usage exceeds memory quota.
// NOTE: All the implementors should be thread-safe.
type ActionOnExceed interface {
	// Action will be called when memory usage exceeds memory quota by the
	// corresponding Tracker.
	Action(t *Tracker)
	// SetLogHook binds a log hook which will be triggered and log an detailed
	// message for the out-of-memory sql.
	SetLogHook(hook func(uint64))
	// SetFallback sets a FallbackAction action which will be triggered if itself has
	// already been triggered.
	SetFallback(a ActionOnExceed)
	// GetFallback get the FallbackAction action of the Action.
	GetFallback() ActionOnExceed
	// GetPriority get the priority of the Action.
	GetPriority() int64
}

type BaseOOMAction struct {
	M              sync.Mutex
	FallbackAction ActionOnExceed
}

func (b *BaseOOMAction) SetFallback(a ActionOnExceed) {
	b.M.Lock()
	defer b.M.Unlock()
	b.FallbackAction = a
}

func (b *BaseOOMAction) GetFallback() ActionOnExceed {
	b.M.Lock()
	defer b.M.Unlock()
	return b.FallbackAction
}

// Default OOM Action priority.
const (
	DefPanicPriority = iota
	DefLogPriority
	DefSpillPriority
	DefRateLimitPriority
)

// LogOnExceed logs a warning only once when memory usage exceeds memory quota.
type LogOnExceed struct {
	BaseOOMAction
	acted   bool
	ConnID  uint64
	logHook func(uint64)
}

// SetLogHook sets a hook for LogOnExceed.
func (a *LogOnExceed) SetLogHook(hook func(uint64)) {
	a.logHook = hook
}

// Action logs a warning only once when memory usage exceeds memory quota.
func (a *LogOnExceed) Action(t *Tracker) {
	a.M.Lock()
	defer a.M.Unlock()
	if !a.acted {
		a.acted = true
		if a.logHook == nil {
			logutil.BgLogger().Warn("memory exceeds quota",
				zap.Error(errMemExceedThreshold.GenWithStackByArgs(t.label, t.BytesConsumed(), t.bytesLimit, t.String())))
			return
		}
		a.logHook(a.ConnID)
	}
}

// GetPriority get the priority of the Action
func (a *LogOnExceed) GetPriority() int64 {
	return DefLogPriority
}

// PanicOnExceed panics when memory usage exceeds memory quota.
type PanicOnExceed struct {
	BaseOOMAction
	acted   bool
	ConnID  uint64
	logHook func(uint64)
}

// SetLogHook sets a hook for PanicOnExceed.
func (a *PanicOnExceed) SetLogHook(hook func(uint64)) {
	a.logHook = hook
}

// Action panics when memory usage exceeds memory quota.
func (a *PanicOnExceed) Action(t *Tracker) {
	a.M.Lock()
	if a.acted {
		a.M.Unlock()
		return
	}
	a.acted = true
	a.M.Unlock()
	if a.logHook != nil {
		a.logHook(a.ConnID)
	}
	panic(PanicMemoryExceed + fmt.Sprintf("[conn_id=%d]", a.ConnID))
}

// GetPriority get the priority of the Action
func (a *PanicOnExceed) GetPriority() int64 {
	return DefPanicPriority
}

var (
	errMemExceedThreshold = dbterror.ClassUtil.NewStd(errno.ErrMemExceedThreshold)
)

const (
	// PanicMemoryExceed represents the panic message when out of memory quota.
	PanicMemoryExceed string = "Out Of Memory Quota!"
)
