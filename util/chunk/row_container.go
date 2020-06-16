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

package chunk

import (
	"errors"
	"sync"

	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/stringutil"
)

// RowContainer provides a place for many rows, so many that we might want to spill them into disk.
type RowContainer struct {
	// records stores the chunks in memory.
	records *List
	// recordsInDisk stores the chunks in disk.
	recordsInDisk *ListInDisk
	// m guarantees spill and get operator for rowContainer is mutually.
	m sync.RWMutex

	fieldType []*types.FieldType
	chunkSize int
	numRow    int

	memTracker  *memory.Tracker
	diskTracker *disk.Tracker
	actionSpill *SpillDiskAction
}

// NewRowContainer creates a new RowContainer in memory.
func NewRowContainer(fieldType []*types.FieldType, chunkSize int) *RowContainer {
	li := NewList(fieldType, chunkSize, chunkSize)
	rc := &RowContainer{records: li, fieldType: fieldType, chunkSize: chunkSize}
	rc.memTracker = li.memTracker
	rc.diskTracker = disk.NewTracker(stringutil.StringerStr("RowContainer"), -1)
	return rc
}

// SpillToDisk spills data to disk.
func (c *RowContainer) SpillToDisk(needLock bool) (err error) {
	if needLock {
		c.m.Lock()
		defer c.m.Unlock()
	}
	N := c.records.NumChunks()
	c.recordsInDisk = NewListInDisk(c.records.FieldTypes())
	c.recordsInDisk.diskTracker.AttachTo(c.diskTracker)
	for i := 0; i < N; i++ {
		chk := c.records.GetChunk(i)
		err = c.recordsInDisk.Add(chk)
		if err != nil {
			return
		}
	}
	c.records.Clear()
	return
}

// Reset resets RowContainer.
func (c *RowContainer) Reset() error {
	if c.AlreadySpilled() {
		err := c.recordsInDisk.Close()
		c.recordsInDisk = nil
		if err != nil {
			return err
		}
	} else {
		c.records.Reset()
	}
	return nil
}

// AlreadySpilled indicates that records have spilled out into disk.
func (c *RowContainer) AlreadySpilled() bool {
	return c.recordsInDisk != nil
}

// AlreadySpilledSafe indicates that records have spilled out into disk. It's thread-safe.
func (c *RowContainer) AlreadySpilledSafe() bool {
	c.m.RLock()
	defer c.m.RUnlock()
	return c.recordsInDisk != nil
}

// NumRow returns the number of rows in the container
func (c *RowContainer) NumRow() int {
	c.m.RLock()
	defer c.m.RUnlock()
	if c.AlreadySpilled() {
		return c.recordsInDisk.Len()
	}
	return c.records.Len()
}

// NumRowsOfChunk returns the number of rows of a chunk in the ListInDisk.
func (c *RowContainer) NumRowsOfChunk(chkID int) int {
	c.m.RLock()
	defer c.m.RUnlock()
	if c.AlreadySpilled() {
		return c.recordsInDisk.NumRowsOfChunk(chkID)
	}
	return c.records.NumRowsOfChunk(chkID)
}

// NumChunks returns the number of chunks in the container.
func (c *RowContainer) NumChunks() int {
	c.m.RLock()
	defer c.m.RUnlock()
	if c.AlreadySpilled() {
		return c.recordsInDisk.NumChunks()
	}
	return c.records.NumChunks()
}

// Add appends a chunk into the RowContainer.
func (c *RowContainer) Add(chk *Chunk) (err error) {
	c.m.RLock()
	defer c.m.RUnlock()
	if c.AlreadySpilled() {
		err = c.recordsInDisk.Add(chk)
	} else {
		c.records.Add(chk)
	}
	return
}

// AppendRow appends a row to the RowContainer, the row is copied to the RowContainer.
func (c *RowContainer) AppendRow(row Row) (RowPtr, error) {
	if c.AlreadySpilled() {
		return RowPtr{}, errors.New("ListInDisk don't support AppendRow")
	}
	return c.records.AppendRow(row), nil
}

// AllocChunk allocates a new chunk from RowContainer.
func (c *RowContainer) AllocChunk() (chk *Chunk) {
	return c.records.allocChunk()
}

// GetChunk returns chkIdx th chunk of in memory records.
func (c *RowContainer) GetChunk(chkIdx int) *Chunk {
	return c.records.GetChunk(chkIdx)
}

// GetList returns the list of in memory records.
func (c *RowContainer) GetList() *List {
	return c.records
}

// GetRow returns the row the ptr pointed to.
func (c *RowContainer) GetRow(ptr RowPtr) (Row, error) {
	c.m.RLock()
	defer c.m.RUnlock()
	if c.AlreadySpilled() {
		return c.recordsInDisk.GetRow(ptr)
	}
	return c.records.GetRow(ptr), nil
}

// GetMemTracker returns the memory tracker in records, panics if the RowContainer has already spilled.
func (c *RowContainer) GetMemTracker() *memory.Tracker {
	return c.memTracker
}

// GetDiskTracker returns the underlying disk usage tracker in recordsInDisk.
func (c *RowContainer) GetDiskTracker() *disk.Tracker {
	return c.diskTracker
}

// Close close the RowContainer
func (c *RowContainer) Close() (err error) {
	if c.AlreadySpilled() {
		err = c.recordsInDisk.Close()
		c.recordsInDisk = nil
	}
	c.records.Clear()
	return
}

// ActionSpill returns a SpillDiskAction for spilling over to disk.
func (c *RowContainer) ActionSpill() *SpillDiskAction {
	c.actionSpill = &SpillDiskAction{c: c}
	return c.actionSpill
}

// SpillDiskAction implements memory.ActionOnExceed for chunk.List. If
// the memory quota of a query is exceeded, SpillDiskAction.Action is
// triggered.
type SpillDiskAction struct {
	c              *RowContainer
	fallbackAction memory.ActionOnExceed
	m              sync.Mutex
}

// Action sends a signal to trigger spillToDisk method of RowContainer
// and if it is already triggered before, call its fallbackAction.
func (a *SpillDiskAction) Action(t *memory.Tracker, trigger *memory.Tracker) {
	a.m.Lock()
	defer a.m.Unlock()
	if a.c.AlreadySpilledSafe() || a.c.GetMemTracker().BytesConsumed() == 0 {
		if a.fallbackAction != nil {
			a.fallbackAction.Action(t, trigger)
		}
	} else {
		// TODO: Refine processing for various errors. Return or Panic.
		err := a.c.SpillToDisk(a.c.GetMemTracker() != trigger)
		if err != nil {
			panic(err)
		}
	}
}

// SetFallback sets the fallback action.
func (a *SpillDiskAction) SetFallback(fallback memory.ActionOnExceed) {
	a.fallbackAction = fallback
}

// SetLogHook sets the hook, it does nothing just to form the memory.ActionOnExceed interface.
func (a *SpillDiskAction) SetLogHook(hook func(uint64)) {}

// ResetRowContainer resets the spill action and sets the RowContainer for the SpillDiskAction.
func (a *SpillDiskAction) ResetRowContainer(c *RowContainer) {
	a.m.Lock()
	defer a.m.Unlock()
	a.c = c
}
