// Copyright 2017 PingCAP, Inc.
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

package executor

import (
	"container/heap"
	"context"
	"fmt"
	"sort"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/stringutil"
)

var rowChunksLabel fmt.Stringer = stringutil.StringerStr("rowChunks")

// SortExec represents sorting executor.
type SortExec struct {
	baseExecutor

	ByItems []*util.ByItems
	Idx     int
	fetched bool
	schema  *expression.Schema

	keyExprs []expression.Expression
	keyTypes []*types.FieldType
	// keyColumns is the column index of the by items.
	keyColumns []int
	// keyCmpFuncs is used to compare each ByItem.
	keyCmpFuncs []chunk.CompareFunc
	// rowChunks is the chunks to store row values.
	rowChunks *chunk.SortedRowContainer

	memTracker  *memory.Tracker
	diskTracker *disk.Tracker

	// partitionList is the chunks to store row values for partitions.
	partitionList []*chunk.SortedRowContainer

	// multiWayMerge uses multi-way merge for spill disk.
	// The multi-way merge algorithm can refer to https://en.wikipedia.org/wiki/K-way_merge_algorithm
	multiWayMerge *multiWayMerge
	// spillAction save the Action for spill disk.
	spillAction *chunk.SortAndSpillDiskAction
}

// Close implements the Executor Close interface.
func (e *SortExec) Close() error {
	for _, container := range e.partitionList {
		err := container.Close()
		if err != nil {
			return err
		}
	}
	e.partitionList = e.partitionList[:0]

	if e.rowChunks != nil {
		e.memTracker.Consume(-e.rowChunks.GetMemTracker().BytesConsumed())
		e.rowChunks = nil
	}
	e.memTracker = nil
	e.diskTracker = nil
	e.multiWayMerge = nil
	e.spillAction = nil
	return e.children[0].Close()
}

// Open implements the Executor Open interface.
func (e *SortExec) Open(ctx context.Context) error {
	e.fetched = false
	e.Idx = 0

	// To avoid duplicated initialization for TopNExec.
	if e.memTracker == nil {
		e.memTracker = memory.NewTracker(e.id, -1)
		e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
		e.diskTracker = memory.NewTracker(e.id, -1)
		e.diskTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.DiskTracker)
	}
	e.partitionList = e.partitionList[:0]
	return e.children[0].Open(ctx)
}

// Next implements the Executor Next interface.
// Sort constructs the result following these step:
// 1. Read as mush as rows into memory.
// 2. If memory quota is triggered, sort these rows in memory and put them into disk as partition 1, then reset
//    the memory quota trigger and return to step 1
// 3. If memory quota is not triggered and child is consumed, sort these rows in memory as partition N.
// 4. Merge sort if the count of partitions is larger than 1. If there is only one partition in step 4, it works
//    just like in-memory sort before.
func (e *SortExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if !e.fetched {
		e.initCompareFuncs()
		e.buildKeyColumns()
		err := e.fetchRowChunks(ctx)
		if err != nil {
			return err
		}
		e.fetched = true
	}

	if len(e.partitionList) == 0 {
		return nil
	}
	if len(e.partitionList) > 1 {
		if err := e.externalSorting(req); err != nil {
			return err
		}
	} else {
		for !req.IsFull() && e.Idx < e.partitionList[0].NumRow() {
			row, err := e.partitionList[0].GetRowByIdx(e.Idx)
			if err != nil {
				return err
			}
			req.AppendRow(row)
			e.Idx++
		}
	}
	return nil
}

type partitionPointer struct {
	row         chunk.Row
	partitionID int
	consumed    int
}

type multiWayMerge struct {
	lessRowFunction func(rowI chunk.Row, rowJ chunk.Row) bool
	elements        []partitionPointer
}

func (h *multiWayMerge) Less(i, j int) bool {
	rowI := h.elements[i].row
	rowJ := h.elements[j].row
	return h.lessRowFunction(rowI, rowJ)
}

func (h *multiWayMerge) Len() int {
	return len(h.elements)
}

func (h *multiWayMerge) Push(x interface{}) {
	// Should never be called.
}

func (h *multiWayMerge) Pop() interface{} {
	h.elements = h.elements[:len(h.elements)-1]
	return nil
}

func (h *multiWayMerge) Swap(i, j int) {
	h.elements[i], h.elements[j] = h.elements[j], h.elements[i]
}

func (e *SortExec) externalSorting(req *chunk.Chunk) (err error) {
	if e.multiWayMerge == nil {
		e.multiWayMerge = &multiWayMerge{e.lessRow, make([]partitionPointer, 0, len(e.partitionList))}
		for i := 0; i < len(e.partitionList); i++ {
			row, err := e.partitionList[i].GetRowByIdx(0)
			if err != nil {
				return err
			}
			e.multiWayMerge.elements = append(e.multiWayMerge.elements, partitionPointer{row: row, partitionID: i, consumed: 0})
		}
		heap.Init(e.multiWayMerge)
	}

	for !req.IsFull() && e.multiWayMerge.Len() > 0 {
		partitionPtr := e.multiWayMerge.elements[0]
		req.AppendRow(partitionPtr.row)
		partitionPtr.consumed++
		if partitionPtr.consumed >= e.partitionList[partitionPtr.partitionID].NumRow() {
			heap.Remove(e.multiWayMerge, 0)
			continue
		}
		partitionPtr.row, err = e.partitionList[partitionPtr.partitionID].
			GetRowByIdx(partitionPtr.consumed)
		if err != nil {
			return err
		}
		e.multiWayMerge.elements[0] = partitionPtr
		heap.Fix(e.multiWayMerge, 0)
	}
	return nil
}

func (e *SortExec) fetchRowChunks(ctx context.Context) error {
	fields := retTypes(e)
	byItemsDesc := make([]bool, len(e.ByItems))
	for i, byItem := range e.ByItems {
		byItemsDesc[i] = byItem.Desc
	}
	e.rowChunks = chunk.NewSortedRowContainer(fields, e.maxChunkSize, byItemsDesc, e.keyColumns, e.keyCmpFuncs)
	e.rowChunks.GetMemTracker().AttachTo(e.memTracker)
	e.rowChunks.GetMemTracker().SetLabel(rowChunksLabel)
	if config.GetGlobalConfig().OOMUseTmpStorage {
		e.spillAction = e.rowChunks.ActionSpill()
		e.ctx.GetSessionVars().StmtCtx.MemTracker.FallbackOldAndSetNewAction(e.spillAction)
		e.rowChunks.GetDiskTracker().AttachTo(e.diskTracker)
		e.rowChunks.GetDiskTracker().SetLabel(rowChunksLabel)
	}
	for {
		chk := newFirstChunk(e.children[0])
		err := Next(ctx, e.children[0], chk)
		if err != nil {
			return err
		}
		rowCount := chk.NumRows()
		if rowCount == 0 {
			break
		}
		if err := e.rowChunks.Add(chk); err != nil {
			if chunk.ErrInsertToPartitionFailed.Equal(err) {
				e.rowChunks.GetMemTracker().Consume(int64(8 * cap(e.rowChunks.M.RowPtrs)))
				e.partitionList = append(e.partitionList, e.rowChunks)
				e.rowChunks = chunk.NewSortedRowContainer(fields, e.maxChunkSize, byItemsDesc, e.keyColumns, e.keyCmpFuncs)
				e.rowChunks.GetMemTracker().AttachTo(e.memTracker)
				e.rowChunks.GetMemTracker().SetLabel(rowChunksLabel)
				e.rowChunks.GetDiskTracker().AttachTo(e.diskTracker)
				e.rowChunks.GetDiskTracker().SetLabel(rowChunksLabel)
				e.spillAction.ResetRowContainer(e.rowChunks)
				err = e.rowChunks.Add(chk)
			}
			if err != nil {
				return err
			}
		}
	}
	if e.rowChunks.NumRow() > 0 {
		e.rowChunks.InitPointersAndSort()
		e.rowChunks.GetMemTracker().Consume(int64(8 * cap(e.rowChunks.M.RowPtrs)))
		e.partitionList = append(e.partitionList, e.rowChunks)
	}
	return nil
}

func (e *SortExec) initCompareFuncs() {
	e.keyCmpFuncs = make([]chunk.CompareFunc, len(e.ByItems))
	for i := range e.ByItems {
		keyType := e.ByItems[i].Expr.GetType()
		e.keyCmpFuncs[i] = chunk.GetCompareFunc(keyType)
	}
}

func (e *SortExec) buildKeyColumns() {
	e.keyColumns = make([]int, 0, len(e.ByItems))
	for _, by := range e.ByItems {
		col := by.Expr.(*expression.Column)
		e.keyColumns = append(e.keyColumns, col.Index)
	}
}

func (e *SortExec) lessRow(rowI, rowJ chunk.Row) bool {
	for i, colIdx := range e.keyColumns {
		cmpFunc := e.keyCmpFuncs[i]
		cmp := cmpFunc(rowI, colIdx, rowJ, colIdx)
		if e.ByItems[i].Desc {
			cmp = -cmp
		}
		if cmp < 0 {
			return true
		} else if cmp > 0 {
			return false
		}
	}
	return false
}

// keyColumnsLess is the less function for key columns.
func (e *SortExec) keyColumnsLess(i, j int) bool {
	rowI := e.rowChunks.GetList().GetRow(e.rowChunks.M.RowPtrs[i])
	rowJ := e.rowChunks.GetList().GetRow(e.rowChunks.M.RowPtrs[j])
	return e.lessRow(rowI, rowJ)
}

// TopNExec implements a Top-N algorithm and it is built from a SELECT statement with ORDER BY and LIMIT.
// Instead of sorting all the rows fetched from the table, it keeps the Top-N elements only in a heap to reduce memory usage.
type TopNExec struct {
	SortExec
	limit      *plannercore.PhysicalLimit
	totalLimit uint64

	chkHeap *topNChunkHeap
}

// topNChunkHeap implements heap.Interface.
type topNChunkHeap struct {
	*TopNExec
}

// Less implement heap.Interface, but since we mantains a max heap,
// this function returns true if row i is greater than row j.
func (h *topNChunkHeap) Less(i, j int) bool {
	rowI := h.rowChunks.GetList().GetRow(h.rowChunks.M.RowPtrs[i])
	rowJ := h.rowChunks.GetList().GetRow(h.rowChunks.M.RowPtrs[j])
	return h.greaterRow(rowI, rowJ)
}

func (h *topNChunkHeap) greaterRow(rowI, rowJ chunk.Row) bool {
	for i, colIdx := range h.keyColumns {
		cmpFunc := h.keyCmpFuncs[i]
		cmp := cmpFunc(rowI, colIdx, rowJ, colIdx)
		if h.ByItems[i].Desc {
			cmp = -cmp
		}
		if cmp > 0 {
			return true
		} else if cmp < 0 {
			return false
		}
	}
	return false
}

func (h *topNChunkHeap) Len() int {
	return len(h.rowChunks.M.RowPtrs)
}

func (h *topNChunkHeap) Push(x interface{}) {
	// Should never be called.
}

func (h *topNChunkHeap) Pop() interface{} {
	h.rowChunks.M.RowPtrs = h.rowChunks.M.RowPtrs[:len(h.rowChunks.M.RowPtrs)-1]
	// We don't need the popped value, return nil to avoid memory allocation.
	return nil
}

func (h *topNChunkHeap) Swap(i, j int) {
	h.rowChunks.M.RowPtrs[i], h.rowChunks.M.RowPtrs[j] = h.rowChunks.M.RowPtrs[j], h.rowChunks.M.RowPtrs[i]
}

// Open implements the Executor Open interface.
func (e *TopNExec) Open(ctx context.Context) error {
	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
	return e.SortExec.Open(ctx)
}

// Next implements the Executor Next interface.
func (e *TopNExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if !e.fetched {
		e.totalLimit = e.limit.Offset + e.limit.Count
		e.Idx = int(e.limit.Offset)
		err := e.loadChunksUntilTotalLimit(ctx)
		if err != nil {
			return err
		}
		err = e.executeTopN(ctx)
		if err != nil {
			return err
		}
		e.fetched = true
	}
	if e.Idx >= len(e.rowChunks.M.RowPtrs) {
		return nil
	}
	for !req.IsFull() && e.Idx < len(e.rowChunks.M.RowPtrs) {
		row, err := e.rowChunks.GetRow(e.rowChunks.M.RowPtrs[e.Idx])
		if err != nil {
			return err
		}
		req.AppendRow(row)
		e.Idx++
	}
	return nil
}

func (e *TopNExec) loadChunksUntilTotalLimit(ctx context.Context) error {
	e.chkHeap = &topNChunkHeap{e}
	e.rowChunks = chunk.NewSortedRowContainer(retTypes(e), e.maxChunkSize, nil, nil, nil)
	e.rowChunks.GetMemTracker().AttachTo(e.memTracker)
	e.rowChunks.GetMemTracker().SetLabel(rowChunksLabel)
	for uint64(e.rowChunks.NumRow()) < e.totalLimit {
		srcChk := newFirstChunk(e.children[0])
		// adjust required rows by total limit
		srcChk.SetRequiredRows(int(e.totalLimit-uint64(e.rowChunks.NumRow())), e.maxChunkSize)
		err := Next(ctx, e.children[0], srcChk)
		if err != nil {
			return err
		}
		if srcChk.NumRows() == 0 {
			break
		}
		if err := e.rowChunks.Add(srcChk); err != nil {
			return err
		}
	}
	e.rowChunks.InitPointers()
	e.initCompareFuncs()
	e.buildKeyColumns()
	return nil
}

const topNCompactionFactor = 4

func (e *TopNExec) executeTopN(ctx context.Context) error {
	heap.Init(e.chkHeap)
	for uint64(len(e.rowChunks.M.RowPtrs)) > e.totalLimit {
		// The number of rows we loaded may exceeds total limit, remove greatest rows by Pop.
		heap.Pop(e.chkHeap)
	}
	childRowChk := newFirstChunk(e.children[0])
	for {
		err := Next(ctx, e.children[0], childRowChk)
		if err != nil {
			return err
		}
		if childRowChk.NumRows() == 0 {
			break
		}
		err = e.processChildChk(childRowChk)
		if err != nil {
			return err
		}
		if e.rowChunks.NumRow() > len(e.rowChunks.M.RowPtrs)*topNCompactionFactor {
			err = e.doCompaction()
			if err != nil {
				return err
			}
		}
	}
	sort.Slice(e.rowChunks.M.RowPtrs, e.keyColumnsLess)
	return nil
}

func (e *TopNExec) processChildChk(childRowChk *chunk.Chunk) error {
	for i := 0; i < childRowChk.NumRows(); i++ {
		heapMaxPtr := e.rowChunks.M.RowPtrs[0]
		var heapMax, next chunk.Row
		heapMax, err := e.rowChunks.GetRow(heapMaxPtr)
		if err != nil {
			return err
		}
		next = childRowChk.GetRow(i)
		if e.chkHeap.greaterRow(heapMax, next) {
			// Evict heap max, keep the next row.
			e.rowChunks.M.RowPtrs[0], err = e.rowChunks.AppendRow(childRowChk.GetRow(i))
			if err != nil {
				return err
			}
			heap.Fix(e.chkHeap, 0)
		}
	}
	return nil
}

// doCompaction rebuild the chunks and row pointers to release memory.
// If we don't do compaction, in a extreme case like the child data is already ascending sorted
// but we want descending top N, then we will keep all data in memory.
// But if data is distributed randomly, this function will be called log(n) times.
func (e *TopNExec) doCompaction() error {
	newRowChunks := chunk.NewSortedRowContainer(retTypes(e), e.maxChunkSize, nil, nil, nil)
	newRowPtrs := make([]chunk.RowPtr, 0, e.rowChunks.NumRow())
	for _, rowPtr := range e.rowChunks.M.RowPtrs {
		row, err := e.rowChunks.GetRow(rowPtr)
		if err != nil {
			return err
		}
		newRowPtr, err := newRowChunks.AppendRow(row)
		if err != nil {
			return err
		}
		newRowPtrs = append(newRowPtrs, newRowPtr)
	}
	newRowChunks.GetMemTracker().SetLabel(rowChunksLabel)
	e.memTracker.ReplaceChild(e.rowChunks.GetMemTracker(), newRowChunks.GetMemTracker())
	e.rowChunks = newRowChunks

	e.memTracker.Consume(int64(-8 * len(e.rowChunks.M.RowPtrs)))
	e.memTracker.Consume(int64(8 * len(newRowPtrs)))
	e.rowChunks.M.RowPtrs = newRowPtrs
	return nil
}
