// Copyright 2019 PingCAP, Inc.
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

package expensivequery

import (
	"fmt"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/util/memory"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	rpprof "runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Handle is the handler for expensive query.
type Handle struct {
	exitCh chan struct{}
	sm     atomic.Value
}

// NewExpensiveQueryHandle builds a new expensive query handler.
func NewExpensiveQueryHandle(exitCh chan struct{}) *Handle {
	return &Handle{exitCh: exitCh}
}

// SetSessionManager sets the SessionManager which is used to fetching the info
// of all active sessions.
func (eqh *Handle) SetSessionManager(sm util.SessionManager) *Handle {
	eqh.sm.Store(sm)
	return eqh
}

// Run starts a expensive query checker goroutine at the start time of the server.
func (eqh *Handle) Run() {
	threshold := atomic.LoadUint64(&variable.ExpensiveQueryTimeThreshold)
	var systemMemThreshold = config.GetGlobalConfig().AlertMemoryQuotaInstance
	var err error
	if systemMemThreshold == 0 {
		systemMemThreshold, err = memory.MemTotal()
		systemMemThreshold = systemMemThreshold / 10 * 8
		if err != nil {
			logutil.BgLogger().Warn("Get system memory fail.", zap.Error(err))
		}
	}
	lastOOMtime := time.Time{}
	// use 100ms as tickInterval temply, may use given interval or use defined variable later
	tickInterval := time.Millisecond * time.Duration(100)
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	sm := eqh.sm.Load().(util.SessionManager)
	for {
		select {
		case <-ticker.C:
			processInfo := sm.ShowProcessList()
			for _, info := range processInfo {
				if len(info.Info) == 0 {
					continue
				}
				costTime := time.Since(info.Time)
				if !info.ExceedExpensiveTimeThresh && costTime >= time.Second*time.Duration(threshold) && log.GetLevel() <= zapcore.WarnLevel {
					logExpensiveQuery(costTime, info)
					info.ExceedExpensiveTimeThresh = true
				}

				if info.MaxExecutionTime > 0 && costTime > time.Duration(info.MaxExecutionTime)*time.Millisecond {
					sm.Kill(info.ID, true)
				}
			}
			threshold = atomic.LoadUint64(&variable.ExpensiveQueryTimeThreshold)

			instanceStats := &runtime.MemStats{}
			runtime.ReadMemStats(instanceStats)
			instanceMem := instanceStats.HeapAlloc
			if err == nil && instanceMem > systemMemThreshold {
				// At least ten seconds between two recordings that memory usage is less than threshold (80% system memory).
				// If the memory is still exceeded, only records once.
				if time.Since(lastOOMtime) > 10*time.Second {
					eqh.oomRecord(instanceMem, systemMemThreshold)
				}
				lastOOMtime = time.Now()
			}
		case <-eqh.exitCh:
			return
		}
	}
}

var (
	tmpDir              string
	lastLogFileName     string
	lastProfileFileName string
)

func (eqh *Handle) oomRecord(memUsage uint64, systemMemThreshold uint64) {
	var err error
	if tmpDir == "" {
		tmpDir, err = ioutil.TempDir("", "TiDBOOM")
		if err != nil {
			return
		}
	}
	tryRemove := func(filename string) {
		if filename == "" {
			return
		}
		err = os.Remove(filename)
	}
	tryRemove(lastLogFileName)
	tryRemove(lastProfileFileName)

	logutil.BgLogger().Warn("The TiDB instance now takes a lot of memory, has the risk of OOM",
		zap.Any("memUsage", memUsage),
		zap.Any("systemMemThreshold", systemMemThreshold),
	)
	eqh.oomRecordSQL()
	eqh.oomRecordProfile()
}

func (eqh *Handle) oomRecordSQL() {
	sm := eqh.sm.Load().(util.SessionManager)
	processInfo := sm.ShowProcessList()
	pinfo := make([]*util.ProcessInfo, 0, len(processInfo))
	for _, info := range processInfo {
		if len(info.Info) != 0 {
			pinfo = append(pinfo, info)
		}
	}
	now := time.Now()

	lastLogFileName = filepath.Join(tmpDir, "oom_sql"+time.Now().Format(time.RFC3339))
	f, err := os.Create(lastLogFileName)
	if err != nil {
		logutil.BgLogger().Warn("Create oom record file fail.", zap.Error(err))
		return
	}
	defer func() {
		err := f.Close()
		if err != nil {
			logutil.BgLogger().Warn("Close oom record file fail.", zap.Error(err))
		}
	}()
	printTop10 := func(cmp func(i, j int) bool) {
		sort.Slice(pinfo, cmp)
		list := pinfo
		if len(list) > 10 {
			list = list[:10]
		}
		var buf strings.Builder
		for i, info := range list {
			buf.WriteString(fmt.Sprintf("SQL %v: \n", i))
			fields := genLogFields(now.Sub(info.Time), info)
			for _, field := range fields {
				switch field.Type {
				case zapcore.StringType:
					buf.WriteString(fmt.Sprintf("%v: %v", field.Key, field.String))
				case zapcore.Uint8Type, zapcore.Uint16Type, zapcore.Uint32Type, zapcore.Uint64Type:
					buf.WriteString(fmt.Sprintf("%v: %v", field.Key, uint64(field.Integer)))
				case zapcore.Int8Type, zapcore.Int16Type, zapcore.Int32Type, zapcore.Int64Type:
					buf.WriteString(fmt.Sprintf("%v: %v", field.Key, field.Integer))
				}
				buf.WriteString("\n")
			}
		}
		_, err = f.WriteString(buf.String())
	}

	_, err = f.WriteString("Top 10 memory usage of SQL for OOM analyze\n")
	printTop10(func(i, j int) bool {
		return pinfo[i].StmtCtx.MemTracker.MaxConsumed() > pinfo[j].StmtCtx.MemTracker.MaxConsumed()
	})

	_, err = f.WriteString("Top 10 time usage of SQL for OOM analyze\n")
	printTop10(func(i, j int) bool {
		return pinfo[i].Time.Before(pinfo[j].Time)
	})

	logutil.BgLogger().Warn("Get oom sql successfully.", zap.Any("SQLs file path:", lastLogFileName))
}

func (eqh *Handle) oomRecordProfile() {
	lastProfileFileName = filepath.Join(tmpDir, "heap.profile"+time.Now().Format(time.RFC3339))
	f, err := os.Create(lastProfileFileName)
	if err != nil {
		logutil.BgLogger().Warn("Create heap profile file fail.", zap.Error(err))
		return
	}
	defer func() {
		err := f.Close()
		if err != nil {
			logutil.BgLogger().Warn("Close heap profile file fail.", zap.Error(err))
		}
	}()
	p := rpprof.Lookup("heap")
	err = p.WriteTo(f, 0)
	if err != nil {
		logutil.BgLogger().Warn("Write heap profile file fail.", zap.Error(err))
		return
	}
	logutil.BgLogger().Warn("Get heap profile successfully.", zap.Any("Profile file path:", lastProfileFileName))
}

// LogOnQueryExceedMemQuota prints a log when memory usage of connID is out of memory quota.
func (eqh *Handle) LogOnQueryExceedMemQuota(connID uint64) {
	if log.GetLevel() > zapcore.WarnLevel {
		return
	}
	// The out-of-memory SQL may be the internal SQL which is executed during
	// the bootstrap phase, and the `sm` is not set at this phase. This is
	// unlikely to happen except for testing. Thus we do not need to log
	// detailed message for it.
	v := eqh.sm.Load()
	if v == nil {
		logutil.BgLogger().Info("expensive_query during bootstrap phase", zap.Uint64("conn_id", connID))
		return
	}
	sm := v.(util.SessionManager)
	info, ok := sm.GetProcessInfo(connID)
	if !ok {
		return
	}
	logExpensiveQuery(time.Since(info.Time), info)
}

func genLogFields(costTime time.Duration, info *util.ProcessInfo) []zap.Field {
	logFields := make([]zap.Field, 0, 20)
	logFields = append(logFields, zap.String("cost_time", strconv.FormatFloat(costTime.Seconds(), 'f', -1, 64)+"s"))
	execDetail := info.StmtCtx.GetExecDetails()
	logFields = append(logFields, execDetail.ToZapFields()...)
	if copTaskInfo := info.StmtCtx.CopTasksDetails(); copTaskInfo != nil {
		logFields = append(logFields, copTaskInfo.ToZapFields()...)
	}
	if statsInfo := info.StatsInfo(info.Plan); len(statsInfo) > 0 {
		var buf strings.Builder
		firstComma := false
		vStr := ""
		for k, v := range statsInfo {
			if v == 0 {
				vStr = "pseudo"
			} else {
				vStr = strconv.FormatUint(v, 10)
			}
			if firstComma {
				buf.WriteString("," + k + ":" + vStr)
			} else {
				buf.WriteString(k + ":" + vStr)
				firstComma = true
			}
		}
		logFields = append(logFields, zap.String("stats", buf.String()))
	}
	if info.ID != 0 {
		logFields = append(logFields, zap.Uint64("conn_id", info.ID))
	}
	if len(info.User) > 0 {
		logFields = append(logFields, zap.String("user", info.User))
	}
	if len(info.DB) > 0 {
		logFields = append(logFields, zap.String("database", info.DB))
	}
	var tableIDs, indexNames string
	if len(info.StmtCtx.TableIDs) > 0 {
		tableIDs = strings.Replace(fmt.Sprintf("%v", info.StmtCtx.TableIDs), " ", ",", -1)
		logFields = append(logFields, zap.String("table_ids", tableIDs))
	}
	if len(info.StmtCtx.IndexNames) > 0 {
		indexNames = strings.Replace(fmt.Sprintf("%v", info.StmtCtx.IndexNames), " ", ",", -1)
		logFields = append(logFields, zap.String("index_names", indexNames))
	}
	logFields = append(logFields, zap.Uint64("txn_start_ts", info.CurTxnStartTS))
	if memTracker := info.StmtCtx.MemTracker; memTracker != nil {
		logFields = append(logFields, zap.String("mem_max", fmt.Sprintf("%d Bytes (%v)", memTracker.MaxConsumed(), memTracker.BytesToString(memTracker.MaxConsumed()))))
	}

	const logSQLLen = 1024 * 8
	var sql string
	if len(info.Info) > 0 {
		sql = info.Info
	}
	if len(sql) > logSQLLen {
		sql = fmt.Sprintf("%s len(%d)", sql[:logSQLLen], len(sql))
	}
	logFields = append(logFields, zap.String("sql", sql))
	return logFields
}

// logExpensiveQuery logs the queries which exceed the time threshold or memory threshold.
func logExpensiveQuery(costTime time.Duration, info *util.ProcessInfo) {
	logutil.BgLogger().Warn("expensive_query", genLogFields(costTime, info)...)
}
