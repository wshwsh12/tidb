// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"context"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tipb/go-tipb"
)

type ftsFuncValidation struct {
}

func (ftsFuncValidation) Name() string {
	return "ftsFuncValidation"
}

// Optimize implements the LogicalOptRule interface.
// The check is performed here because we don't know whether a function is invalid or not until:
// 1. it's pushed down after applying the predicate push down rule.
// 2. it's really checked whether can be used to build a fts index request.
// So final check is performed here.
func (f *ftsFuncValidation) Optimize(ctx context.Context, p base.LogicalPlan) (base.LogicalPlan, bool, error) {
	planChanged := substituteMatchAgainstScoreColumnsInPlan(p)
	if err := f.doQuickValidation(ctx, p); err != nil {
		return p, planChanged, err
	}
	if pruneUnusedMatchAgainstScoreColumnsInPlan(p) {
		planChanged = true
	}
	return p, planChanged, nil
}

func (f *ftsFuncValidation) doQuickValidation(ctx context.Context, p base.LogicalPlan) error {
	switch x := p.(type) {
	case *logicalop.LogicalProjection:
		if expression.ContainsFullTextSearchFn(x.Exprs...) {
			return plannererrors.ErrWrongUsage.FastGen("Currently 'FTS_MATCH_WORD()' cannot be used in SELECT fields. It can be used in WHERE only")
		}
	case *logicalop.LogicalSelection:
		if expression.ContainsFullTextSearchFn(x.Conditions...) {
			return plannererrors.ErrWrongUsage.FastGen("Currently 'FTS_MATCH_WORD()' must be used alone. It cannot be placed inside any other function or expression as a parameter, or used multiple times. A valid example: SELECT * FROM <TABLE> WHERE FTS_MATCH_WORD(...)")
		}
	case *logicalop.LogicalTopN:
		for _, item := range x.ByItems {
			if expression.ContainsFullTextSearchFn(item.Expr) || isFTSScoreOrderExpr(item.Expr, firstChildPlan(x)) {
				return plannererrors.ErrWrongUsage.FastGen("Currently 'FTS_MATCH_WORD()' in ORDER BY is not supported")
			}
		}
	case *logicalop.LogicalSort:
		for _, item := range x.ByItems {
			if expression.ContainsFullTextSearchFn(item.Expr) || isFTSScoreOrderExpr(item.Expr, firstChildPlan(x)) {
				return plannererrors.ErrWrongUsage.FastGen("Currently 'FTS_MATCH_WORD()' in ORDER BY clause is not supported")
			}
		}
	case *logicalop.LogicalJoin:
		if expression.ContainsFullTextSearchFn(x.OtherConditions...) {
			return plannererrors.ErrWrongUsage.FastGen("Currently 'FTS_MATCH_WORD()' cannot be used in JOIN ON conditions")
		}
		if expression.ContainsFullTextSearchFn(x.LeftConditions...) {
			return plannererrors.ErrWrongUsage.FastGen("Currently 'FTS_MATCH_WORD()' cannot be used in JOIN ON conditions")
		}
		if expression.ContainsFullTextSearchFn(x.RightConditions...) {
			return plannererrors.ErrWrongUsage.FastGen("Currently 'FTS_MATCH_WORD()' cannot be used in JOIN ON conditions")
		}
	case *logicalop.LogicalWindow:
		for _, item := range x.WindowFuncDescs {
			if expression.ContainsFullTextSearchFn(item.Args...) {
				return plannererrors.ErrWrongUsage.FastGen("Currently 'FTS_MATCH_WORD()' cannot be used in window function")
			}
		}
	case *logicalop.LogicalAggregation:
		for _, agg := range x.AggFuncs {
			if expression.ContainsFullTextSearchFn(agg.Args...) {
				return plannererrors.ErrWrongUsage.FastGen("Currently 'FTS_MATCH_WORD()' cannot be used in GROUP BY or HAVING")
			}
		}
		if expression.ContainsFullTextSearchFn(x.GroupByItems...) {
			return plannererrors.ErrWrongUsage.FastGen("Currently 'FTS_MATCH_WORD()' cannot be used in GROUP BY or HAVING")
		}
	case *logicalop.LogicalCTE:
		// current cte's optimization is a separate process, so we don't need to check the cte's children.
	case *logicalop.LogicalSequence:
		// last child is the main query.
		return f.doQuickValidation(ctx, x.Children()[len(x.Children())-1])
	}
	for _, child := range p.Children() {
		if err := f.doQuickValidation(ctx, child); err != nil {
			return err
		}
	}
	return nil
}

func firstChildPlan(p base.LogicalPlan) base.LogicalPlan {
	if len(p.Children()) == 0 {
		return nil
	}
	return p.Children()[0]
}

func isFTSScoreOrderExpr(expr expression.Expression, input base.LogicalPlan) bool {
	if hasVirtualFTSScoreColumn(expr) {
		return true
	}
	proj, ok := input.(*logicalop.LogicalProjection)
	if !ok || len(proj.Children()) == 0 {
		return false
	}
	for _, col := range expression.ExtractColumns(expr) {
		idx := proj.Schema().ColumnIndex(col)
		if idx < 0 || idx >= len(proj.Exprs) {
			continue
		}
		if isFTSScoreOrderExpr(proj.Exprs[idx], firstChildPlan(proj)) {
			return true
		}
	}
	return false
}

func hasVirtualFTSScoreColumn(expr expression.Expression) bool {
	for _, col := range expression.ExtractColumns(expr) {
		if col.ID == model.VirtualColFTSScoreID {
			return true
		}
	}
	return false
}

func pruneUnusedMatchAgainstScoreColumnsInPlan(p base.LogicalPlan) bool {
	usedScoreCols := make(map[int64]struct{})
	collectUsedVirtualFTSScoreColumns(p, usedScoreCols)
	return pruneUnusedVirtualFTSScoreColumns(p, usedScoreCols)
}

func collectUsedVirtualFTSScoreColumns(p base.LogicalPlan, usedScoreCols map[int64]struct{}) {
	switch x := p.(type) {
	case *logicalop.LogicalProjection:
		markUsedVirtualFTSScoreColumns(usedScoreCols, x.Exprs...)
	case *logicalop.LogicalSelection:
		markUsedVirtualFTSScoreColumns(usedScoreCols, x.Conditions...)
	case *logicalop.LogicalTopN:
		for _, item := range x.ByItems {
			markUsedVirtualFTSScoreColumns(usedScoreCols, item.Expr)
		}
	case *logicalop.LogicalSort:
		for _, item := range x.ByItems {
			markUsedVirtualFTSScoreColumns(usedScoreCols, item.Expr)
		}
	case *logicalop.LogicalJoin:
		markUsedVirtualFTSScoreColumns(usedScoreCols, x.OtherConditions...)
		markUsedVirtualFTSScoreColumns(usedScoreCols, x.LeftConditions...)
		markUsedVirtualFTSScoreColumns(usedScoreCols, x.RightConditions...)
	case *logicalop.LogicalWindow:
		for _, item := range x.WindowFuncDescs {
			markUsedVirtualFTSScoreColumns(usedScoreCols, item.Args...)
		}
	case *logicalop.LogicalAggregation:
		for _, agg := range x.AggFuncs {
			markUsedVirtualFTSScoreColumns(usedScoreCols, agg.Args...)
		}
		markUsedVirtualFTSScoreColumns(usedScoreCols, x.GroupByItems...)
	}
	for _, child := range p.Children() {
		collectUsedVirtualFTSScoreColumns(child, usedScoreCols)
	}
}

func markUsedVirtualFTSScoreColumns(usedScoreCols map[int64]struct{}, exprs ...expression.Expression) {
	for _, expr := range exprs {
		for _, col := range expression.ExtractColumns(expr) {
			if col.ID == model.VirtualColFTSScoreID {
				usedScoreCols[col.UniqueID] = struct{}{}
			}
		}
	}
}

func pruneUnusedVirtualFTSScoreColumns(p base.LogicalPlan, usedScoreCols map[int64]struct{}) bool {
	changed := false
	for _, child := range p.Children() {
		if pruneUnusedVirtualFTSScoreColumns(child, usedScoreCols) {
			changed = true
		}
	}
	ds, ok := p.(*logicalop.DataSource)
	if !ok {
		return changed
	}
	if !hasUnusedVirtualFTSScoreColumn(ds, usedScoreCols) {
		return changed
	}
	for _, path := range ds.PossibleAccessPaths {
		if path.FtsQueryInfo != nil && path.FtsQueryInfo.BooleanQuery != nil {
			path.FtsQueryInfo.QueryType = tipb.FTSQueryType_FTSQueryTypeNoScore
		}
	}
	if removeVirtualFTSScoreColumnFromDS(ds) {
		changed = true
	}
	return changed
}

func hasUnusedVirtualFTSScoreColumn(ds *logicalop.DataSource, usedScoreCols map[int64]struct{}) bool {
	if ds.Schema() == nil {
		return false
	}
	for _, col := range ds.Schema().Columns {
		if col.ID != model.VirtualColFTSScoreID {
			continue
		}
		_, used := usedScoreCols[col.UniqueID]
		return !used
	}
	return false
}

func removeVirtualFTSScoreColumnFromDS(ds *logicalop.DataSource) bool {
	if ds.Schema() == nil {
		return false
	}
	schemaIdx := -1
	for i, col := range ds.Schema().Columns {
		if col.ID == model.VirtualColFTSScoreID {
			schemaIdx = i
			break
		}
	}
	if schemaIdx == -1 {
		return false
	}
	ds.Schema().Columns = append(ds.Schema().Columns[:schemaIdx], ds.Schema().Columns[schemaIdx+1:]...)
	for i, col := range ds.Columns {
		if col.ID == model.VirtualColFTSScoreID {
			ds.Columns = append(ds.Columns[:i], ds.Columns[i+1:]...)
			break
		}
	}
	outputNames := ds.OutputNames()
	if schemaIdx < len(outputNames) {
		ds.SetOutputNames(append(outputNames[:schemaIdx], outputNames[schemaIdx+1:]...))
	}
	for i, col := range ds.TblCols {
		if col.ID == model.VirtualColFTSScoreID {
			ds.TblCols = append(ds.TblCols[:i], ds.TblCols[i+1:]...)
			break
		}
	}
	if ds.TblColsByID != nil {
		delete(ds.TblColsByID, model.VirtualColFTSScoreID)
	}
	if ds.ColsRequiringFullLen != nil {
		for i, col := range ds.ColsRequiringFullLen {
			if col.ID == model.VirtualColFTSScoreID {
				ds.ColsRequiringFullLen = append(ds.ColsRequiringFullLen[:i], ds.ColsRequiringFullLen[i+1:]...)
				break
			}
		}
	}
	return true
}

func substituteMatchAgainstScoreColumnsInPlan(p base.LogicalPlan) bool {
	exprToColumn := make(ExprColumnMap)
	collectMatchAgainstScoreColumns(p, exprToColumn)
	if len(exprToColumn) == 0 {
		return false
	}
	return substituteMatchAgainstScoreColumnsInProjection(p, exprToColumn)
}

func collectMatchAgainstScoreColumns(p base.LogicalPlan, exprToColumn ExprColumnMap) {
	for _, child := range p.Children() {
		collectMatchAgainstScoreColumns(child, exprToColumn)
	}
	ds, ok := p.(*logicalop.DataSource)
	if !ok {
		return
	}
	for _, col := range ds.Schema().Columns {
		if col.ID != model.VirtualColFTSScoreID || col.VirtualExpr == nil {
			continue
		}
		exprToColumn[col.VirtualExpr] = col
	}
}

func substituteMatchAgainstScoreColumnsInProjection(p base.LogicalPlan, exprToColumn ExprColumnMap) bool {
	changed := false
	for _, child := range p.Children() {
		if substituteMatchAgainstScoreColumnsInProjection(child, exprToColumn) {
			changed = true
		}
	}

	proj, ok := p.(*logicalop.LogicalProjection)
	if !ok || len(proj.Children()) == 0 {
		return changed
	}
	childSchema := proj.Children()[0].Schema()
	ectx := p.SCtx().GetExprCtx().GetEvalCtx()
	for i := range proj.Exprs {
		tp := proj.Exprs[i].GetType(ectx).EvalType()
		for candidateExpr, column := range exprToColumn {
			if tryToSubstituteExpr(&proj.Exprs[i], p, candidateExpr, tp, childSchema, column) {
				changed = true
			}
		}
	}
	return changed
}
