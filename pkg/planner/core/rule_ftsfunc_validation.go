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
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
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
	planChanged, err := substituteMatchAgainstScoreColumns(p)
	if err != nil {
		return p, false, err
	}
	return p, planChanged, f.doQuickValidation(ctx, p)
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
			if expression.ContainsFullTextSearchFn(item.Expr) {
				return plannererrors.ErrWrongUsage.FastGen("Currently 'FTS_MATCH_WORD()' in ORDER BY is not supported")
			}
		}
	case *logicalop.LogicalSort:
		for _, item := range x.ByItems {
			if expression.ContainsFullTextSearchFn(item.Expr) {
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

func substituteMatchAgainstScoreColumns(p base.LogicalPlan) (bool, error) {
	changed := false
	exprToColumn := make(ExprColumnMap)
	collectMatchAgainstScoreColumns(p, exprToColumn)
	if len(exprToColumn) > 0 && substituteMatchAgainstScoreColumnsInPlan(p, exprToColumn) {
		changed = true
	}

	synthesized, err := synthesizeMatchAgainstScoreColumnsInPlan(p)
	if err != nil {
		return false, err
	}
	if !synthesized {
		return changed, nil
	}

	exprToColumn = make(ExprColumnMap)
	collectMatchAgainstScoreColumns(p, exprToColumn)
	if len(exprToColumn) > 0 && substituteMatchAgainstScoreColumnsInPlan(p, exprToColumn) {
		changed = true
	}
	return true, nil
}

func synthesizeMatchAgainstScoreColumnsInPlan(p base.LogicalPlan) (bool, error) {
	changed := false
	for _, child := range p.Children() {
		childChanged, err := synthesizeMatchAgainstScoreColumnsInPlan(child)
		if err != nil {
			return false, err
		}
		changed = changed || childChanged
	}

	var exprs []expression.Expression
	switch x := p.(type) {
	case *logicalop.LogicalProjection:
		exprs = x.Exprs
	case *logicalop.LogicalSort:
		for _, item := range x.ByItems {
			exprs = append(exprs, item.Expr)
		}
	case *logicalop.LogicalTopN:
		for _, item := range x.ByItems {
			exprs = append(exprs, item.Expr)
		}
	default:
		return changed, nil
	}
	if len(p.Children()) != 1 {
		return changed, nil
	}
	childDS := findSingleDataSource(p.Children()[0])
	if childDS == nil || len(childDS.PushedDownConds) > 0 {
		return changed, nil
	}
	for _, expr := range exprs {
		built, err := maybeSynthesizeMatchAgainstScoreColumn(childDS, expr)
		if err != nil {
			return false, err
		}
		changed = changed || built
	}
	return changed, nil
}

func maybeSynthesizeMatchAgainstScoreColumn(ds *logicalop.DataSource, expr expression.Expression) (bool, error) {
	scalarFunc, ok := expr.(*expression.ScalarFunction)
	if !ok || scalarFunc.FuncName.L != ast.FTSMysqlMatchAgainst {
		return false, nil
	}
	resolvedExpr := resolveExprToVirtualColumn(scalarFunc, ds.Schema(), ds.SCtx().GetExprCtx().GetEvalCtx())
	if col, ok := resolvedExpr.(*expression.Column); ok && col.ID == model.VirtualColFTSScoreID {
		return false, nil
	}
	if ds.PossibleAccessPaths != nil && len(ds.PossibleAccessPaths) > 0 && ds.PossibleAccessPaths[0].FtsQueryInfo != nil {
		return false, nil
	}
	index := ds.ChooseTiCIIndexForScoreExpr(scalarFunc)
	if index == nil {
		return false, nil
	}
	if err := ds.BuildTiCIScoreOnlyPath(index, scalarFunc); err != nil {
		return false, err
	}
	return true, nil
}

func findSingleDataSource(p base.LogicalPlan) *logicalop.DataSource {
	var found *logicalop.DataSource
	multiple := false
	var visit func(base.LogicalPlan)
	visit = func(plan base.LogicalPlan) {
		if plan == nil || multiple {
			return
		}
		if ds, ok := plan.(*logicalop.DataSource); ok {
			if found != nil && found != ds {
				multiple = true
				return
			}
			found = ds
			return
		}
		for _, child := range plan.Children() {
			visit(child)
		}
	}
	visit(p)
	if multiple {
		return nil
	}
	return found
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

func substituteMatchAgainstScoreColumnsInPlan(p base.LogicalPlan, exprToColumn ExprColumnMap) bool {
	changed := false
	for _, child := range p.Children() {
		if substituteMatchAgainstScoreColumnsInPlan(child, exprToColumn) {
			changed = true
		}
	}

	ectx := p.SCtx().GetExprCtx().GetEvalCtx()
	switch x := p.(type) {
	case *logicalop.LogicalProjection:
		for i := range x.Exprs {
			tp := x.Exprs[i].GetType(ectx).EvalType()
			for candidateExpr, column := range exprToColumn {
				if tryToSubstituteExpr(&x.Exprs[i], p, candidateExpr, tp, x.Children()[0].Schema(), column) {
					changed = true
				}
			}
		}
	case *logicalop.LogicalSort:
		for i := range x.ByItems {
			tp := x.ByItems[i].Expr.GetType(ectx).EvalType()
			for candidateExpr, column := range exprToColumn {
				if tryToSubstituteExpr(&x.ByItems[i].Expr, p, candidateExpr, tp, x.Children()[0].Schema(), column) {
					changed = true
				}
			}
		}
	case *logicalop.LogicalTopN:
		for i := range x.ByItems {
			tp := x.ByItems[i].Expr.GetType(ectx).EvalType()
			for candidateExpr, column := range exprToColumn {
				if tryToSubstituteExpr(&x.ByItems[i].Expr, p, candidateExpr, tp, x.Children()[0].Schema(), column) {
					changed = true
				}
			}
		}
	}
	return changed
}
