package core

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	mockctx "github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestFTSFuncValidationSubstitutesMatchAgainstInProjection(t *testing.T) {
	ctx := mockctx.NewContext()

	ds, matchFn := newTestDataSourceWithScoreColumn(t, ctx, "hello")
	proj := logicalop.LogicalProjection{
		Exprs: []expression.Expression{
			ds.Schema().Columns[0],
			matchFn.Clone(),
		},
	}.Init(ctx, 0)
	proj.SetChildren(ds)
	proj.SetSchema(expression.NewSchema(
		ds.Schema().Columns[0],
		&expression.Column{
			ID:       10,
			UniqueID: 10,
			RetType:  types.NewFieldType(mysql.TypeDouble),
		},
	))

	optimized, changed, err := (&ftsFuncValidation{}).Optimize(context.TODO(), proj)
	require.NoError(t, err)
	require.True(t, changed)

	optimizedProj := optimized.(*logicalop.LogicalProjection)
	scoreCol, ok := optimizedProj.Exprs[1].(*expression.Column)
	require.True(t, ok)
	require.Equal(t, model.VirtualColFTSScoreID, scoreCol.ID)
}

func TestFTSFuncValidationSubstitutesMatchAgainstInSort(t *testing.T) {
	ctx := mockctx.NewContext()

	ds, matchFn := newTestDataSourceWithScoreColumn(t, ctx, "hello")
	sort := logicalop.LogicalSort{
		ByItems: []*plannerutil.ByItems{{
			Expr: matchFn.Clone(),
			Desc: true,
		}},
	}.Init(ctx, 0)
	sort.SetChildren(ds)

	optimized, changed, err := (&ftsFuncValidation{}).Optimize(context.TODO(), sort)
	require.NoError(t, err)
	require.True(t, changed)

	optimizedSort := optimized.(*logicalop.LogicalSort)
	scoreCol, ok := optimizedSort.ByItems[0].Expr.(*expression.Column)
	require.True(t, ok)
	require.Equal(t, model.VirtualColFTSScoreID, scoreCol.ID)
}

func TestFTSFuncValidationSynthesizesMatchAgainstWithoutWhere(t *testing.T) {
	ctx := mockctx.NewContext()
	ctx.Store = &mockctx.Store{Client: &mockctx.Client{}}

	ds, matchFn := newTestTiCIDataSourceWithoutScoreColumn(t, ctx, "hello")
	proj := logicalop.LogicalProjection{
		Exprs: []expression.Expression{
			ds.Schema().Columns[0],
			matchFn.Clone(),
		},
	}.Init(ctx, 0)
	proj.SetChildren(ds)
	proj.SetSchema(expression.NewSchema(
		ds.Schema().Columns[0],
		&expression.Column{
			ID:       10,
			UniqueID: 10,
			RetType:  types.NewFieldType(mysql.TypeDouble),
		},
	))

	optimized, changed, err := (&ftsFuncValidation{}).Optimize(context.TODO(), proj)
	require.NoError(t, err)
	require.True(t, changed)

	optimizedProj := optimized.(*logicalop.LogicalProjection)
	scoreCol, ok := optimizedProj.Exprs[1].(*expression.Column)
	require.True(t, ok)
	require.Equal(t, model.VirtualColFTSScoreID, scoreCol.ID)
	require.NotNil(t, ds.PossibleAccessPaths[0].FtsQueryInfo)
	require.Equal(t, tipb.FTSQueryType_FTSQueryTypeWithScore, ds.PossibleAccessPaths[0].FtsQueryInfo.GetQueryType())
	require.Len(t, ds.PossibleAccessPaths[0].FtsQueryInfo.MatchExpr, 0)
	require.NotNil(t, ds.PossibleAccessPaths[0].FtsQueryInfo.BooleanQuery)
}

func newTestTiCIDataSourceWithoutScoreColumn(t *testing.T, ctx *mockctx.Context, pattern string) (*logicalop.DataSource, *expression.ScalarFunction) {
	t.Helper()

	titleCol := &expression.Column{
		ID:       1,
		UniqueID: 1,
		RetType:  types.NewFieldType(mysql.TypeString),
		OrigName: "test.t.title",
	}
	matchFn := buildTestMatchAgainstExpr(t, ctx, pattern, titleCol)
	titleColumnInfo := &model.ColumnInfo{
		ID:        1,
		Offset:    0,
		Name:      ast.NewCIStr("title"),
		FieldType: *types.NewFieldType(mysql.TypeString),
	}
	indexInfo := &model.IndexInfo{
		ID:   11,
		Name: ast.NewCIStr("idx_title"),
		Columns: []*model.IndexColumn{{
			Name:   ast.NewCIStr("title"),
			Offset: 0,
			Length: types.UnspecifiedLength,
		}},
		FullTextInfo: &model.FullTextIndexInfo{ParserType: model.FullTextParserTypeStandardV1},
	}
	path := &plannerutil.AccessPath{Index: indexInfo}
	ds := logicalop.DataSource{
		TableInfo: &model.TableInfo{
			ID:      100,
			Name:    ast.NewCIStr("t"),
			Columns: []*model.ColumnInfo{titleColumnInfo},
		},
		Columns:                []*model.ColumnInfo{titleColumnInfo},
		PossibleAccessPaths:    []*plannerutil.AccessPath{path},
		AllPossibleAccessPaths: []*plannerutil.AccessPath{path},
	}.Init(ctx, 0)
	ds.SetSchema(expression.NewSchema(titleCol))
	ds.SetOutputNames(types.NameSlice{{
		DBName:      ast.NewCIStr("test"),
		TblName:     ast.NewCIStr("t"),
		ColName:     ast.NewCIStr("title"),
		OrigColName: ast.NewCIStr("title"),
	}})
	ds.TblCols = []*expression.Column{titleCol}
	ds.TblColsByID = map[int64]*expression.Column{1: titleCol}
	return ds, matchFn
}

func newTestDataSourceWithScoreColumn(t *testing.T, ctx *mockctx.Context, pattern string) (*logicalop.DataSource, *expression.ScalarFunction) {
	t.Helper()

	titleCol := &expression.Column{
		ID:       1,
		UniqueID: 1,
		RetType:  types.NewFieldType(mysql.TypeString),
		OrigName: "test.t.title",
	}
	matchFn := buildTestMatchAgainstExpr(t, ctx, pattern, titleCol)
	scoreCol := &expression.Column{
		ID:          model.VirtualColFTSScoreID,
		UniqueID:    2,
		RetType:     types.NewFieldType(mysql.TypeDouble),
		VirtualExpr: matchFn.Clone(),
		OrigName:    model.VirtualColFTSScoreName.O,
	}

	ds := logicalop.DataSource{
		TableInfo: &model.TableInfo{
			ID:   100,
			Name: ast.NewCIStr("t"),
		},
	}.Init(ctx, 0)
	ds.SetSchema(expression.NewSchema(titleCol, scoreCol))
	return ds, matchFn
}

func buildTestMatchAgainstExpr(
	t *testing.T,
	ctx *mockctx.Context,
	pattern string,
	matchCols ...expression.Expression,
) *expression.ScalarFunction {
	t.Helper()

	args := make([]expression.Expression, 0, len(matchCols)+1)
	args = append(args, &expression.Constant{
		Value:   types.NewStringDatum(pattern),
		RetType: types.NewFieldType(mysql.TypeString),
	})
	args = append(args, matchCols...)
	expr, err := expression.NewFunction(
		ctx,
		ast.FTSMysqlMatchAgainst,
		types.NewFieldType(mysql.TypeDouble),
		args...,
	)
	require.NoError(t, err)
	matchFn, ok := expr.(*expression.ScalarFunction)
	require.True(t, ok)
	require.NoError(t, expression.SetFTSMysqlMatchAgainstModifier(matchFn, ast.FulltextSearchModifierBooleanMode))
	return matchFn
}
