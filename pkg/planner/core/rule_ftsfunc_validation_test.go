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

func TestFTSFuncValidationRejectsProjectedScoreInSort(t *testing.T) {
	ctx := mockctx.NewContext()

	proj, scoreCol := newTestProjectionWithProjectedScore(t, ctx, "hello")
	sort := logicalop.LogicalSort{
		ByItems: []*plannerutil.ByItems{{Expr: scoreCol}},
	}.Init(ctx, 0)
	sort.SetChildren(proj)

	_, _, err := (&ftsFuncValidation{}).Optimize(context.TODO(), sort)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Currently 'FTS_MATCH_WORD()' in ORDER BY clause is not supported")
}

func TestFTSFuncValidationRejectsProjectedScoreInTopN(t *testing.T) {
	ctx := mockctx.NewContext()

	proj, scoreCol := newTestProjectionWithProjectedScore(t, ctx, "hello")
	topN := logicalop.LogicalTopN{
		ByItems: []*plannerutil.ByItems{{Expr: scoreCol}},
	}.Init(ctx, 0)
	topN.SetChildren(proj)

	_, _, err := (&ftsFuncValidation{}).Optimize(context.TODO(), topN)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Currently 'FTS_MATCH_WORD()' in ORDER BY is not supported")
}

func TestFTSFuncValidationPrunesUnusedScorePathForFilterOnlyQuery(t *testing.T) {
	ctx := mockctx.NewContext()

	ds, _ := newTestDataSourceWithScoreColumn(t, ctx, "hello")

	optimized, changed, err := (&ftsFuncValidation{}).Optimize(context.TODO(), ds)
	require.NoError(t, err)
	require.True(t, changed)

	optimizedDS := optimized.(*logicalop.DataSource)
	require.Nil(t, findSchemaColumnByID(optimizedDS.Schema(), model.VirtualColFTSScoreID))
	require.Len(t, optimizedDS.Columns, 1)
	require.Len(t, optimizedDS.OutputNames(), 1)
	require.Len(t, optimizedDS.TblCols, 1)
	require.NotNil(t, optimizedDS.PossibleAccessPaths[0].FtsQueryInfo)
	require.Equal(t, tipb.FTSQueryType_FTSQueryTypeNoScore, optimizedDS.PossibleAccessPaths[0].FtsQueryInfo.GetQueryType())
	require.NotNil(t, optimizedDS.PossibleAccessPaths[0].FtsQueryInfo.BooleanQuery)
}

func TestFTSFuncValidationKeepsScorePathForProjectedScore(t *testing.T) {
	ctx := mockctx.NewContext()

	proj, _ := newTestProjectionWithProjectedScore(t, ctx, "hello")

	optimized, changed, err := (&ftsFuncValidation{}).Optimize(context.TODO(), proj)
	require.NoError(t, err)
	require.True(t, changed)

	optimizedProj := optimized.(*logicalop.LogicalProjection)
	optimizedDS := optimizedProj.Children()[0].(*logicalop.DataSource)
	require.NotNil(t, findSchemaColumnByID(optimizedDS.Schema(), model.VirtualColFTSScoreID))
	require.Equal(t, tipb.FTSQueryType_FTSQueryTypeWithScore, optimizedDS.PossibleAccessPaths[0].FtsQueryInfo.GetQueryType())
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
		IsHidden:    true,
	}
	titleColInfo := &model.ColumnInfo{
		ID:        1,
		Offset:    0,
		Name:      ast.NewCIStr("title"),
		FieldType: *types.NewFieldType(mysql.TypeString),
	}
	path := &plannerutil.AccessPath{
		FtsQueryInfo: &tipb.FTSQueryInfo{
			QueryType: tipb.FTSQueryType_FTSQueryTypeWithScore,
			BooleanQuery: &tipb.FTSBooleanQuery{
				Nodes: []*tipb.FTSBooleanNode{{}},
			},
		},
	}

	ds := logicalop.DataSource{
		DBName: ast.NewCIStr("test"),
		TableInfo: &model.TableInfo{
			ID:      100,
			Name:    ast.NewCIStr("t"),
			Columns: []*model.ColumnInfo{titleColInfo},
		},
		Columns:                []*model.ColumnInfo{titleColInfo, model.NewVirtualFTSScoreColInfo()},
		PossibleAccessPaths:    []*plannerutil.AccessPath{path},
		AllPossibleAccessPaths: []*plannerutil.AccessPath{path},
	}.Init(ctx, 0)
	ds.SetSchema(expression.NewSchema(titleCol, scoreCol))
	ds.SetOutputNames(types.NameSlice{
		&types.FieldName{
			DBName:      ast.NewCIStr("test"),
			TblName:     ast.NewCIStr("t"),
			ColName:     ast.NewCIStr("title"),
			OrigColName: ast.NewCIStr("title"),
		},
		&types.FieldName{
			DBName:      ast.NewCIStr("test"),
			TblName:     ast.NewCIStr("t"),
			ColName:     model.VirtualColFTSScoreName,
			OrigColName: model.VirtualColFTSScoreName,
			Hidden:      true,
		},
	})
	ds.TblCols = []*expression.Column{titleCol, scoreCol}
	ds.TblColsByID = map[int64]*expression.Column{
		titleCol.ID: titleCol,
		scoreCol.ID: scoreCol,
	}
	return ds, matchFn
}

func newTestProjectionWithProjectedScore(t *testing.T, ctx *mockctx.Context, pattern string) (*logicalop.LogicalProjection, *expression.Column) {
	t.Helper()

	ds, matchFn := newTestDataSourceWithScoreColumn(t, ctx, pattern)
	titleCol := ds.Schema().Columns[0]
	projScoreExpr := matchFn.Clone()
	projScoreCol := &expression.Column{
		ID:       10,
		UniqueID: 10,
		RetType:  types.NewFieldType(mysql.TypeDouble),
	}
	proj := logicalop.LogicalProjection{
		Exprs: []expression.Expression{
			titleCol,
			projScoreExpr,
		},
	}.Init(ctx, 0)
	proj.SetChildren(ds)
	proj.SetSchema(expression.NewSchema(titleCol, projScoreCol))
	return proj, projScoreCol
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

func findSchemaColumnByID(schema *expression.Schema, id int64) *expression.Column {
	if schema == nil {
		return nil
	}
	for _, col := range schema.Columns {
		if col.ID == id {
			return col
		}
	}
	return nil
}
