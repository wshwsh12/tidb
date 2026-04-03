package logicalop

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intset"
	mockctx "github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestTiCIBuildPathUsesStructuredBooleanQuery(t *testing.T) {
	ctx := mockctx.NewContext()
	ctx.Store = &mockctx.Store{Client: &mockctx.Client{}}

	ds, indexInfo := newTestTiCIDataSource(t, ctx, buildTestMatchAgainst(t, ctx, `+a -b c >d <e ~f`))
	matchedExprSet := intset.NewFastIntSet()
	matchedExprSet.Insert(0)

	matchedCondIdxes, err := ds.rewriteMatchedTiCIFTSExprs(indexInfo, matchedExprSet)
	require.NoError(t, err)
	require.Equal(t, ast.FTSMysqlMatchAgainst, ds.PushedDownConds[0].(*expression.ScalarFunction).FuncName.L)

	err = ds.buildTiCIFTSPathAndCleanUp(indexInfo, matchedCondIdxes)
	require.NoError(t, err)

	ftsQueryInfo := ds.PossibleAccessPaths[0].FtsQueryInfo
	require.NotNil(t, ftsQueryInfo)
	require.Equal(t, tipb.FTSQueryType_FTSQueryTypeWithScore, ftsQueryInfo.GetQueryType())
	require.Len(t, ftsQueryInfo.MatchExpr, 1)
	require.Len(t, ftsQueryInfo.Columns, 1)
	require.Equal(t, int64(1), ftsQueryInfo.Columns[0].GetColumnId())
	require.NotNil(t, ftsQueryInfo.BooleanQuery)
	require.Len(t, ftsQueryInfo.BooleanQuery.Nodes, 6)
	require.Equal(t, tipb.FTSBooleanOccur_FTSBooleanOccurMust, ftsQueryInfo.BooleanQuery.Nodes[0].GetOccur())
	require.Equal(t, "a", ftsQueryInfo.BooleanQuery.Nodes[0].GetTerm().GetText())
	require.Len(t, ds.PossibleAccessPaths[0].AccessConds, 1)
	require.Equal(t, ast.FTSMysqlMatchAgainst, ds.PossibleAccessPaths[0].AccessConds[0].(*expression.ScalarFunction).FuncName.L)
	scoreCol := findSchemaColumnByID(ds.Schema(), model.VirtualColFTSScoreID)
	require.NotNil(t, scoreCol)
	require.Equal(t, byte(mysql.TypeDouble), scoreCol.RetType.GetType())
	require.NotNil(t, scoreCol.VirtualExpr)
	require.True(t, scoreCol.VirtualExpr.Equal(ctx.GetExprCtx().GetEvalCtx(), ds.PossibleAccessPaths[0].AccessConds[0]))
}

func TestTiCIBuildPathFallsBackForMultipleMatchAgainst(t *testing.T) {
	ctx := mockctx.NewContext()
	ctx.Store = &mockctx.Store{Client: &mockctx.Client{}}

	ds, indexInfo := newTestTiCIDataSource(
		t,
		ctx,
		buildTestMatchAgainst(t, ctx, `hello`),
		buildTestMatchAgainst(t, ctx, `world`),
	)
	matchedExprSet := intset.NewFastIntSet()
	matchedExprSet.Insert(0)
	matchedExprSet.Insert(1)

	matchedCondIdxes, err := ds.rewriteMatchedTiCIFTSExprs(indexInfo, matchedExprSet)
	require.NoError(t, err)
	for _, cond := range ds.PushedDownConds {
		require.NotEqual(t, ast.FTSMysqlMatchAgainst, cond.(*expression.ScalarFunction).FuncName.L)
	}

	err = ds.buildTiCIFTSPathAndCleanUp(indexInfo, matchedCondIdxes)
	require.NoError(t, err)

	ftsQueryInfo := ds.PossibleAccessPaths[0].FtsQueryInfo
	require.NotNil(t, ftsQueryInfo)
	require.Equal(t, tipb.FTSQueryType_FTSQueryTypeNoScore, ftsQueryInfo.GetQueryType())
	require.Nil(t, ftsQueryInfo.BooleanQuery)
	require.Len(t, ftsQueryInfo.MatchExpr, 2)
	require.Len(t, ds.PossibleAccessPaths[0].AccessConds, 2)
	for _, cond := range ds.PossibleAccessPaths[0].AccessConds {
		require.NotEqual(t, ast.FTSMysqlMatchAgainst, cond.(*expression.ScalarFunction).FuncName.L)
	}
}

func TestTiCISingleScanTreatsVirtualScoreAsCovered(t *testing.T) {
	ctx := mockctx.NewContext()
	ctx.Store = &mockctx.Store{Client: &mockctx.Client{}}

	ds, indexInfo := newTestTiCIDataSource(t, ctx, buildTestMatchAgainst(t, ctx, `+a -b c >d <e ~f`))
	matchedExprSet := intset.NewFastIntSet()
	matchedExprSet.Insert(0)

	matchedCondIdxes, err := ds.rewriteMatchedTiCIFTSExprs(indexInfo, matchedExprSet)
	require.NoError(t, err)
	require.NoError(t, ds.buildTiCIFTSPathAndCleanUp(indexInfo, matchedCondIdxes))

	path := ds.PossibleAccessPaths[0]
	path.IdxCols, path.IdxColLens, path.FullIdxCols, path.FullIdxColLens = plannerutil.IndexInfo2Cols(ds.Columns, ds.Schema().Columns, path.Index)
	ds.ColsRequiringFullLen = ds.Schema().Columns

	require.Len(t, path.FullIdxCols, 1)
	require.Equal(t, int64(1), path.FullIdxCols[0].ID)
	require.Empty(t, path.TableFilters)
	require.True(t, ds.indexCoveringColumn(ds.Schema().Columns[0], path.FullIdxCols, path.FullIdxColLens, false))
	require.True(t, ds.IsTiCISingleScan(path.FullIdxCols, path.FullIdxColLens, path))
}

func newTestTiCIDataSource(
	t *testing.T,
	ctx *mockctx.Context,
	conds ...expression.Expression,
) (*DataSource, *model.IndexInfo) {
	t.Helper()

	titleFieldType := types.NewFieldType(mysql.TypeString)
	titleColumnInfo := &model.ColumnInfo{
		ID:        1,
		Offset:    0,
		Name:      ast.NewCIStr("title"),
		FieldType: *titleFieldType,
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
	ds := DataSource{
		TableInfo: &model.TableInfo{
			ID:      100,
			Name:    ast.NewCIStr("t"),
			Columns: []*model.ColumnInfo{titleColumnInfo},
		},
		Columns:                []*model.ColumnInfo{titleColumnInfo},
		PushedDownConds:        conds,
		PossibleAccessPaths:    []*plannerutil.AccessPath{path},
		AllPossibleAccessPaths: []*plannerutil.AccessPath{path},
	}.Init(ctx, 0)
	titleCol := &expression.Column{
		ID:       1,
		UniqueID: 1,
		RetType:  types.NewFieldType(mysql.TypeString),
		OrigName: "test.t.title",
	}
	ds.SetSchema(expression.NewSchema(titleCol))
	ds.SetOutputNames(types.NameSlice{{
		DBName:      ast.NewCIStr("test"),
		TblName:     ast.NewCIStr("t"),
		ColName:     ast.NewCIStr("title"),
		OrigColName: ast.NewCIStr("title"),
	}})
	ds.TblCols = []*expression.Column{titleCol}
	ds.TblColsByID = map[int64]*expression.Column{1: titleCol}
	return ds, indexInfo
}

func buildTestMatchAgainst(t *testing.T, ctx *mockctx.Context, pattern string) *expression.ScalarFunction {
	t.Helper()

	titleCol := &expression.Column{
		ID:       1,
		UniqueID: 1,
		RetType:  types.NewFieldType(mysql.TypeString),
		OrigName: "test.t.title",
	}
	expr, err := expression.NewFunction(
		ctx,
		ast.FTSMysqlMatchAgainst,
		types.NewFieldType(mysql.TypeDouble),
		&expression.Constant{
			Value:   types.NewStringDatum(pattern),
			RetType: types.NewFieldType(mysql.TypeString),
		},
		titleCol,
	)
	require.NoError(t, err)
	sf, ok := expr.(*expression.ScalarFunction)
	require.True(t, ok)
	require.NoError(t, expression.SetFTSMysqlMatchAgainstModifier(sf, ast.FulltextSearchModifierBooleanMode))
	return sf
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
