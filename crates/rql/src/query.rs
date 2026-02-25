// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::nodes::{
	AggregateNode, AppendQueryNode, ApplyNode, AssertNode, DictionaryScanNode, DistinctNode, EnvironmentNode,
	ExtendNode, FilterNode, FlowScanNode, GeneratorNode, IndexScanNode, InlineDataNode, JoinInnerNode,
	JoinLeftNode, JoinNaturalNode, MapNode, PatchNode, RingBufferScanNode, RowListLookupNode, RowPointLookupNode,
	RowRangeScanNode, ScalarizeNode, SeriesScanNode, SortNode, TableScanNode, TableVirtualScanNode, TakeNode,
	VariableNode, ViewScanNode, WindowNode,
};

#[derive(Debug, Clone)]
pub enum QueryPlan {
	TableScan(TableScanNode),
	TableVirtualScan(TableVirtualScanNode),
	ViewScan(ViewScanNode),
	RingBufferScan(RingBufferScanNode),
	FlowScan(FlowScanNode),
	DictionaryScan(DictionaryScanNode),
	SeriesScan(SeriesScanNode),
	IndexScan(IndexScanNode),

	/// O(1) point lookup by row number: `filter rownum == N`
	RowPointLookup(RowPointLookupNode),
	/// O(k) list lookup by row numbers: `filter rownum in [a, b, c]`
	RowListLookup(RowListLookupNode),
	/// Range scan by row numbers: `filter rownum between X and Y`
	RowRangeScan(RowRangeScanNode),

	Aggregate(AggregateNode),
	Assert(AssertNode),
	Distinct(DistinctNode),
	Filter(FilterNode),
	JoinInner(JoinInnerNode),
	JoinLeft(JoinLeftNode),
	JoinNatural(JoinNaturalNode),
	Append(AppendQueryNode),
	Take(TakeNode),
	Sort(SortNode),
	Map(MapNode),
	Extend(ExtendNode),
	Patch(PatchNode),
	Apply(ApplyNode),
	InlineData(InlineDataNode),
	Generator(GeneratorNode),
	Window(WindowNode),

	Variable(VariableNode),
	Environment(EnvironmentNode),

	Scalarize(ScalarizeNode),
}
