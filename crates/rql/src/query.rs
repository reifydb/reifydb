// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use crate::nodes::{
	AggregateNode, AppendQueryNode, ApplyNode, AssertNode, CallFunctionNode, DictionaryScanNode, DistinctNode,
	EnvironmentNode, ExtendNode, FilterNode, GateNode, GeneratorNode, IndexScanNode, InlineDataNode, JoinInnerNode,
	JoinLeftNode, JoinNaturalNode, MapNode, PatchNode, RemoteScanNode, RingBufferScanNode, RowListLookupNode,
	RowPointLookupNode, RowRangeScanNode, RunTestsNode, ScalarizeNode, SeriesScanNode, SortNode, TableScanNode,
	TableVirtualScanNode, TakeNode, VariableNode, ViewScanNode, WindowNode,
};

#[derive(Debug, Clone)]
pub enum QueryPlan {
	RemoteScan(RemoteScanNode),
	TableScan(TableScanNode),
	TableVirtualScan(TableVirtualScanNode),
	ViewScan(ViewScanNode),
	RingBufferScan(RingBufferScanNode),
	DictionaryScan(DictionaryScanNode),
	SeriesScan(SeriesScanNode),
	IndexScan(IndexScanNode),

	RowPointLookup(RowPointLookupNode),

	RowListLookup(RowListLookupNode),

	RowRangeScan(RowRangeScanNode),

	Aggregate(AggregateNode),
	Assert(AssertNode),
	Distinct(DistinctNode),
	Filter(FilterNode),
	Gate(GateNode),
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

	RunTests(RunTestsNode),

	CallFunction(CallFunctionNode),
}
