// SPDX-License-Identifier: Apache-2.0
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

impl QueryPlan {
	pub fn name(&self) -> &'static str {
		match self {
			QueryPlan::RemoteScan(_) => "remote_scan",
			QueryPlan::TableScan(_) => "table_scan",
			QueryPlan::TableVirtualScan(_) => "table_virtual_scan",
			QueryPlan::ViewScan(_) => "view_scan",
			QueryPlan::RingBufferScan(_) => "ring_buffer_scan",
			QueryPlan::DictionaryScan(_) => "dictionary_scan",
			QueryPlan::SeriesScan(_) => "series_scan",
			QueryPlan::IndexScan(_) => "index_scan",
			QueryPlan::RowPointLookup(_) => "row_point_lookup",
			QueryPlan::RowListLookup(_) => "row_list_lookup",
			QueryPlan::RowRangeScan(_) => "row_range_scan",
			QueryPlan::Aggregate(_) => "aggregate",
			QueryPlan::Assert(_) => "assert",
			QueryPlan::Distinct(_) => "distinct",
			QueryPlan::Filter(_) => "filter",
			QueryPlan::Gate(_) => "gate",
			QueryPlan::JoinInner(_) => "join",
			QueryPlan::JoinLeft(_) => "join",
			QueryPlan::JoinNatural(_) => "join",
			QueryPlan::Append(_) => "append",
			QueryPlan::Take(_) => "take",
			QueryPlan::Sort(_) => "sort",
			QueryPlan::Map(_) => "map",
			QueryPlan::Extend(_) => "extend",
			QueryPlan::Patch(_) => "patch",
			QueryPlan::Apply(_) => "apply",
			QueryPlan::InlineData(_) => "inline_data",
			QueryPlan::Generator(_) => "generator",
			QueryPlan::Window(_) => "window",
			QueryPlan::Variable(_) => "variable",
			QueryPlan::Environment(_) => "environment",
			QueryPlan::Scalarize(_) => "scalarize",
			QueryPlan::RunTests(_) => "run_tests",
			QueryPlan::CallFunction(_) => "call_function",
		}
	}
}
