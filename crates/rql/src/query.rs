// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::nodes::{
	AggregateNode, ApplyNode, DictionaryScanNode, DistinctNode, EnvironmentNode, ExtendNode, FilterNode,
	FlowScanNode, GeneratorNode, IndexScanNode, InlineDataNode, JoinInnerNode, JoinLeftNode, JoinNaturalNode,
	MapNode, MergeNode, PatchNode, PhysicalPlan, RingBufferScanNode, RowListLookupNode, RowPointLookupNode,
	RowRangeScanNode, ScalarizeNode, SortNode, TableScanNode, TableVirtualScanNode, TakeNode, VariableNode,
	ViewScanNode, WindowNode,
};

#[derive(Debug, Clone)]
pub enum QueryPlan {
	TableScan(TableScanNode),
	TableVirtualScan(TableVirtualScanNode),
	ViewScan(ViewScanNode),
	RingBufferScan(RingBufferScanNode),
	FlowScan(FlowScanNode),
	DictionaryScan(DictionaryScanNode),
	IndexScan(IndexScanNode),

	/// O(1) point lookup by row number: `filter rownum == N`
	RowPointLookup(RowPointLookupNode),
	/// O(k) list lookup by row numbers: `filter rownum in [a, b, c]`
	RowListLookup(RowListLookupNode),
	/// Range scan by row numbers: `filter rownum between X and Y`
	RowRangeScan(RowRangeScanNode),

	Aggregate(AggregateNode),
	Distinct(DistinctNode),
	Filter(FilterNode),
	JoinInner(JoinInnerNode),
	JoinLeft(JoinLeftNode),
	JoinNatural(JoinNaturalNode),
	Merge(MergeNode),
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

impl From<QueryPlan> for PhysicalPlan {
	fn from(plan: QueryPlan) -> Self {
		match plan {
			QueryPlan::TableScan(node) => PhysicalPlan::TableScan(node),
			QueryPlan::TableVirtualScan(node) => PhysicalPlan::TableVirtualScan(node),
			QueryPlan::ViewScan(node) => PhysicalPlan::ViewScan(node),
			QueryPlan::RingBufferScan(node) => PhysicalPlan::RingBufferScan(node),
			QueryPlan::FlowScan(node) => PhysicalPlan::FlowScan(node),
			QueryPlan::DictionaryScan(node) => PhysicalPlan::DictionaryScan(node),
			QueryPlan::IndexScan(node) => PhysicalPlan::IndexScan(node),
			QueryPlan::RowPointLookup(node) => PhysicalPlan::RowPointLookup(node),
			QueryPlan::RowListLookup(node) => PhysicalPlan::RowListLookup(node),
			QueryPlan::RowRangeScan(node) => PhysicalPlan::RowRangeScan(node),
			QueryPlan::Aggregate(node) => PhysicalPlan::Aggregate(node),
			QueryPlan::Distinct(node) => PhysicalPlan::Distinct(node),
			QueryPlan::Filter(node) => PhysicalPlan::Filter(node),
			QueryPlan::JoinInner(node) => PhysicalPlan::JoinInner(node),
			QueryPlan::JoinLeft(node) => PhysicalPlan::JoinLeft(node),
			QueryPlan::JoinNatural(node) => PhysicalPlan::JoinNatural(node),
			QueryPlan::Merge(node) => PhysicalPlan::Merge(node),
			QueryPlan::Take(node) => PhysicalPlan::Take(node),
			QueryPlan::Sort(node) => PhysicalPlan::Sort(node),
			QueryPlan::Map(node) => PhysicalPlan::Map(node),
			QueryPlan::Extend(node) => PhysicalPlan::Extend(node),
			QueryPlan::Patch(node) => PhysicalPlan::Patch(node),
			QueryPlan::Apply(node) => PhysicalPlan::Apply(node),
			QueryPlan::InlineData(node) => PhysicalPlan::InlineData(node),
			QueryPlan::Generator(node) => PhysicalPlan::Generator(node),
			QueryPlan::Window(node) => PhysicalPlan::Window(node),
			QueryPlan::Variable(node) => PhysicalPlan::Variable(node),
			QueryPlan::Environment(node) => PhysicalPlan::Environment(node),
			QueryPlan::Scalarize(node) => PhysicalPlan::Scalarize(node),
		}
	}
}
