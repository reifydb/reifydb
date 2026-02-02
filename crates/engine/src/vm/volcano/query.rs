// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::resolved::ResolvedPrimitive,
	value::{
		batch::lazy::LazyBatch,
		column::{columns::Columns, headers::ColumnHeaders},
	},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::params::Params;

use crate::vm::{
	services::Services,
	stack::SymbolTable,
	volcano::{
		aggregate::AggregateNode,
		environment::EnvironmentNode,
		extend::{ExtendNode, ExtendWithoutInputNode},
		filter::FilterNode,
		generator::GeneratorNode,
		inline::InlineDataNode,
		join::{inner::InnerJoinNode, left::LeftJoinNode, natural::NaturalJoinNode},
		map::{MapNode, MapWithoutInputNode},
		row_lookup::{RowListLookupNode, RowPointLookupNode, RowRangeScanNode},
		scalarize::ScalarizeNode,
		scan::{
			dictionary::DictionaryScanNode, index::IndexScanNode, ringbuffer::RingBufferScan,
			table::TableScanNode, view::ViewScanNode, vtable::VirtualScanNode,
		},
		sort::SortNode,
		take::TakeNode,
		top_k::TopKNode,
		variable::VariableNode,
	},
};

/// Unified trait for query execution nodes following the volcano iterator pattern
pub(crate) trait QueryNode {
	/// Initialize the operator with execution context
	/// Called once before iteration begins
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> crate::Result<()>;

	/// Get the next batch of results (volcano iterator pattern)
	/// Returns None when exhausted
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> crate::Result<Option<Columns>>;

	/// Get the next batch as a LazyBatch for deferred materialization
	/// Returns None if this node doesn't support lazy evaluation or is exhausted
	/// Default implementation returns None (falls back to materialized evaluation)
	fn next_lazy<'a>(
		&mut self,
		_rx: &mut Transaction<'a>,
		_ctx: &mut QueryContext,
	) -> crate::Result<Option<LazyBatch>> {
		Ok(None)
	}

	/// Get the headers of columns this node produces
	fn headers(&self) -> Option<ColumnHeaders>;
}

#[derive(Clone)]
pub struct QueryContext {
	pub services: Arc<Services>,
	pub source: Option<ResolvedPrimitive>,
	pub batch_size: u64,
	pub params: Params,
	pub stack: SymbolTable,
}

pub(crate) enum QueryPlan {
	Aggregate(AggregateNode),
	DictionaryScan(DictionaryScanNode),
	Filter(FilterNode),
	IndexScan(IndexScanNode),
	InlineData(InlineDataNode),
	InnerJoin(InnerJoinNode),
	LeftJoin(LeftJoinNode),
	NaturalJoin(NaturalJoinNode),
	Map(MapNode),
	MapWithoutInput(MapWithoutInputNode),
	Extend(ExtendNode),
	ExtendWithoutInput(ExtendWithoutInputNode),
	Sort(SortNode),
	TableScan(TableScanNode),
	Take(TakeNode),
	TopK(TopKNode),
	ViewScan(ViewScanNode),
	Variable(VariableNode),
	Environment(EnvironmentNode),
	VirtualScan(VirtualScanNode),
	RingBufferScan(RingBufferScan),
	Generator(GeneratorNode),
	Scalarize(ScalarizeNode),
	// Row-number optimized access
	RowPointLookup(RowPointLookupNode),
	RowListLookup(RowListLookupNode),
	RowRangeScan(RowRangeScanNode),
}

// Implement QueryNode for Box<QueryPlan> to allow chaining

impl QueryNode for Box<QueryPlan> {
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> crate::Result<()> {
		(**self).initialize(rx, ctx)
	}

	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> crate::Result<Option<Columns>> {
		(**self).next(rx, ctx)
	}

	fn next_lazy<'a>(
		&mut self,
		rx: &mut Transaction<'a>,
		ctx: &mut QueryContext,
	) -> crate::Result<Option<LazyBatch>> {
		(**self).next_lazy(rx, ctx)
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		(**self).headers()
	}
}

impl QueryNode for QueryPlan {
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> crate::Result<()> {
		match self {
			QueryPlan::Aggregate(node) => node.initialize(rx, ctx),
			QueryPlan::DictionaryScan(node) => node.initialize(rx, ctx),
			QueryPlan::Filter(node) => node.initialize(rx, ctx),
			QueryPlan::IndexScan(node) => node.initialize(rx, ctx),
			QueryPlan::InlineData(node) => node.initialize(rx, ctx),
			QueryPlan::InnerJoin(node) => node.initialize(rx, ctx),
			QueryPlan::LeftJoin(node) => node.initialize(rx, ctx),
			QueryPlan::NaturalJoin(node) => node.initialize(rx, ctx),
			QueryPlan::Map(node) => node.initialize(rx, ctx),
			QueryPlan::MapWithoutInput(node) => node.initialize(rx, ctx),
			QueryPlan::Extend(node) => node.initialize(rx, ctx),
			QueryPlan::ExtendWithoutInput(node) => node.initialize(rx, ctx),
			QueryPlan::Sort(node) => node.initialize(rx, ctx),
			QueryPlan::TableScan(node) => node.initialize(rx, ctx),
			QueryPlan::Take(node) => node.initialize(rx, ctx),
			QueryPlan::TopK(node) => node.initialize(rx, ctx),
			QueryPlan::ViewScan(node) => node.initialize(rx, ctx),
			QueryPlan::Variable(node) => node.initialize(rx, ctx),
			QueryPlan::Environment(node) => node.initialize(rx, ctx),
			QueryPlan::VirtualScan(node) => node.initialize(rx, ctx),
			QueryPlan::RingBufferScan(node) => node.initialize(rx, ctx),
			QueryPlan::Generator(node) => node.initialize(rx, ctx),
			QueryPlan::Scalarize(node) => node.initialize(rx, ctx),
			QueryPlan::RowPointLookup(node) => node.initialize(rx, ctx),
			QueryPlan::RowListLookup(node) => node.initialize(rx, ctx),
			QueryPlan::RowRangeScan(node) => node.initialize(rx, ctx),
		}
	}

	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> crate::Result<Option<Columns>> {
		match self {
			QueryPlan::Aggregate(node) => node.next(rx, ctx),
			QueryPlan::DictionaryScan(node) => node.next(rx, ctx),
			QueryPlan::Filter(node) => node.next(rx, ctx),
			QueryPlan::IndexScan(node) => node.next(rx, ctx),
			QueryPlan::InlineData(node) => node.next(rx, ctx),
			QueryPlan::InnerJoin(node) => node.next(rx, ctx),
			QueryPlan::LeftJoin(node) => node.next(rx, ctx),
			QueryPlan::NaturalJoin(node) => node.next(rx, ctx),
			QueryPlan::Map(node) => node.next(rx, ctx),
			QueryPlan::MapWithoutInput(node) => node.next(rx, ctx),
			QueryPlan::Extend(node) => node.next(rx, ctx),
			QueryPlan::ExtendWithoutInput(node) => node.next(rx, ctx),
			QueryPlan::Sort(node) => node.next(rx, ctx),
			QueryPlan::TableScan(node) => node.next(rx, ctx),
			QueryPlan::Take(node) => node.next(rx, ctx),
			QueryPlan::TopK(node) => node.next(rx, ctx),
			QueryPlan::ViewScan(node) => node.next(rx, ctx),
			QueryPlan::Variable(node) => node.next(rx, ctx),
			QueryPlan::Environment(node) => node.next(rx, ctx),
			QueryPlan::VirtualScan(node) => node.next(rx, ctx),
			QueryPlan::RingBufferScan(node) => node.next(rx, ctx),
			QueryPlan::Generator(node) => node.next(rx, ctx),
			QueryPlan::Scalarize(node) => node.next(rx, ctx),
			QueryPlan::RowPointLookup(node) => node.next(rx, ctx),
			QueryPlan::RowListLookup(node) => node.next(rx, ctx),
			QueryPlan::RowRangeScan(node) => node.next(rx, ctx),
		}
	}

	fn next_lazy<'a>(
		&mut self,
		rx: &mut Transaction<'a>,
		ctx: &mut QueryContext,
	) -> crate::Result<Option<LazyBatch>> {
		match self {
			// Only TableScan supports lazy evaluation for now
			QueryPlan::TableScan(node) => node.next_lazy(rx, ctx),
			// All other nodes return None (use default materialized path)
			_ => Ok(None),
		}
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		match self {
			QueryPlan::Aggregate(node) => node.headers(),
			QueryPlan::DictionaryScan(node) => node.headers(),
			QueryPlan::Filter(node) => node.headers(),
			QueryPlan::IndexScan(node) => node.headers(),
			QueryPlan::InlineData(node) => node.headers(),
			QueryPlan::InnerJoin(node) => node.headers(),
			QueryPlan::LeftJoin(node) => node.headers(),
			QueryPlan::NaturalJoin(node) => node.headers(),
			QueryPlan::Map(node) => node.headers(),
			QueryPlan::MapWithoutInput(node) => node.headers(),
			QueryPlan::Extend(node) => node.headers(),
			QueryPlan::ExtendWithoutInput(node) => node.headers(),
			QueryPlan::Sort(node) => node.headers(),
			QueryPlan::TableScan(node) => node.headers(),
			QueryPlan::Take(node) => node.headers(),
			QueryPlan::TopK(node) => node.headers(),
			QueryPlan::ViewScan(node) => node.headers(),
			QueryPlan::Variable(node) => node.headers(),
			QueryPlan::Environment(node) => node.headers(),
			QueryPlan::VirtualScan(node) => node.headers(),
			QueryPlan::RingBufferScan(node) => node.headers(),
			QueryPlan::Generator(node) => node.headers(),
			QueryPlan::Scalarize(node) => node.headers(),
			QueryPlan::RowPointLookup(node) => node.headers(),
			QueryPlan::RowListLookup(node) => node.headers(),
			QueryPlan::RowRangeScan(node) => node.headers(),
		}
	}
}
