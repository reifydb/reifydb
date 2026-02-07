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
		patch::PatchNode,
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

pub(crate) enum QueryOperator {
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
	Patch(PatchNode),
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

// Implement QueryNode for Box<QueryOperator> to allow chaining

impl QueryNode for Box<QueryOperator> {
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

impl QueryNode for QueryOperator {
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> crate::Result<()> {
		match self {
			QueryOperator::Aggregate(node) => node.initialize(rx, ctx),
			QueryOperator::DictionaryScan(node) => node.initialize(rx, ctx),
			QueryOperator::Filter(node) => node.initialize(rx, ctx),
			QueryOperator::IndexScan(node) => node.initialize(rx, ctx),
			QueryOperator::InlineData(node) => node.initialize(rx, ctx),
			QueryOperator::InnerJoin(node) => node.initialize(rx, ctx),
			QueryOperator::LeftJoin(node) => node.initialize(rx, ctx),
			QueryOperator::NaturalJoin(node) => node.initialize(rx, ctx),
			QueryOperator::Map(node) => node.initialize(rx, ctx),
			QueryOperator::MapWithoutInput(node) => node.initialize(rx, ctx),
			QueryOperator::Extend(node) => node.initialize(rx, ctx),
			QueryOperator::ExtendWithoutInput(node) => node.initialize(rx, ctx),
			QueryOperator::Patch(node) => node.initialize(rx, ctx),
			QueryOperator::Sort(node) => node.initialize(rx, ctx),
			QueryOperator::TableScan(node) => node.initialize(rx, ctx),
			QueryOperator::Take(node) => node.initialize(rx, ctx),
			QueryOperator::TopK(node) => node.initialize(rx, ctx),
			QueryOperator::ViewScan(node) => node.initialize(rx, ctx),
			QueryOperator::Variable(node) => node.initialize(rx, ctx),
			QueryOperator::Environment(node) => node.initialize(rx, ctx),
			QueryOperator::VirtualScan(node) => node.initialize(rx, ctx),
			QueryOperator::RingBufferScan(node) => node.initialize(rx, ctx),
			QueryOperator::Generator(node) => node.initialize(rx, ctx),
			QueryOperator::Scalarize(node) => node.initialize(rx, ctx),
			QueryOperator::RowPointLookup(node) => node.initialize(rx, ctx),
			QueryOperator::RowListLookup(node) => node.initialize(rx, ctx),
			QueryOperator::RowRangeScan(node) => node.initialize(rx, ctx),
		}
	}

	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> crate::Result<Option<Columns>> {
		match self {
			QueryOperator::Aggregate(node) => node.next(rx, ctx),
			QueryOperator::DictionaryScan(node) => node.next(rx, ctx),
			QueryOperator::Filter(node) => node.next(rx, ctx),
			QueryOperator::IndexScan(node) => node.next(rx, ctx),
			QueryOperator::InlineData(node) => node.next(rx, ctx),
			QueryOperator::InnerJoin(node) => node.next(rx, ctx),
			QueryOperator::LeftJoin(node) => node.next(rx, ctx),
			QueryOperator::NaturalJoin(node) => node.next(rx, ctx),
			QueryOperator::Map(node) => node.next(rx, ctx),
			QueryOperator::MapWithoutInput(node) => node.next(rx, ctx),
			QueryOperator::Extend(node) => node.next(rx, ctx),
			QueryOperator::ExtendWithoutInput(node) => node.next(rx, ctx),
			QueryOperator::Patch(node) => node.next(rx, ctx),
			QueryOperator::Sort(node) => node.next(rx, ctx),
			QueryOperator::TableScan(node) => node.next(rx, ctx),
			QueryOperator::Take(node) => node.next(rx, ctx),
			QueryOperator::TopK(node) => node.next(rx, ctx),
			QueryOperator::ViewScan(node) => node.next(rx, ctx),
			QueryOperator::Variable(node) => node.next(rx, ctx),
			QueryOperator::Environment(node) => node.next(rx, ctx),
			QueryOperator::VirtualScan(node) => node.next(rx, ctx),
			QueryOperator::RingBufferScan(node) => node.next(rx, ctx),
			QueryOperator::Generator(node) => node.next(rx, ctx),
			QueryOperator::Scalarize(node) => node.next(rx, ctx),
			QueryOperator::RowPointLookup(node) => node.next(rx, ctx),
			QueryOperator::RowListLookup(node) => node.next(rx, ctx),
			QueryOperator::RowRangeScan(node) => node.next(rx, ctx),
		}
	}

	fn next_lazy<'a>(
		&mut self,
		rx: &mut Transaction<'a>,
		ctx: &mut QueryContext,
	) -> crate::Result<Option<LazyBatch>> {
		match self {
			// Only TableScan supports lazy evaluation for now
			QueryOperator::TableScan(node) => node.next_lazy(rx, ctx),
			// All other nodes return None (use default materialized path)
			_ => Ok(None),
		}
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		match self {
			QueryOperator::Aggregate(node) => node.headers(),
			QueryOperator::DictionaryScan(node) => node.headers(),
			QueryOperator::Filter(node) => node.headers(),
			QueryOperator::IndexScan(node) => node.headers(),
			QueryOperator::InlineData(node) => node.headers(),
			QueryOperator::InnerJoin(node) => node.headers(),
			QueryOperator::LeftJoin(node) => node.headers(),
			QueryOperator::NaturalJoin(node) => node.headers(),
			QueryOperator::Map(node) => node.headers(),
			QueryOperator::MapWithoutInput(node) => node.headers(),
			QueryOperator::Extend(node) => node.headers(),
			QueryOperator::ExtendWithoutInput(node) => node.headers(),
			QueryOperator::Patch(node) => node.headers(),
			QueryOperator::Sort(node) => node.headers(),
			QueryOperator::TableScan(node) => node.headers(),
			QueryOperator::Take(node) => node.headers(),
			QueryOperator::TopK(node) => node.headers(),
			QueryOperator::ViewScan(node) => node.headers(),
			QueryOperator::Variable(node) => node.headers(),
			QueryOperator::Environment(node) => node.headers(),
			QueryOperator::VirtualScan(node) => node.headers(),
			QueryOperator::RingBufferScan(node) => node.headers(),
			QueryOperator::Generator(node) => node.headers(),
			QueryOperator::Scalarize(node) => node.headers(),
			QueryOperator::RowPointLookup(node) => node.headers(),
			QueryOperator::RowListLookup(node) => node.headers(),
			QueryOperator::RowRangeScan(node) => node.headers(),
		}
	}
}
