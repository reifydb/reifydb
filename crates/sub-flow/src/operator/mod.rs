// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::flow::FlowNodeId, value::column::columns::Columns};
use reifydb_type::{Result, value::row_number::RowNumber};

use crate::transaction::FlowTransaction;

pub mod append;
pub mod apply;
pub mod distinct;
pub mod extend;
#[cfg(reifydb_target = "native")]
pub mod ffi;
pub mod filter;
pub mod join;
pub mod map;
pub mod scan;
pub mod sink;
pub mod sort;
pub mod stateful;
pub mod take;
pub mod window;

use append::AppendOperator;
use apply::ApplyOperator;
use distinct::DistinctOperator;
use extend::ExtendOperator;
use filter::FilterOperator;
use join::operator::JoinOperator;
use map::MapOperator;
use reifydb_core::interface::change::Change;
use scan::{
	flow::PrimitiveFlowOperator, ringbuffer::PrimitiveRingBufferOperator, series::PrimitiveSeriesOperator,
	table::PrimitiveTableOperator, view::PrimitiveViewOperator,
};
use sink::{subscription::SinkSubscriptionOperator, view::SinkViewOperator};
use sort::SortOperator;
use take::TakeOperator;
use window::WindowOperator;

pub trait Operator: Send + Sync {
	fn id(&self) -> FlowNodeId;

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change>;

	fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> Result<Columns>;
}

pub type BoxedOperator = Box<dyn Operator + Send + Sync>;

pub enum Operators {
	SourceTable(PrimitiveTableOperator),
	SourceView(PrimitiveViewOperator),
	SourceFlow(PrimitiveFlowOperator),
	SourceRingBuffer(PrimitiveRingBufferOperator),
	SourceSeries(PrimitiveSeriesOperator),
	Filter(FilterOperator),
	Map(MapOperator),
	Extend(ExtendOperator),
	Join(JoinOperator),
	Sort(SortOperator),
	Take(TakeOperator),
	Distinct(DistinctOperator),
	Append(AppendOperator),
	Apply(ApplyOperator),
	SinkView(SinkViewOperator),
	SinkSubscription(SinkSubscriptionOperator),
	Window(WindowOperator),
}

impl Operators {
	pub fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		match self {
			Operators::Filter(op) => op.apply(txn, change),
			Operators::Map(op) => op.apply(txn, change),
			Operators::Extend(op) => op.apply(txn, change),
			Operators::Join(op) => op.apply(txn, change),
			Operators::Sort(op) => op.apply(txn, change),
			Operators::Take(op) => op.apply(txn, change),
			Operators::Distinct(op) => op.apply(txn, change),
			Operators::Append(op) => op.apply(txn, change),
			Operators::Apply(op) => op.apply(txn, change),
			Operators::SinkView(op) => op.apply(txn, change),
			Operators::SinkSubscription(op) => op.apply(txn, change),
			Operators::Window(op) => op.apply(txn, change),
			Operators::SourceTable(op) => op.apply(txn, change),
			Operators::SourceView(op) => op.apply(txn, change),
			Operators::SourceFlow(op) => op.apply(txn, change),
			Operators::SourceRingBuffer(op) => op.apply(txn, change),
			Operators::SourceSeries(op) => op.apply(txn, change),
		}
	}

	fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> Result<Columns> {
		match self {
			Operators::Filter(op) => op.pull(txn, rows),
			Operators::Map(op) => op.pull(txn, rows),
			Operators::Extend(op) => op.pull(txn, rows),
			Operators::Join(op) => op.pull(txn, rows),
			Operators::Sort(op) => op.pull(txn, rows),
			Operators::Take(op) => op.pull(txn, rows),
			Operators::Distinct(op) => op.pull(txn, rows),
			Operators::Append(op) => op.pull(txn, rows),
			Operators::Apply(op) => op.pull(txn, rows),
			Operators::SinkView(op) => op.pull(txn, rows),
			Operators::SinkSubscription(op) => op.pull(txn, rows),
			Operators::Window(op) => op.pull(txn, rows),
			Operators::SourceTable(op) => op.pull(txn, rows),
			Operators::SourceView(op) => op.pull(txn, rows),
			Operators::SourceFlow(op) => op.pull(txn, rows),
			Operators::SourceRingBuffer(op) => op.pull(txn, rows),
			Operators::SourceSeries(op) => op.pull(txn, rows),
		}
	}
}
