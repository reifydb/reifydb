// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::flow::FlowNodeId, value::column::columns::Columns};
use reifydb_engine::evaluate::column::StandardColumnEvaluator;
use reifydb_type::value::row_number::RowNumber;

use crate::transaction::FlowTransaction;

pub mod apply;
pub mod distinct;
pub mod extend;
#[cfg(reifydb_target = "native")]
pub mod ffi;
pub mod filter;
pub mod join;
pub mod map;
pub mod merge;
pub mod scan;
pub mod sink;
pub mod sort;
pub mod stateful;
pub mod take;
pub mod window;

use apply::ApplyOperator;
use distinct::DistinctOperator;
use extend::ExtendOperator;
use filter::FilterOperator;
use join::operator::JoinOperator;
use map::MapOperator;
use merge::MergeOperator;
use reifydb_core::interface::change::Change;
use scan::{flow::PrimitiveFlowOperator, table::PrimitiveTableOperator, view::PrimitiveViewOperator};
use sink::{subscription::SinkSubscriptionOperator, view::SinkViewOperator};
use sort::SortOperator;
use take::TakeOperator;
use window::WindowOperator;

pub trait Operator: Send + Sync {
	fn id(&self) -> FlowNodeId;

	fn apply(
		&self,
		txn: &mut FlowTransaction,
		change: Change,
		evaluator: &StandardColumnEvaluator,
	) -> reifydb_type::Result<Change>;

	fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> reifydb_type::Result<Columns>;
}

pub type BoxedOperator = Box<dyn Operator + Send + Sync>;

pub enum Operators {
	SourceTable(PrimitiveTableOperator),
	SourceView(PrimitiveViewOperator),
	SourceFlow(PrimitiveFlowOperator),
	Filter(FilterOperator),
	Map(MapOperator),
	Extend(ExtendOperator),
	Join(JoinOperator),
	Sort(SortOperator),
	Take(TakeOperator),
	Distinct(DistinctOperator),
	Merge(MergeOperator),
	Apply(ApplyOperator),
	SinkView(SinkViewOperator),
	SinkSubscription(SinkSubscriptionOperator),
	Window(WindowOperator),
}

impl Operators {
	pub fn apply(
		&self,
		txn: &mut FlowTransaction,
		change: Change,
		evaluator: &StandardColumnEvaluator,
	) -> reifydb_type::Result<Change> {
		match self {
			Operators::Filter(op) => op.apply(txn, change, evaluator),
			Operators::Map(op) => op.apply(txn, change, evaluator),
			Operators::Extend(op) => op.apply(txn, change, evaluator),
			Operators::Join(op) => op.apply(txn, change, evaluator),
			Operators::Sort(op) => op.apply(txn, change, evaluator),
			Operators::Take(op) => op.apply(txn, change, evaluator),
			Operators::Distinct(op) => op.apply(txn, change, evaluator),
			Operators::Merge(op) => op.apply(txn, change, evaluator),
			Operators::Apply(op) => op.apply(txn, change, evaluator),
			Operators::SinkView(op) => op.apply(txn, change, evaluator),
			Operators::SinkSubscription(op) => op.apply(txn, change, evaluator),
			Operators::Window(op) => op.apply(txn, change, evaluator),
			Operators::SourceTable(op) => op.apply(txn, change, evaluator),
			Operators::SourceView(op) => op.apply(txn, change, evaluator),
			Operators::SourceFlow(op) => op.apply(txn, change, evaluator),
		}
	}

	fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> reifydb_type::Result<Columns> {
		match self {
			Operators::Filter(op) => op.pull(txn, rows),
			Operators::Map(op) => op.pull(txn, rows),
			Operators::Extend(op) => op.pull(txn, rows),
			Operators::Join(op) => op.pull(txn, rows),
			Operators::Sort(op) => op.pull(txn, rows),
			Operators::Take(op) => op.pull(txn, rows),
			Operators::Distinct(op) => op.pull(txn, rows),
			Operators::Merge(op) => op.pull(txn, rows),
			Operators::Apply(op) => op.pull(txn, rows),
			Operators::SinkView(op) => op.pull(txn, rows),
			Operators::SinkSubscription(op) => op.pull(txn, rows),
			Operators::Window(op) => op.pull(txn, rows),
			Operators::SourceTable(op) => op.pull(txn, rows),
			Operators::SourceView(op) => op.pull(txn, rows),
			Operators::SourceFlow(op) => op.pull(txn, rows),
		}
	}
}
