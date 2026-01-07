// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::FlowNodeId, value::column::Columns};
use reifydb_engine::StandardColumnEvaluator;
use reifydb_type::RowNumber;

use crate::transaction::FlowTransaction;

mod apply;
pub mod context;
mod distinct;
mod extend;
mod ffi;
mod filter;
pub mod info;
pub mod join;
mod map;
mod merge;
mod scan;
mod sink;
mod sort;
pub mod stateful;
mod take;
pub mod transform;
mod window;

pub use apply::ApplyOperator;
#[allow(unused_imports)]
pub use context::{OperatorContextEntry, OperatorContextGuard, capture_operator_chain};
pub use distinct::DistinctOperator;
pub use extend::ExtendOperator;
pub use ffi::FFIOperator;
pub use filter::FilterOperator;
pub use info::OperatorInfo;
pub use join::JoinOperator;
pub use map::MapOperator;
pub use merge::MergeOperator;
use reifydb_sdk::FlowChange;
pub use scan::{PrimitiveFlowOperator, PrimitiveTableOperator, PrimitiveViewOperator};
pub use sink::{SinkSubscriptionOperator, SinkViewOperator};
pub use sort::SortOperator;
pub use take::TakeOperator;
pub use window::WindowOperator;

pub trait Operator: Send + Sync {
	fn id(&self) -> FlowNodeId; // FIXME replace by operator id

	fn apply(
		&self,
		txn: &mut FlowTransaction,
		change: FlowChange,
		evaluator: &StandardColumnEvaluator,
	) -> reifydb_type::Result<FlowChange>;

	fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> reifydb_type::Result<Columns>;
}

pub type BoxedOperator = Box<dyn Operator>;

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
		change: FlowChange,
		evaluator: &StandardColumnEvaluator,
	) -> reifydb_type::Result<FlowChange> {
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

impl OperatorInfo for Operators {
	fn operator_name(&self) -> &'static str {
		match self {
			Operators::SourceTable(_) => "SourceTable",
			Operators::SourceView(_) => "SourceView",
			Operators::SourceFlow(_) => "SourceFlow",
			Operators::Filter(_) => "Filter",
			Operators::Map(_) => "Map",
			Operators::Extend(_) => "Extend",
			Operators::Join(_) => "Join",
			Operators::Sort(_) => "Sort",
			Operators::Take(_) => "Take",
			Operators::Distinct(_) => "Distinct",
			Operators::Merge(_) => "Merge",
			Operators::Apply(_) => "Apply",
			Operators::SinkView(_) => "SinkView",
			Operators::SinkSubscription(_) => "SinkSubscription",
			Operators::Window(_) => "Window",
		}
	}

	fn operator_id(&self) -> FlowNodeId {
		match self {
			Operators::SourceTable(op) => op.id(),
			Operators::SourceView(op) => op.id(),
			Operators::SourceFlow(op) => op.id(),
			Operators::Filter(op) => op.id(),
			Operators::Map(op) => op.id(),
			Operators::Extend(op) => op.id(),
			Operators::Join(op) => op.id(),
			Operators::Sort(op) => op.id(),
			Operators::Take(op) => op.id(),
			Operators::Distinct(op) => op.id(),
			Operators::Merge(op) => op.id(),
			Operators::Apply(op) => op.id(),
			Operators::SinkView(op) => op.id(),
			Operators::SinkSubscription(op) => op.id(),
			Operators::Window(op) => op.id(),
		}
	}
}
