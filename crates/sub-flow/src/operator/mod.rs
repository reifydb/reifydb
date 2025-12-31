// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use async_trait::async_trait;
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
pub use sink::SinkViewOperator;
pub use sort::SortOperator;
pub use take::TakeOperator;
pub use window::WindowOperator;

#[async_trait]
pub trait Operator: Send + Sync {
	fn id(&self) -> FlowNodeId; // FIXME replace by operator id

	async fn apply(
		&self,
		txn: &mut FlowTransaction,
		change: FlowChange,
		evaluator: &StandardColumnEvaluator,
	) -> crate::Result<FlowChange>;

	async fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> crate::Result<Columns>;
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
	Window(WindowOperator),
}

impl Operators {
	pub async fn apply(
		&self,
		txn: &mut FlowTransaction,
		change: FlowChange,
		evaluator: &StandardColumnEvaluator,
	) -> crate::Result<FlowChange> {
		match self {
			Operators::Filter(op) => op.apply(txn, change, evaluator).await,
			Operators::Map(op) => op.apply(txn, change, evaluator).await,
			Operators::Extend(op) => op.apply(txn, change, evaluator).await,
			Operators::Join(op) => op.apply(txn, change, evaluator).await,
			Operators::Sort(op) => op.apply(txn, change, evaluator).await,
			Operators::Take(op) => op.apply(txn, change, evaluator).await,
			Operators::Distinct(op) => op.apply(txn, change, evaluator).await,
			Operators::Merge(op) => op.apply(txn, change, evaluator).await,
			Operators::Apply(op) => op.apply(txn, change, evaluator).await,
			Operators::SinkView(op) => op.apply(txn, change, evaluator).await,
			Operators::Window(op) => op.apply(txn, change, evaluator).await,
			Operators::SourceTable(op) => op.apply(txn, change, evaluator).await,
			Operators::SourceView(op) => op.apply(txn, change, evaluator).await,
			Operators::SourceFlow(op) => op.apply(txn, change, evaluator).await,
		}
	}

	async fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> crate::Result<Columns> {
		match self {
			Operators::Filter(op) => op.pull(txn, rows).await,
			Operators::Map(op) => op.pull(txn, rows).await,
			Operators::Extend(op) => op.pull(txn, rows).await,
			Operators::Join(op) => op.pull(txn, rows).await,
			Operators::Sort(op) => op.pull(txn, rows).await,
			Operators::Take(op) => op.pull(txn, rows).await,
			Operators::Distinct(op) => op.pull(txn, rows).await,
			Operators::Merge(op) => op.pull(txn, rows).await,
			Operators::Apply(op) => op.pull(txn, rows).await,
			Operators::SinkView(op) => op.pull(txn, rows).await,
			Operators::Window(op) => op.pull(txn, rows).await,
			Operators::SourceTable(op) => op.pull(txn, rows).await,
			Operators::SourceView(op) => op.pull(txn, rows).await,
			Operators::SourceFlow(op) => op.pull(txn, rows).await,
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
			Operators::Window(op) => op.id(),
		}
	}
}
