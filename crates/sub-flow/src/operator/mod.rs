use reifydb_core::{Row, interface::FlowNodeId};
use reifydb_engine::StandardRowEvaluator;
use reifydb_type::RowNumber;

use crate::transaction::FlowTransaction;

mod apply;
mod distinct;
mod extend;
mod ffi;
mod filter;
pub mod join;
mod map;
mod sink;
mod sort;
mod source;
pub mod stateful;
mod take;
pub mod transform;
mod union;
mod window;

pub use apply::ApplyOperator;
pub use distinct::DistinctOperator;
pub use extend::ExtendOperator;
pub use ffi::FFIOperator;
pub use filter::FilterOperator;
pub use join::JoinOperator;
pub use map::MapOperator;
use reifydb_flow_operator_sdk::FlowChange;
pub use sink::SinkViewOperator;
pub use sort::SortOperator;
pub use source::{SourceTableOperator, SourceViewOperator};
pub use take::TakeOperator;
pub use transform::registry::TransformOperatorRegistry;
pub use union::UnionOperator;
pub use window::WindowOperator;

pub trait Operator: Send + Sync {
	fn id(&self) -> FlowNodeId; // FIXME replace by operator id

	fn apply(
		&self,
		txn: &mut FlowTransaction,
		change: FlowChange,
		evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange>;

	fn get_rows(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> crate::Result<Vec<Option<Row>>>;
}

pub type BoxedOperator = Box<dyn Operator>;

pub enum Operators {
	SourceTable(SourceTableOperator),
	SourceView(SourceViewOperator),
	Filter(FilterOperator),
	Map(MapOperator),
	Extend(ExtendOperator),
	Join(JoinOperator),
	Sort(SortOperator),
	Take(TakeOperator),
	Distinct(DistinctOperator),
	Union(UnionOperator),
	Apply(ApplyOperator),
	SinkView(SinkViewOperator),
	Window(WindowOperator),
}

impl Operators {
	pub fn apply(
		&self,
		txn: &mut FlowTransaction,
		change: FlowChange,
		evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		match self {
			Operators::Filter(op) => op.apply(txn, change, evaluator),
			Operators::Map(op) => op.apply(txn, change, evaluator),
			Operators::Extend(op) => op.apply(txn, change, evaluator),
			Operators::Join(op) => op.apply(txn, change, evaluator),
			Operators::Sort(op) => op.apply(txn, change, evaluator),
			Operators::Take(op) => op.apply(txn, change, evaluator),
			Operators::Distinct(op) => op.apply(txn, change, evaluator),
			Operators::Union(op) => op.apply(txn, change, evaluator),
			Operators::Apply(op) => op.apply(txn, change, evaluator),
			Operators::SinkView(op) => op.apply(txn, change, evaluator),
			Operators::Window(op) => op.apply(txn, change, evaluator),
			Operators::SourceTable(op) => op.apply(txn, change, evaluator),
			Operators::SourceView(op) => op.apply(txn, change, evaluator),
		}
	}

	fn get_rows(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> crate::Result<Vec<Option<Row>>> {
		match self {
			Operators::Filter(op) => op.get_rows(txn, rows),
			Operators::Map(op) => op.get_rows(txn, rows),
			Operators::Extend(op) => op.get_rows(txn, rows),
			Operators::Join(op) => op.get_rows(txn, rows),
			Operators::Sort(op) => op.get_rows(txn, rows),
			Operators::Take(op) => op.get_rows(txn, rows),
			Operators::Distinct(op) => op.get_rows(txn, rows),
			Operators::Union(op) => op.get_rows(txn, rows),
			Operators::Apply(op) => op.get_rows(txn, rows),
			Operators::SinkView(op) => op.get_rows(txn, rows),
			Operators::Window(op) => op.get_rows(txn, rows),
			Operators::SourceTable(op) => op.get_rows(txn, rows),
			Operators::SourceView(op) => op.get_rows(txn, rows),
		}
	}
}
