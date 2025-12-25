use async_trait::async_trait;
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
mod merge;
mod primitive;
mod sink;
mod sort;
pub mod stateful;
mod take;
pub mod transform;
mod window;

pub use apply::ApplyOperator;
pub use distinct::DistinctOperator;
pub use extend::ExtendOperator;
pub use ffi::FFIOperator;
pub use filter::FilterOperator;
pub use join::JoinOperator;
pub use map::MapOperator;
pub use merge::MergeOperator;
pub use primitive::{PrimitiveFlowOperator, PrimitiveTableOperator, PrimitiveViewOperator};
use reifydb_flow_operator_sdk::FlowChange;
pub use sink::SinkViewOperator;
pub use sort::SortOperator;
pub use take::TakeOperator;
pub use transform::registry::TransformOperatorRegistry;
pub use window::WindowOperator;

#[async_trait]
pub trait Operator: Send + Sync {
	fn id(&self) -> FlowNodeId; // FIXME replace by operator id

	async fn apply(
		&self,
		txn: &mut FlowTransaction,
		change: FlowChange,
		evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange>;

	async fn get_rows(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> crate::Result<Vec<Option<Row>>>;
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
		evaluator: &StandardRowEvaluator,
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

	async fn get_rows(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> crate::Result<Vec<Option<Row>>> {
		match self {
			Operators::Filter(op) => op.get_rows(txn, rows).await,
			Operators::Map(op) => op.get_rows(txn, rows).await,
			Operators::Extend(op) => op.get_rows(txn, rows).await,
			Operators::Join(op) => op.get_rows(txn, rows).await,
			Operators::Sort(op) => op.get_rows(txn, rows).await,
			Operators::Take(op) => op.get_rows(txn, rows).await,
			Operators::Distinct(op) => op.get_rows(txn, rows).await,
			Operators::Merge(op) => op.get_rows(txn, rows).await,
			Operators::Apply(op) => op.get_rows(txn, rows).await,
			Operators::SinkView(op) => op.get_rows(txn, rows).await,
			Operators::Window(op) => op.get_rows(txn, rows).await,
			Operators::SourceTable(op) => op.get_rows(txn, rows).await,
			Operators::SourceView(op) => op.get_rows(txn, rows).await,
			Operators::SourceFlow(op) => op.get_rows(txn, rows).await,
		}
	}
}
