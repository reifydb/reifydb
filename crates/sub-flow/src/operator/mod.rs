use reifydb_core::{flow::FlowChange, interface::Transaction};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator};

mod apply;
mod distinct;
mod extend;
mod filter;
mod join;
mod map;
mod sink;
mod sort;
mod take;
pub(crate) mod transform;
mod union;

pub use apply::ApplyOperator;
pub use distinct::DistinctOperator;
pub use extend::ExtendOperator;
pub use filter::FilterOperator;
pub use join::JoinOperator;
pub use map::MapOperator;
pub use sink::SinkViewOperator;
pub use sort::SortOperator;
pub use take::TakeOperator;
pub use union::UnionOperator;

pub trait Operator<T: Transaction>: Send + Sync {
	fn apply(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange>;
}

pub enum Operators<T: Transaction> {
	Filter(FilterOperator),
	Map(MapOperator),
	Extend(ExtendOperator),
	Join(JoinOperator),
	Sort(SortOperator),
	Take(TakeOperator),
	Distinct(DistinctOperator),
	Union(UnionOperator),
	Apply(ApplyOperator<T>),
	SinkView(SinkViewOperator),
}

impl<T: Transaction> Operators<T> {
	pub fn apply(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		let result = match self {
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
		};
		result
	}
}
