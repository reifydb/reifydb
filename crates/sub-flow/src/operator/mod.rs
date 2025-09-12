use reifydb_core::flow::FlowChange;
use reifydb_engine::{StandardCommandTransaction, StandardEvaluator};

mod aggregate;
mod apply;
mod distinct;
mod extend;
mod filter;
mod join;
mod map;
mod sort;
pub(crate) mod stateful;
mod take;
mod union;

pub use aggregate::AggregateOperator;
pub use apply::ApplyOperator;
pub use distinct::DistinctOperator;
pub use extend::ExtendOperator;
pub use filter::FilterOperator;
pub use join::JoinOperator;
pub use map::{MapOperator, MapTerminalOperator};
use reifydb_core::interface::Transaction;
pub use sort::SortOperator;
pub use take::TakeOperator;
pub use union::UnionOperator;

pub trait Operator<T: Transaction>: Send + Sync {
	fn apply(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		change: &FlowChange,
		evaluator: &StandardEvaluator,
	) -> crate::Result<FlowChange>;
}

pub enum Operators<T: Transaction> {
	Filter(FilterOperator),
	Map(MapOperator),
	Extend(ExtendOperator),
	MapTerminal(MapTerminalOperator),
	Aggregate(AggregateOperator),
	Join(JoinOperator),
	Sort(SortOperator),
	Take(TakeOperator),
	Distinct(DistinctOperator),
	Union(UnionOperator),
	Apply(ApplyOperator<T>),
}

impl<T: Transaction> Operators<T> {
	pub fn apply(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		change: &FlowChange,
		evaluator: &StandardEvaluator,
	) -> crate::Result<FlowChange> {
		let result = match self {
			Operators::Filter(op) => {
				op.apply(txn, change, evaluator)
			}
			Operators::Map(op) => op.apply(txn, change, evaluator),
			Operators::Extend(op) => {
				op.apply(txn, change, evaluator)
			}
			Operators::MapTerminal(op) => {
				op.apply(txn, change, evaluator)
			}
			Operators::Aggregate(op) => {
				op.apply(txn, change, evaluator)
			}
			Operators::Join(op) => op.apply(txn, change, evaluator),
			Operators::Sort(op) => op.apply(txn, change, evaluator),
			Operators::Take(op) => op.apply(txn, change, evaluator),
			Operators::Distinct(op) => {
				op.apply(txn, change, evaluator)
			}
			Operators::Union(op) => {
				op.apply(txn, change, evaluator)
			}
			Operators::Apply(op) => {
				op.apply(txn, change, evaluator)
			}
		};
		result
	}
}
