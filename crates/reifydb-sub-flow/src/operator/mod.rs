mod aggregate;
mod distinct;
mod filter;
mod join;
mod map;
mod sort;
mod take;
mod union;
mod window;

use std::marker::PhantomData;

pub use aggregate::AggregateOperator;
pub use distinct::DistinctOperator;
pub use filter::FilterOperator;
pub use join::JoinOperator;
pub use map::MapOperator;
use reifydb_core::{
	flow::FlowChange,
	interface::{
		CommandTransaction, EvaluationContext, Evaluator,
		expression::Expression,
	},
	value::columnar::Column,
};
pub use sort::SortOperator;
pub use take::TakeOperator;
pub use union::UnionOperator;
pub use window::WindowOperator;

pub trait Operator<E: Evaluator>: Send + Sync + 'static {
	fn apply<T: CommandTransaction>(
		&self,
		ctx: &mut OperatorContext<E, T>,
		change: &FlowChange,
	) -> crate::Result<FlowChange>;
}

// Enum for dynamic dispatch of operators
pub enum OperatorEnum<E: Evaluator> {
	Filter(FilterOperator),
	Map(MapOperator),
	Aggregate(AggregateOperator),
	Join(JoinOperator),
	Sort(SortOperator),
	Take(TakeOperator),
	Distinct(DistinctOperator),
	Union(UnionOperator),
	Window(WindowOperator),
	_Phantom(PhantomData<E>),
}

impl<E: Evaluator> OperatorEnum<E> {
	pub fn apply<T: CommandTransaction>(
		&self,
		ctx: &mut OperatorContext<E, T>,
		change: &FlowChange,
	) -> crate::Result<FlowChange> {
		match self {
			OperatorEnum::Filter(op) => op.apply(ctx, change),
			OperatorEnum::Map(op) => op.apply(ctx, change),
			OperatorEnum::Aggregate(op) => op.apply(ctx, change),
			OperatorEnum::Join(op) => op.apply(ctx, change),
			OperatorEnum::Sort(op) => op.apply(ctx, change),
			OperatorEnum::Take(op) => op.apply(ctx, change),
			OperatorEnum::Distinct(op) => op.apply(ctx, change),
			OperatorEnum::Union(op) => op.apply(ctx, change),
			OperatorEnum::Window(op) => op.apply(ctx, change),
			OperatorEnum::_Phantom(_) => unreachable!(),
		}
	}
}

pub struct OperatorContext<'a, E: Evaluator, T: CommandTransaction> {
	pub evaluator: &'a E,
	pub txn: &'a mut T,
}

impl<'a, E: Evaluator, T: CommandTransaction> OperatorContext<'a, E, T> {
	pub fn new(evaluator: &'a E, txn: &'a mut T) -> Self {
		Self {
			evaluator,
			txn,
		}
	}

	pub fn evaluate(
		&self,
		ctx: &EvaluationContext,
		expr: &Expression,
	) -> crate::Result<Column> {
		self.evaluator.evaluate(ctx, expr)
	}
}
