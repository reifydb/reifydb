mod aggregate;
mod filter;
mod map;

use std::marker::PhantomData;

use crate::core::Change;
pub use aggregate::AggregateOperator;
pub use filter::FilterOperator;
pub use map::MapOperator;
use reifydb_core::interface::CommandTransaction;
use reifydb_core::{
	interface::{
		expression::Expression, EvaluationContext, Evaluator,
		Transaction,
	},
	value::columnar::Column,
};

pub trait Operator<E: Evaluator>: Send + Sync + 'static {
	fn apply<T: CommandTransaction>(
		&self,
		ctx: &mut OperatorContext<E, T>,
		change: &Change,
	) -> crate::Result<Change>;
}

// Enum for dynamic dispatch of operators
pub enum OperatorEnum<E: Evaluator> {
	Filter(FilterOperator),
	Map(MapOperator),
	Aggregate(AggregateOperator),
	_Phantom(PhantomData<E>),
}

impl<E: Evaluator> OperatorEnum<E> {
	pub fn apply<T: CommandTransaction>(
		&self,
		ctx: &mut OperatorContext<E, T>,
		change: &Change,
	) -> crate::Result<Change> {
		match self {
			OperatorEnum::Filter(op) => op.apply(ctx, change),
			OperatorEnum::Map(op) => op.apply(ctx, change),
			OperatorEnum::Aggregate(op) => op.apply(ctx, change),
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
