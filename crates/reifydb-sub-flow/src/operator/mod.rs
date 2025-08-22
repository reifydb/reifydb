mod aggregate;
mod filter;
mod map;

use std::marker::PhantomData;

pub use aggregate::AggregateOperator;
pub use filter::FilterOperator;
pub use map::MapOperator;
use reifydb_core::{
    interface::{
		EvaluationContext, Evaluator, Transaction,
		expression::Expression,
	},
    transaction::StandardCommandTransaction,
    value::columnar::Column,
};

use crate::core::Change;

pub trait Operator<E: Evaluator>: Send + Sync + 'static {
	fn apply<T: Transaction>(
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
	pub fn apply<T: Transaction>(
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

pub struct OperatorContext<'a, E: Evaluator, T: Transaction> {
	pub evaluator: &'a E,
	pub txn: &'a mut StandardCommandTransaction<T>,
}

impl<'a, E: Evaluator, T: Transaction> OperatorContext<'a, E, T> {
	pub fn new(
        evaluator: &'a E,
        txn: &'a mut StandardCommandTransaction<T>,
	) -> Self {
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
