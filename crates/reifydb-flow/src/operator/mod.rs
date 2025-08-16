mod filter;
mod map;

pub use filter::FilterOperator;
pub use map::MapOperator;
use reifydb_core::{
	interface::{EvaluationContext, Evaluator, expression::Expression},
	value::columnar::Column,
};

use crate::core::Change;

pub trait Operator<E: Evaluator>: Send + Sync + 'static {
	fn apply(
		&self,
		ctx: &OperatorContext<E>,
		change: &Change,
	) -> crate::Result<Change>;
}

pub struct OperatorContext<'a, E: Evaluator> {
	pub evaluator: &'a E,
}

impl<'a, E: Evaluator> OperatorContext<'a, E> {
	pub fn new(evaluator: &'a E) -> Self {
		Self {
			evaluator,
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
