mod filter;
mod map;

pub use filter::FilterOperator;
pub use map::MapOperator;
use reifydb_core::{
	interface::{Evaluate, EvaluationContext, expression::Expression},
	value::columnar::Column,
};

use crate::core::Change;

pub trait Operator<E: Evaluate> {
	fn apply(
		&self,
		ctx: &OperatorContext<E>,
		change: Change,
	) -> crate::Result<Change>;
}

pub struct OperatorContext<'a, E: Evaluate> {
	// pub state: StateStore,
	pub evaluator: &'a E,
}

impl<'a, E: Evaluate> OperatorContext<'a, E> {
	pub fn new(evaluator: &'a E) -> Self {
		Self {
			// state: StateStore::new(),
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

	// pub fn with_state(state: StateStore) -> Self {
	// 	Self {
	// 		state,
	// 	}
	// }
}
