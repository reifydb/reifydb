pub mod filter;
pub mod map;

pub use filter::FilterOperator;
pub use map::MapOperator;

use crate::flow::{change::Change, state::StateStore};

pub trait Operator {
	fn apply(
		&self,
		ctx: &OperatorContext,
		change: Change,
	) -> crate::Result<Change>;
}

pub struct OperatorContext {
	pub state: StateStore,
}

impl OperatorContext {
	pub fn new() -> Self {
		Self {
			state: StateStore::new(),
		}
	}

	pub fn with_state(state: StateStore) -> Self {
		Self {
			state,
		}
	}
}
