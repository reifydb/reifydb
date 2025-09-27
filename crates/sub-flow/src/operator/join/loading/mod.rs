use reifydb_core::{flow::FlowDiff, interface::Transaction, value::row::Row};
use reifydb_engine::StandardCommandTransaction;
use reifydb_hash::Hash128;

use crate::operator::join::{JoinState, operator::JoinOperator};

mod eager;
mod lazy;

pub(crate) use eager::EagerLoading;
pub(crate) use lazy::LazyLoading;

#[derive(Debug, Clone)]
pub(crate) enum LoadingStrategy {
	Eager(EagerLoading),
	Lazy(LazyLoading),
}

impl LoadingStrategy {
	pub(crate) fn new_eager() -> Self {
		LoadingStrategy::Eager(EagerLoading::new())
	}

	pub(crate) fn new_lazy() -> Self {
		LoadingStrategy::Lazy(LazyLoading::new())
	}

	pub(crate) fn handle_left_insert<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		post: &Row,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		match self {
			LoadingStrategy::Eager(s) => s.handle_left_insert(txn, post, key_hash, state, operator),
			LoadingStrategy::Lazy(s) => s.handle_left_insert(txn, post, key_hash, state, operator),
		}
	}

	pub(crate) fn handle_right_insert<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		post: &Row,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		match self {
			LoadingStrategy::Eager(s) => s.handle_right_insert(txn, post, key_hash, state, operator),
			LoadingStrategy::Lazy(s) => s.handle_right_insert(txn, post, key_hash, state, operator),
		}
	}

	pub(crate) fn handle_left_remove<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		pre: &Row,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		match self {
			LoadingStrategy::Eager(s) => s.handle_left_remove(txn, pre, key_hash, state, operator),
			LoadingStrategy::Lazy(s) => s.handle_left_remove(txn, pre, key_hash, state, operator),
		}
	}

	pub(crate) fn handle_right_remove<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		pre: &Row,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		match self {
			LoadingStrategy::Eager(s) => s.handle_right_remove(txn, pre, key_hash, state, operator),
			LoadingStrategy::Lazy(s) => s.handle_right_remove(txn, pre, key_hash, state, operator),
		}
	}

	pub(crate) fn handle_left_update<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		pre: &Row,
		post: &Row,
		old_key: Option<Hash128>,
		new_key: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		match self {
			LoadingStrategy::Eager(s) => {
				s.handle_left_update(txn, pre, post, old_key, new_key, state, operator)
			}
			LoadingStrategy::Lazy(s) => {
				s.handle_left_update(txn, pre, post, old_key, new_key, state, operator)
			}
		}
	}

	pub(crate) fn handle_right_update<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		pre: &Row,
		post: &Row,
		old_key: Option<Hash128>,
		new_key: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		match self {
			LoadingStrategy::Eager(s) => {
				s.handle_right_update(txn, pre, post, old_key, new_key, state, operator)
			}
			LoadingStrategy::Lazy(s) => {
				s.handle_right_update(txn, pre, post, old_key, new_key, state, operator)
			}
		}
	}
}
