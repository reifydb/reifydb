use reifydb_core::{CommitVersion, JoinType, Row, interface::Transaction};
use reifydb_engine::{StandardCommandTransaction, execute::Executor};
use reifydb_hash::Hash128;
use reifydb_rql::query::QueryString;

use crate::{
	flow::FlowDiff,
	operator::join::{JoinSide, JoinState, operator::JoinOperator},
};

mod eager;
mod inner_eager;
mod inner_lazy;
mod lazy;
mod left_eager;
mod left_lazy;

use crate::operator::join::strategy::{
	inner_eager::InnerEagerJoin, inner_lazy::InnerLazyJoin, left_eager::LeftEagerJoin, left_lazy::LeftLazyJoin,
};

pub(crate) enum JoinStrategy {
	LeftEager(LeftEagerJoin),
	LeftLazy(LeftLazyJoin),
	InnerEager(InnerEagerJoin),
	InnerLazy(InnerLazyJoin),
}

impl JoinStrategy {
	pub(crate) fn from(
		storage_strategy: reifydb_core::JoinStrategy,
		join_type: JoinType,
		right_query: QueryString,
		executor: Executor,
	) -> Self {
		match (storage_strategy, join_type) {
			(reifydb_core::JoinStrategy::EagerLoading, JoinType::Left) => {
				JoinStrategy::LeftEager(LeftEagerJoin)
			}
			(reifydb_core::JoinStrategy::LazyLoading, JoinType::Left) => {
				JoinStrategy::LeftLazy(LeftLazyJoin {
					query: right_query,
					executor,
				})
			}
			(reifydb_core::JoinStrategy::EagerLoading, JoinType::Inner) => {
				JoinStrategy::InnerEager(InnerEagerJoin)
			}
			(reifydb_core::JoinStrategy::LazyLoading, JoinType::Inner) => {
				JoinStrategy::InnerLazy(InnerLazyJoin {
					query: right_query,
					executor,
				})
			}
		}
	}

	/// Handle insert operations
	pub(crate) fn handle_insert<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		post: &Row,
		side: JoinSide,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
		version: CommitVersion,
	) -> crate::Result<Vec<FlowDiff>> {
		match self {
			JoinStrategy::LeftEager(join_type) => {
				join_type.handle_insert(txn, post, side, key_hash, state, operator)
			}
			JoinStrategy::LeftLazy(join_type) => {
				join_type.handle_insert(txn, post, side, key_hash, state, operator, version)
			}
			JoinStrategy::InnerEager(join_type) => {
				join_type.handle_insert(txn, post, side, key_hash, state, operator)
			}
			JoinStrategy::InnerLazy(join_type) => {
				join_type.handle_insert(txn, post, side, key_hash, state, operator, version)
			}
		}
	}

	/// Handle remove operations
	pub(crate) fn handle_remove<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		pre: &Row,
		side: JoinSide,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
		version: CommitVersion,
	) -> crate::Result<Vec<FlowDiff>> {
		match self {
			JoinStrategy::LeftEager(join_type) => {
				join_type.handle_remove(txn, pre, side, key_hash, state, operator)
			}
			JoinStrategy::LeftLazy(join_type) => {
				join_type.handle_remove(txn, pre, side, key_hash, state, operator, version)
			}
			JoinStrategy::InnerEager(join_type) => {
				join_type.handle_remove(txn, pre, side, key_hash, state, operator)
			}
			JoinStrategy::InnerLazy(join_type) => {
				join_type.handle_remove(txn, pre, side, key_hash, state, operator, version)
			}
		}
	}

	/// Handle update operations
	pub(crate) fn handle_update<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		pre: &Row,
		post: &Row,
		side: JoinSide,
		old_key: Option<Hash128>,
		new_key: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
		version: CommitVersion,
	) -> crate::Result<Vec<FlowDiff>> {
		match self {
			JoinStrategy::LeftEager(join_type) => {
				join_type.handle_update(txn, pre, post, side, old_key, new_key, state, operator)
			}
			JoinStrategy::LeftLazy(join_type) => join_type
				.handle_update(txn, pre, post, side, old_key, new_key, state, operator, version),
			JoinStrategy::InnerEager(join_type) => {
				join_type.handle_update(txn, pre, post, side, old_key, new_key, state, operator)
			}
			JoinStrategy::InnerLazy(join_type) => join_type
				.handle_update(txn, pre, post, side, old_key, new_key, state, operator, version),
		}
	}
}
