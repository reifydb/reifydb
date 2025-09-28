use reifydb_core::{JoinType, interface::Transaction, value::row::Row};
use reifydb_engine::{StandardCommandTransaction, execute::Executor};
use reifydb_hash::Hash128;
use reifydb_rql::query::QueryString;

use crate::{
	flow::FlowDiff,
	operator::join::{JoinSide, JoinState, loading::EagerLoading, operator::JoinOperator},
};

mod inner;
mod left;
mod left_eager;
mod left_lazy;

pub(crate) use inner::InnerJoin;

use crate::operator::join::strategy::{left_eager::LeftEagerJoin, left_lazy::LeftLazyJoin};

pub(crate) enum JoinStrategy {
	LeftEager(LeftEagerJoin, EagerLoading),
	LeftLazy(LeftLazyJoin, EagerLoading),
	InnerEager(InnerJoin, EagerLoading),
	// InnerLazy(InnerJoin, LazyLoading),
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
				JoinStrategy::LeftEager(LeftEagerJoin, EagerLoading::new())
			}
			(reifydb_core::JoinStrategy::LazyLoading, JoinType::Left) => JoinStrategy::LeftLazy(
				LeftLazyJoin {
					query: right_query,
					executor,
				},
				EagerLoading::new(),
			),
			(reifydb_core::JoinStrategy::EagerLoading, JoinType::Inner) => {
				JoinStrategy::InnerEager(InnerJoin, EagerLoading::new())
			}
			(reifydb_core::JoinStrategy::LazyLoading, JoinType::Inner) => {
				// JoinStrategy::InnerLazy(InnerJoin, LazyLoading::new(right_query, executor))
				panic!()
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
	) -> crate::Result<Vec<FlowDiff>> {
		match self {
			JoinStrategy::LeftEager(join_type, loading) => {
				join_type.handle_insert(txn, post, side, key_hash, state, operator)
			}
			JoinStrategy::LeftLazy(join_type, loading) => {
				join_type.handle_insert(txn, post, side, key_hash, state, operator)
			}
			JoinStrategy::InnerEager(join_type, loading) => join_type
				.handle_insert_with_loading(txn, post, side, key_hash, state, operator, loading),
			// JoinStrategy::InnerLazy(join_type, loading) => join_type
			// 	.handle_insert_with_loading(txn, post, side, key_hash, state, operator, loading),
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
	) -> crate::Result<Vec<FlowDiff>> {
		match self {
			JoinStrategy::LeftEager(join_type, loading) => {
				join_type.handle_remove(txn, pre, side, key_hash, state, operator)
			}
			JoinStrategy::LeftLazy(join_type, loading) => {
				join_type.handle_remove(txn, pre, side, key_hash, state, operator)
			}
			JoinStrategy::InnerEager(join_type, loading) => {
				join_type.handle_remove_with_loading(txn, pre, side, key_hash, state, operator, loading)
			} /* JoinStrategy::InnerLazy(join_type, loading) => {
			   * 	join_type.handle_remove_with_loading(txn, pre, side, key_hash, state, operator,
			   * loading) } */
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
	) -> crate::Result<Vec<FlowDiff>> {
		match self {
			JoinStrategy::LeftEager(join_type, loading) => {
				join_type.handle_update(txn, pre, post, side, old_key, new_key, state, operator)
			}
			JoinStrategy::LeftLazy(join_type, loading) => {
				join_type.handle_update(txn, pre, post, side, old_key, new_key, state, operator)
			}
			JoinStrategy::InnerEager(join_type, loading) => join_type.handle_update_with_loading(
				txn, pre, post, side, old_key, new_key, state, operator, loading,
			),
			// JoinStrategy::InnerLazy(join_type, loading) => join_type.handle_update_with_loading(
			// 	txn, pre, post, side, old_key, new_key, state, operator, loading,
			// ),
		}
	}
}
