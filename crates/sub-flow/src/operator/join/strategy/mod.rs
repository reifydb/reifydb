use reifydb_core::{JoinType, interface::Transaction, value::row::Row};
use reifydb_engine::StandardCommandTransaction;
use reifydb_hash::Hash128;
use reifydb_rql::query::QueryString;

use crate::{
	flow::FlowDiff,
	operator::join::{
		JoinSide, JoinState,
		loading::{EagerLoading, LazyLoading},
		operator::JoinOperator,
	},
};

mod inner;
mod left;

pub(crate) use inner::InnerJoin;
pub(crate) use left::LeftJoin;

#[derive(Debug, Clone)]
pub(crate) enum JoinStrategy {
	LeftEager(LeftJoin, EagerLoading),
	LeftLazy(LeftJoin, LazyLoading),
	InnerEager(InnerJoin, EagerLoading),
	InnerLazy(InnerJoin, LazyLoading),
}

impl JoinStrategy {
	pub(crate) fn from(
		storage_strategy: reifydb_core::JoinStrategy,
		join_type: JoinType,
		right_query: QueryString,
	) -> Self {
		match (storage_strategy, join_type) {
			(reifydb_core::JoinStrategy::EagerLoading, JoinType::Left) => {
				JoinStrategy::LeftEager(LeftJoin, EagerLoading::new())
			}
			(reifydb_core::JoinStrategy::LazyLoading, JoinType::Left) => {
				JoinStrategy::LeftLazy(LeftJoin, LazyLoading::new(right_query))
			}
			(reifydb_core::JoinStrategy::EagerLoading, JoinType::Inner) => {
				JoinStrategy::InnerEager(InnerJoin, EagerLoading::new())
			}
			(reifydb_core::JoinStrategy::LazyLoading, JoinType::Inner) => {
				JoinStrategy::InnerLazy(InnerJoin, LazyLoading::new(right_query))
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
			JoinStrategy::LeftEager(join_type, loading) => join_type
				.handle_insert_with_loading(txn, post, side, key_hash, state, operator, loading),
			JoinStrategy::LeftLazy(join_type, loading) => join_type
				.handle_insert_with_loading(txn, post, side, key_hash, state, operator, loading),
			JoinStrategy::InnerEager(join_type, loading) => join_type
				.handle_insert_with_loading(txn, post, side, key_hash, state, operator, loading),
			JoinStrategy::InnerLazy(join_type, loading) => join_type
				.handle_insert_with_loading(txn, post, side, key_hash, state, operator, loading),
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
				join_type.handle_remove_with_loading(txn, pre, side, key_hash, state, operator, loading)
			}
			JoinStrategy::LeftLazy(join_type, loading) => {
				join_type.handle_remove_with_loading(txn, pre, side, key_hash, state, operator, loading)
			}
			JoinStrategy::InnerEager(join_type, loading) => {
				join_type.handle_remove_with_loading(txn, pre, side, key_hash, state, operator, loading)
			}
			JoinStrategy::InnerLazy(join_type, loading) => {
				join_type.handle_remove_with_loading(txn, pre, side, key_hash, state, operator, loading)
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
	) -> crate::Result<Vec<FlowDiff>> {
		match self {
			JoinStrategy::LeftEager(join_type, loading) => join_type.handle_update_with_loading(
				txn, pre, post, side, old_key, new_key, state, operator, loading,
			),
			JoinStrategy::LeftLazy(join_type, loading) => join_type.handle_update_with_loading(
				txn, pre, post, side, old_key, new_key, state, operator, loading,
			),
			JoinStrategy::InnerEager(join_type, loading) => join_type.handle_update_with_loading(
				txn, pre, post, side, old_key, new_key, state, operator, loading,
			),
			JoinStrategy::InnerLazy(join_type, loading) => join_type.handle_update_with_loading(
				txn, pre, post, side, old_key, new_key, state, operator, loading,
			),
		}
	}
}
