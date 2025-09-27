use reifydb_core::{JoinType, flow::FlowDiff, interface::Transaction, value::row::Row};
use reifydb_engine::StandardCommandTransaction;
use reifydb_hash::Hash128;

use crate::operator::join::{JoinSide, JoinState, operator::JoinOperator};

mod inner;
mod left;

pub(crate) use inner::InnerJoin;
pub(crate) use left::LeftJoin;

#[derive(Debug, Clone)]
pub(crate) enum JoinStrategy {
	Left(LeftJoin),
	Inner(InnerJoin),
}

impl JoinStrategy {
	pub(crate) fn from(strategy: reifydb_core::JoinStrategy, join_type: JoinType) -> Self {
		match join_type {
			JoinType::Left => JoinStrategy::Left(LeftJoin),
			JoinType::Inner => JoinStrategy::Inner(InnerJoin),
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
			JoinStrategy::Left(s) => s.handle_insert(txn, post, side, key_hash, state, operator),
			JoinStrategy::Inner(s) => s.handle_insert(txn, post, side, key_hash, state, operator),
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
			JoinStrategy::Left(s) => s.handle_remove(txn, pre, side, key_hash, state, operator),
			JoinStrategy::Inner(s) => s.handle_remove(txn, pre, side, key_hash, state, operator),
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
			JoinStrategy::Left(s) => {
				s.handle_update(txn, pre, post, side, old_key, new_key, state, operator)
			}
			JoinStrategy::Inner(s) => {
				s.handle_update(txn, pre, post, side, old_key, new_key, state, operator)
			}
		}
	}
}
