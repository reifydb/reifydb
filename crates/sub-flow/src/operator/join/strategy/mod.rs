use reifydb_core::{CommitVersion, JoinType, Row};
use reifydb_engine::StandardCommandTransaction;
use reifydb_hash::Hash128;

use crate::{
	flow::FlowDiff,
	operator::join::{JoinSide, JoinState, operator::JoinOperator},
};

mod hash;
mod hash_inner;
mod hash_left;

use crate::operator::join::strategy::{hash_inner::InnerHashJoin, hash_left::LeftHashJoin};

pub(crate) enum JoinStrategy {
	LeftHash(LeftHashJoin),
	InnerHash(InnerHashJoin),
}

impl JoinStrategy {
	pub(crate) fn from(join_type: JoinType) -> Self {
		match join_type {
			JoinType::Left => JoinStrategy::LeftHash(LeftHashJoin),
			JoinType::Inner => JoinStrategy::InnerHash(InnerHashJoin),
		}
	}

	/// Handle insert operations
	pub(crate) fn handle_insert(
		&self,
		txn: &mut StandardCommandTransaction,
		post: &Row,
		side: JoinSide,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
		version: CommitVersion,
	) -> crate::Result<Vec<FlowDiff>> {
		match self {
			JoinStrategy::LeftHash(join_type) => {
				join_type.handle_insert(txn, post, side, key_hash, state, operator, version)
			}
			JoinStrategy::InnerHash(join_type) => {
				join_type.handle_insert(txn, post, side, key_hash, state, operator, version)
			}
		}
	}

	/// Handle remove operations
	pub(crate) fn handle_remove(
		&self,
		txn: &mut StandardCommandTransaction,
		pre: &Row,
		side: JoinSide,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
		version: CommitVersion,
	) -> crate::Result<Vec<FlowDiff>> {
		match self {
			JoinStrategy::LeftHash(join_type) => {
				join_type.handle_remove(txn, pre, side, key_hash, state, operator, version)
			}
			JoinStrategy::InnerHash(join_type) => {
				join_type.handle_remove(txn, pre, side, key_hash, state, operator, version)
			}
		}
	}

	/// Handle update operations
	pub(crate) fn handle_update(
		&self,
		txn: &mut StandardCommandTransaction,
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
			JoinStrategy::LeftHash(join_type) => join_type
				.handle_update(txn, pre, post, side, old_key, new_key, state, operator, version),
			JoinStrategy::InnerHash(join_type) => join_type
				.handle_update(txn, pre, post, side, old_key, new_key, state, operator, version),
		}
	}
}
