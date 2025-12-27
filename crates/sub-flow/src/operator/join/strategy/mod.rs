use reifydb_core::{CommitVersion, JoinType, Row};
use reifydb_flow_operator_sdk::FlowDiff;
use reifydb_hash::Hash128;

use crate::{
	operator::join::{JoinSide, JoinState, operator::JoinOperator},
	transaction::FlowTransaction,
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

	pub(crate) async fn handle_insert(
		&self,
		txn: &mut FlowTransaction,
		post: &Row,
		side: JoinSide,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		match self {
			JoinStrategy::LeftHash(s) => s.handle_insert(txn, post, side, key_hash, state, operator).await,
			JoinStrategy::InnerHash(s) => s.handle_insert(txn, post, side, key_hash, state, operator).await,
		}
	}

	pub(crate) async fn handle_remove(
		&self,
		txn: &mut FlowTransaction,
		pre: &Row,
		side: JoinSide,
		key_hash: Option<Hash128>,
		state: &mut JoinState,
		operator: &JoinOperator,
		version: CommitVersion,
	) -> crate::Result<Vec<FlowDiff>> {
		match self {
			JoinStrategy::LeftHash(s) => {
				s.handle_remove(txn, pre, side, key_hash, state, operator, version).await
			}
			JoinStrategy::InnerHash(s) => {
				s.handle_remove(txn, pre, side, key_hash, state, operator, version).await
			}
		}
	}

	pub(crate) async fn handle_update(
		&self,
		txn: &mut FlowTransaction,
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
			JoinStrategy::LeftHash(s) => {
				s.handle_update(txn, pre, post, side, old_key, new_key, state, operator, version).await
			}
			JoinStrategy::InnerHash(s) => {
				s.handle_update(txn, pre, post, side, old_key, new_key, state, operator, version).await
			}
		}
	}

	pub(crate) async fn handle_insert_multiple(
		&self,
		txn: &mut FlowTransaction,
		rows: &[Row],
		side: JoinSide,
		key_hash: &Hash128,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> crate::Result<Vec<FlowDiff>> {
		match self {
			JoinStrategy::LeftHash(s) => {
				s.handle_insert_multiple(txn, rows, side, key_hash, state, operator).await
			}
			JoinStrategy::InnerHash(s) => {
				s.handle_insert_multiple(txn, rows, side, key_hash, state, operator).await
			}
		}
	}

	pub(crate) async fn handle_remove_multiple(
		&self,
		txn: &mut FlowTransaction,
		rows: &[Row],
		side: JoinSide,
		key_hash: &Hash128,
		state: &mut JoinState,
		operator: &JoinOperator,
		version: CommitVersion,
	) -> crate::Result<Vec<FlowDiff>> {
		match self {
			JoinStrategy::LeftHash(s) => {
				s.handle_remove_multiple(txn, rows, side, key_hash, state, operator, version).await
			}
			JoinStrategy::InnerHash(s) => {
				s.handle_remove_multiple(txn, rows, side, key_hash, state, operator, version).await
			}
		}
	}
}
