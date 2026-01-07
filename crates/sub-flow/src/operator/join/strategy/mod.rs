// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{CommitVersion, JoinType, value::column::Columns};
use reifydb_hash::Hash128;
use reifydb_sdk::FlowDiff;

use crate::{
	operator::join::{JoinSide, JoinState, operator::JoinOperator},
	transaction::FlowTransaction,
};

pub(crate) mod hash;
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

	/// Handle insert for rows with undefined join keys (processed individually)
	pub(crate) fn handle_insert_undefined(
		&self,
		txn: &mut FlowTransaction,
		post: &Columns,
		row_idx: usize,
		side: JoinSide,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> reifydb_type::Result<Vec<FlowDiff>> {
		match self {
			JoinStrategy::LeftHash(s) => {
				s.handle_insert_undefined(txn, post, row_idx, side, state, operator)
			}
			JoinStrategy::InnerHash(s) => {
				s.handle_insert_undefined(txn, post, row_idx, side, state, operator)
			}
		}
	}

	/// Handle remove for rows with undefined join keys (processed individually)
	pub(crate) fn handle_remove_undefined(
		&self,
		txn: &mut FlowTransaction,
		pre: &Columns,
		row_idx: usize,
		side: JoinSide,
		state: &mut JoinState,
		operator: &JoinOperator,
		version: CommitVersion,
	) -> reifydb_type::Result<Vec<FlowDiff>> {
		match self {
			JoinStrategy::LeftHash(s) => {
				s.handle_remove_undefined(txn, pre, row_idx, side, state, operator, version)
			}
			JoinStrategy::InnerHash(s) => {
				s.handle_remove_undefined(txn, pre, row_idx, side, state, operator, version)
			}
		}
	}

	/// Handle update for rows with undefined join keys (processed individually)
	pub(crate) fn handle_update_undefined(
		&self,
		txn: &mut FlowTransaction,
		pre: &Columns,
		post: &Columns,
		row_idx: usize,
		side: JoinSide,
		state: &mut JoinState,
		operator: &JoinOperator,
		version: CommitVersion,
	) -> reifydb_type::Result<Vec<FlowDiff>> {
		match self {
			JoinStrategy::LeftHash(s) => {
				s.handle_update_undefined(txn, pre, post, row_idx, side, state, operator, version)
			}
			JoinStrategy::InnerHash(s) => {
				s.handle_update_undefined(txn, pre, post, row_idx, side, state, operator, version)
			}
		}
	}

	/// Handle insert for rows with defined join keys (batched by key)
	pub(crate) fn handle_insert(
		&self,
		txn: &mut FlowTransaction,
		post: &Columns,
		indices: &[usize],
		side: JoinSide,
		key_hash: &Hash128,
		state: &mut JoinState,
		operator: &JoinOperator,
	) -> reifydb_type::Result<Vec<FlowDiff>> {
		match self {
			JoinStrategy::LeftHash(s) => {
				s.handle_insert(txn, post, indices, side, key_hash, state, operator)
			}
			JoinStrategy::InnerHash(s) => {
				s.handle_insert(txn, post, indices, side, key_hash, state, operator)
			}
		}
	}

	/// Handle remove for rows with defined join keys (batched by key)
	pub(crate) fn handle_remove(
		&self,
		txn: &mut FlowTransaction,
		pre: &Columns,
		indices: &[usize],
		side: JoinSide,
		key_hash: &Hash128,
		state: &mut JoinState,
		operator: &JoinOperator,
		version: CommitVersion,
	) -> reifydb_type::Result<Vec<FlowDiff>> {
		match self {
			JoinStrategy::LeftHash(s) => {
				s.handle_remove(txn, pre, indices, side, key_hash, state, operator, version)
			}
			JoinStrategy::InnerHash(s) => {
				s.handle_remove(txn, pre, indices, side, key_hash, state, operator, version)
			}
		}
	}

	/// Handle update for rows with defined join keys (batched by key)
	pub(crate) fn handle_update(
		&self,
		txn: &mut FlowTransaction,
		pre: &Columns,
		post: &Columns,
		indices: &[usize],
		side: JoinSide,
		old_key: &Hash128,
		new_key: &Hash128,
		state: &mut JoinState,
		operator: &JoinOperator,
		version: CommitVersion,
	) -> reifydb_type::Result<Vec<FlowDiff>> {
		match self {
			JoinStrategy::LeftHash(s) => s.handle_update(
				txn, pre, post, indices, side, old_key, new_key, state, operator, version,
			),
			JoinStrategy::InnerHash(s) => s.handle_update(
				txn, pre, post, indices, side, old_key, new_key, state, operator, version,
			),
		}
	}
}
