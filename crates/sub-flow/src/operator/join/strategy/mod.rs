// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::{CommitVersion, JoinType},
	interface::change::Diff,
	value::column::columns::Columns,
};
use reifydb_runtime::hash::Hash128;

use crate::{
	operator::join::{
		operator::JoinOperator,
		state::{JoinSide, JoinState},
	},
	transaction::FlowTransaction,
};

pub(crate) mod hash;
pub mod hash_inner;
pub mod hash_left;

use reifydb_type::Result;

use crate::operator::join::strategy::{hash_inner::InnerHashJoin, hash_left::LeftHashJoin};

/// Shared context for join strategy operations, grouping the side, mutable state, operator reference, and version.
pub(crate) struct JoinContext<'a> {
	pub side: JoinSide,
	pub state: &'a mut JoinState,
	pub operator: &'a JoinOperator,
	pub version: CommitVersion,
}

/// Pre- and post-update join key hashes for a batched key update.
pub(crate) struct UpdateKeys<'a> {
	pub pre: &'a Hash128,
	pub post: &'a Hash128,
}

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
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		match self {
			JoinStrategy::LeftHash(s) => s.handle_insert_undefined(txn, post, row_idx, ctx),
			JoinStrategy::InnerHash(s) => s.handle_insert_undefined(txn, post, row_idx, ctx),
		}
	}

	/// Handle remove for rows with undefined join keys (processed individually)
	pub(crate) fn handle_remove_undefined(
		&self,
		txn: &mut FlowTransaction,
		pre: &Columns,
		row_idx: usize,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		match self {
			JoinStrategy::LeftHash(s) => s.handle_remove_undefined(txn, pre, row_idx, ctx),
			JoinStrategy::InnerHash(s) => s.handle_remove_undefined(txn, pre, row_idx, ctx),
		}
	}

	/// Handle update for rows with undefined join keys (processed individually)
	pub(crate) fn handle_update_undefined(
		&self,
		txn: &mut FlowTransaction,
		pre: &Columns,
		post: &Columns,
		row_idx: usize,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		match self {
			JoinStrategy::LeftHash(s) => s.handle_update_undefined(txn, pre, post, row_idx, ctx),
			JoinStrategy::InnerHash(s) => s.handle_update_undefined(txn, pre, post, row_idx, ctx),
		}
	}

	/// Handle insert for rows with defined join keys (batched by key)
	pub(crate) fn handle_insert(
		&self,
		txn: &mut FlowTransaction,
		post: &Columns,
		indices: &[usize],
		key_hash: &Hash128,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		match self {
			JoinStrategy::LeftHash(s) => s.handle_insert(txn, post, indices, key_hash, ctx),
			JoinStrategy::InnerHash(s) => s.handle_insert(txn, post, indices, key_hash, ctx),
		}
	}

	/// Handle remove for rows with defined join keys (batched by key)
	pub(crate) fn handle_remove(
		&self,
		txn: &mut FlowTransaction,
		pre: &Columns,
		indices: &[usize],
		key_hash: &Hash128,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		match self {
			JoinStrategy::LeftHash(s) => s.handle_remove(txn, pre, indices, key_hash, ctx),
			JoinStrategy::InnerHash(s) => s.handle_remove(txn, pre, indices, key_hash, ctx),
		}
	}

	/// Handle update for rows with defined join keys (batched by key)
	pub(crate) fn handle_update(
		&self,
		txn: &mut FlowTransaction,
		pre: &Columns,
		post: &Columns,
		indices: &[usize],
		keys: UpdateKeys,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		match self {
			JoinStrategy::LeftHash(s) => s.handle_update(txn, pre, post, indices, keys, ctx),
			JoinStrategy::InnerHash(s) => s.handle_update(txn, pre, post, indices, keys, ctx),
		}
	}
}
