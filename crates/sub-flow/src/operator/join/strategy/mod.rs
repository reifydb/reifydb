// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{common::JoinType, interface::change::Diff, value::column::columns::Columns};
use reifydb_value::util::hash::Hash128;

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
pub(crate) mod latest;
pub mod latest_inner;
pub mod latest_left;

use reifydb_value::Result;

use crate::operator::join::strategy::{
	hash_inner::InnerHashJoin, hash_left::LeftHashJoin, latest_inner::LatestInnerHashJoin,
	latest_left::LatestLeftHashJoin,
};

pub(crate) struct JoinContext<'a> {
	pub side: JoinSide,
	pub state: &'a mut JoinState,
	pub operator: &'a JoinOperator,
}

#[derive(Clone, Copy)]
pub(crate) struct UpdateKeys<'a> {
	pub pre: &'a Hash128,
	pub post: &'a Hash128,
}

pub(crate) enum JoinStrategy {
	Left(LeftHashJoin),
	Inner(InnerHashJoin),
	LatestLeft(LatestLeftHashJoin),
	LatestInner(LatestInnerHashJoin),
}

impl JoinStrategy {
	pub(crate) fn from(join_type: JoinType, latest: bool) -> Self {
		match (join_type, latest) {
			(JoinType::Left, false) => JoinStrategy::Left(LeftHashJoin),
			(JoinType::Inner, false) => JoinStrategy::Inner(InnerHashJoin),
			(JoinType::Left, true) => JoinStrategy::LatestLeft(LatestLeftHashJoin),
			(JoinType::Inner, true) => JoinStrategy::LatestInner(LatestInnerHashJoin),
		}
	}

	pub(crate) fn handle_insert_undefined(
		&self,
		txn: &mut FlowTransaction,
		post: &Columns,
		row_idx: usize,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		match self {
			JoinStrategy::Left(s) => s.handle_insert_undefined(txn, post, row_idx, ctx),
			JoinStrategy::Inner(s) => s.handle_insert_undefined(txn, post, row_idx, ctx),
			JoinStrategy::LatestLeft(s) => s.handle_insert_undefined(txn, post, row_idx, ctx),
			JoinStrategy::LatestInner(s) => s.handle_insert_undefined(txn, post, row_idx, ctx),
		}
	}

	pub(crate) fn handle_remove_undefined(
		&self,
		txn: &mut FlowTransaction,
		pre: &Columns,
		row_idx: usize,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		match self {
			JoinStrategy::Left(s) => s.handle_remove_undefined(txn, pre, row_idx, ctx),
			JoinStrategy::Inner(s) => s.handle_remove_undefined(txn, pre, row_idx, ctx),
			JoinStrategy::LatestLeft(s) => s.handle_remove_undefined(txn, pre, row_idx, ctx),
			JoinStrategy::LatestInner(s) => s.handle_remove_undefined(txn, pre, row_idx, ctx),
		}
	}

	pub(crate) fn handle_update_undefined(
		&self,
		txn: &mut FlowTransaction,
		pre: &Columns,
		post: &Columns,
		row_idx: usize,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		match self {
			JoinStrategy::Left(s) => s.handle_update_undefined(txn, pre, post, row_idx, ctx),
			JoinStrategy::Inner(s) => s.handle_update_undefined(txn, pre, post, row_idx, ctx),
			JoinStrategy::LatestLeft(s) => s.handle_update_undefined(txn, pre, post, row_idx, ctx),
			JoinStrategy::LatestInner(s) => s.handle_update_undefined(txn, pre, post, row_idx, ctx),
		}
	}

	pub(crate) fn handle_insert(
		&self,
		txn: &mut FlowTransaction,
		post: &Columns,
		indices: &[usize],
		key_hash: &Hash128,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		match self {
			JoinStrategy::Left(s) => s.handle_insert(txn, post, indices, key_hash, ctx),
			JoinStrategy::Inner(s) => s.handle_insert(txn, post, indices, key_hash, ctx),
			JoinStrategy::LatestLeft(s) => s.handle_insert(txn, post, indices, key_hash, ctx),
			JoinStrategy::LatestInner(s) => s.handle_insert(txn, post, indices, key_hash, ctx),
		}
	}

	pub(crate) fn handle_remove(
		&self,
		txn: &mut FlowTransaction,
		pre: &Columns,
		indices: &[usize],
		key_hash: &Hash128,
		ctx: &mut JoinContext,
	) -> Result<Vec<Diff>> {
		match self {
			JoinStrategy::Left(s) => s.handle_remove(txn, pre, indices, key_hash, ctx),
			JoinStrategy::Inner(s) => s.handle_remove(txn, pre, indices, key_hash, ctx),
			JoinStrategy::LatestLeft(s) => s.handle_remove(txn, pre, indices, key_hash, ctx),
			JoinStrategy::LatestInner(s) => s.handle_remove(txn, pre, indices, key_hash, ctx),
		}
	}

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
			JoinStrategy::Left(s) => s.handle_update(txn, pre, post, indices, keys, ctx),
			JoinStrategy::Inner(s) => s.handle_update(txn, pre, post, indices, keys, ctx),
			JoinStrategy::LatestLeft(s) => s.handle_update(txn, pre, post, indices, keys, ctx),
			JoinStrategy::LatestInner(s) => s.handle_update(txn, pre, post, indices, keys, ctx),
		}
	}
}
