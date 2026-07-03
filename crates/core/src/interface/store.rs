// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	encoded::row::EncodedRow,
	key::encoded::{EncodedKey, EncodedKeyRange},
};
use reifydb_value::{Result, util::cowvec::CowVec};

use crate::{
	common::CommitVersion,
	delta::Delta,
	interface::catalog::{flow::FlowNodeId, shape::ShapeId},
	key::{
		EncodableKeyRange, Key, flow_node_internal_state::FlowNodeInternalStateKeyRange,
		flow_node_state::FlowNodeStateKeyRange, kind::KeyKind, row::RowKeyRange,
	},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Tier {
	Buffer,
	Persistent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntryKind {
	Multi,

	Source(ShapeId),

	Operator(FlowNodeId),
}

pub fn classify_key(key: &EncodedKey) -> EntryKind {
	match Key::decode(key) {
		Some(Key::Row(row_key)) => EntryKind::Source(row_key.shape),
		Some(Key::FlowNodeState(state_key)) => EntryKind::Operator(state_key.node),
		Some(Key::FlowNodeInternalState(internal_key)) => EntryKind::Operator(internal_key.node),
		_ => EntryKind::Multi,
	}
}

pub fn is_single_version_semantics_key(key: &EncodedKey) -> bool {
	Key::kind(key).is_some_and(|kind| matches!(kind, KeyKind::FlowNodeState | KeyKind::FlowNodeInternalState))
}

pub fn classify_range(range: &EncodedKeyRange) -> Option<EntryKind> {
	if let (Some(start), Some(_end)) = RowKeyRange::decode(range) {
		return Some(EntryKind::Source(start.shape));
	}

	if let (Some(start), Some(_end)) = FlowNodeStateKeyRange::decode(range) {
		return Some(EntryKind::Operator(start.node));
	}

	if let (Some(start), Some(_end)) = FlowNodeInternalStateKeyRange::decode(range) {
		return Some(EntryKind::Operator(start.node));
	}

	None
}

#[derive(Debug, Clone)]
pub struct MultiVersionRow {
	pub key: EncodedKey,
	pub row: EncodedRow,
	pub version: CommitVersion,
}

#[derive(Debug, Clone)]
pub struct SingleVersionRow {
	pub key: EncodedKey,
	pub row: EncodedRow,
}

#[derive(Debug, Clone)]
pub struct MultiVersionBatch {
	pub items: Vec<MultiVersionRow>,

	pub has_more: bool,
}

impl MultiVersionBatch {
	pub fn empty() -> Self {
		Self {
			items: Vec::new(),
			has_more: false,
		}
	}

	pub fn is_empty(&self) -> bool {
		self.items.is_empty()
	}
}

pub trait MultiVersionCommit: Send + Sync {
	fn commit(&self, deltas: CowVec<Delta>, version: CommitVersion) -> Result<()>;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadOptions {
	pub bypass_buffer: bool,
}

pub trait MultiVersionGet: Send + Sync {
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> Result<Option<MultiVersionRow>>;

	fn get_with_options(
		&self,
		key: &EncodedKey,
		version: CommitVersion,
		_options: ReadOptions,
	) -> Result<Option<MultiVersionRow>> {
		self.get(key, version)
	}
}

pub trait MultiVersionContains: Send + Sync {
	fn contains(&self, key: &EncodedKey, version: CommitVersion) -> Result<bool>;

	fn contains_with_options(
		&self,
		key: &EncodedKey,
		version: CommitVersion,
		_options: ReadOptions,
	) -> Result<bool> {
		self.contains(key, version)
	}
}

pub trait MultiVersionGetPrevious: Send + Sync {
	fn get_previous_version(
		&self,
		key: &EncodedKey,
		before_version: CommitVersion,
	) -> Result<Option<MultiVersionRow>>;
}

pub trait MultiVersionStore:
	Send + Sync + Clone + MultiVersionCommit + MultiVersionGet + MultiVersionGetPrevious + MultiVersionContains + 'static
{
}

#[derive(Debug, Clone)]
pub struct SingleVersionBatch {
	pub items: Vec<SingleVersionRow>,

	pub has_more: bool,
}

impl SingleVersionBatch {
	pub fn empty() -> Self {
		Self {
			items: Vec::new(),
			has_more: false,
		}
	}

	pub fn is_empty(&self) -> bool {
		self.items.is_empty()
	}
}

pub trait SingleVersionCommit: Send + Sync {
	fn commit(&mut self, deltas: CowVec<Delta>) -> Result<()>;
}

pub trait SingleVersionGet: Send + Sync {
	fn get(&self, key: &EncodedKey) -> Result<Option<SingleVersionRow>>;
}

pub trait SingleVersionContains: Send + Sync {
	fn contains(&self, key: &EncodedKey) -> Result<bool>;
}

pub trait SingleVersionSet: SingleVersionCommit {
	fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<()> {
		Self::commit(
			self,
			CowVec::new(vec![Delta::Set {
				key: key.clone(),
				row: row.clone(),
			}]),
		)
	}
}

pub trait SingleVersionRemove: SingleVersionCommit {
	fn unset(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<()> {
		Self::commit(
			self,
			CowVec::new(vec![Delta::Unset {
				key: key.clone(),
				row,
			}]),
		)
	}

	fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		Self::commit(
			self,
			CowVec::new(vec![Delta::Remove {
				key: key.clone(),
			}]),
		)
	}
}

pub trait SingleVersionRange: Send + Sync {
	fn range_batch(&self, range: EncodedKeyRange, batch_size: u64) -> Result<SingleVersionBatch>;

	fn range(&self, range: EncodedKeyRange) -> Result<SingleVersionBatch> {
		self.range_batch(range, 1024)
	}

	fn prefix(&self, prefix: &EncodedKey) -> Result<SingleVersionBatch> {
		self.range(EncodedKeyRange::prefix(prefix))
	}
}

pub trait SingleVersionRangeRev: Send + Sync {
	fn range_rev_batch(&self, range: EncodedKeyRange, batch_size: u64) -> Result<SingleVersionBatch>;

	fn range_rev(&self, range: EncodedKeyRange) -> Result<SingleVersionBatch> {
		self.range_rev_batch(range, 1024)
	}

	fn prefix_rev(&self, prefix: &EncodedKey) -> Result<SingleVersionBatch> {
		self.range_rev(EncodedKeyRange::prefix(prefix))
	}
}

pub trait SingleVersionStore:
	Send
	+ Sync
	+ Clone
	+ SingleVersionCommit
	+ SingleVersionGet
	+ SingleVersionContains
	+ SingleVersionSet
	+ SingleVersionRemove
	+ SingleVersionRange
	+ SingleVersionRangeRev
	+ 'static
{
}
