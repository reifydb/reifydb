// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap},
	mem::take,
	sync::Arc,
};

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{bytes::EncodedBytes, pod::EncodedPodRow, shape::RowShape},
};
use reifydb_core::{
	actors::pending::{Pending, PendingWrite},
	common::{CommitVersion, SourceVersion},
	interface::{
		catalog::{flow::OperatorId, id::ViewId},
		change::Change,
		store::{MultiVersionBatch, MultiVersionRow},
	},
	internal_err,
	key::{
		any::TaggedKey,
		operator::state::{GroupId, GroupStateKey, KeyspaceMask},
	},
};
use reifydb_flow::txn::{Batch, Get};
use reifydb_runtime::context::clock::Clock;
use reifydb_store_operator::store::OperatorStore;
use reifydb_transaction::{
	dictionary::DictionaryAllocatorRegistry,
	interceptor::interceptors::Interceptors,
	multi::{RangeScope, transaction::read::MultiReadTransaction},
	transaction::admin::AdminTransaction,
};
use reifydb_value::{Result, byte_size::ByteSize, value::datetime::DateTime};

pub mod deferred;
pub mod dictionary;
pub mod frontier;
pub mod join_expiry;
#[cfg(test)]
pub(crate) mod mock;
pub mod read;
pub mod reclaim;
pub mod row_number;
pub mod scope;
pub mod state;
pub mod substrate;
pub mod watermark;

use crate::{
	operator::sink::DurableSink,
	timer::{Timer, TimerDue},
	transaction::{read::flow_merge_pending_iterator, substrate::FlowSubstrate},
};

#[derive(Clone, Copy)]
pub struct ChangeCoordinate {
	pub at: Option<DateTime>,
}

pub struct DeferredParams {
	pub version: CommitVersion,
	pub pending: Pending,
	pub query: Option<MultiReadTransaction>,
	pub state_query: Option<MultiReadTransaction>,
	pub catalog: Catalog,
	pub interceptors: Interceptors,
	pub clock: Clock,

	pub substrate: FlowSubstrate,

	pub lookup: Option<LookupContext>,
}

pub trait LookupVersions: Send + Sync {
	fn view_version(&self, view: ViewId, source: SourceVersion) -> Option<CommitVersion>;

	fn view_commit_through(&self, view: ViewId, commit: CommitVersion) -> Option<CommitVersion>;
}

#[derive(Clone)]
pub struct LookupContext {
	pub versions: Arc<dyn LookupVersions>,
	pub floor: CommitVersion,
}

impl DeferredParams {
	pub fn from_parent(
		parent: &AdminTransaction,
		operators: OperatorStore,
		version: CommitVersion,
		catalog: Catalog,
		interceptors: Interceptors,
		clock: Clock,
	) -> Self {
		Self {
			version,
			pending: Pending::new(),
			query: Some(parent.multi.begin_query().unwrap()),
			state_query: Some(parent.multi.begin_query().unwrap()),
			catalog,
			interceptors,
			clock,
			substrate: FlowSubstrate::with_dictionary(
				parent.dictionary_allocators()
					.expect("admin transaction reached a flow without a dictionary registry"),
				operators,
			),
			lookup: None,
		}
	}
}

pub trait FlowTransaction: Get + Batch + Sized + Send + 'static {
	fn version(&self) -> CommitVersion;

	fn clock(&self) -> &Clock;

	fn catalog(&self) -> &Catalog;

	fn query(&self) -> MultiReadTransaction;

	fn substrate(&self) -> &FlowSubstrate;

	fn pending(&self) -> &Pending;

	fn pending_mut(&mut self) -> &mut Pending;

	fn armed_mut(&mut self) -> &mut Vec<TimerDue>;

	fn change_coordinate(&self) -> Option<ChangeCoordinate>;

	fn set_change_coordinate(&mut self, coordinate: ChangeCoordinate);

	fn flow_watermark(&self) -> Option<DateTime>;

	fn set_flow_watermark(&mut self, watermark: DateTime);

	fn source_watermark_cache(&mut self) -> &mut HashMap<OperatorId, i64>;

	fn row_shape_cache(&mut self, operator: OperatorId) -> &mut HashMap<EncodedKey, RowShape>;

	fn run_durable_sink(&mut self, sink: &mut dyn DurableSink, change: Change) -> Result<Change>;

	fn run_durable_sink_timer(&mut self, sink: &mut dyn DurableSink, timer: Timer) -> Result<Option<Change>>;

	fn storage_get(&mut self, key: &EncodedKey) -> Result<Option<EncodedBytes>>;

	fn storage_get_many(&mut self, keys: &[EncodedKey]) -> Result<Vec<Option<EncodedBytes>>> {
		keys.iter().map(|key| self.storage_get(key)).collect()
	}

	fn storage_contains(&mut self, key: &EncodedKey) -> Result<bool>;

	fn storage_range(
		&mut self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>>> + Send + '_>;

	fn fetch_state(&mut self, id: OperatorId, keys: &[GroupStateKey]) -> Result<Vec<Option<EncodedBytes>>>;

	fn lookup_floor(&self) -> Option<CommitVersion> {
		None
	}

	fn lookup_view_version(&self, _view: ViewId, _source: SourceVersion) -> Option<CommitVersion> {
		None
	}

	fn lookup_view_commit_through(&self, _view: ViewId, _commit: CommitVersion) -> Option<CommitVersion> {
		None
	}

	fn take_pending(&mut self) -> Pending {
		take(self.pending_mut())
	}

	fn push_armed(&mut self, armed: TimerDue) {
		self.armed_mut().push(armed);
	}

	fn take_armed(&mut self) -> Vec<TimerDue> {
		take(self.armed_mut())
	}

	fn contains_key(&mut self, key: &EncodedKey) -> Result<bool> {
		let pending = match self.pending().write_at(key) {
			Some(PendingWrite::Remove {
				..
			}) => Some(false),
			Some(PendingWrite::Set(_)) => Some(true),
			None => None,
		};
		if let Some(result) = pending {
			return Ok(result);
		}
		self.storage_contains(key)
	}

	fn range(
		&mut self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>>> + Send + '_> {
		let mut merged = BTreeMap::new();
		self.pending().collect_range((range.start.as_ref(), range.end.as_ref()), &mut merged);
		let pending_vec: Vec<(EncodedKey, PendingWrite)> = merged.into_iter().collect();
		let version = self.version();
		let storage_iter = self.storage_range(range, scope, batch_size);
		Box::new(flow_merge_pending_iterator(pending_vec, storage_iter, version))
	}

	fn prefix(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch<TaggedKey>> {
		let range = EncodedKeyRange::prefix(prefix);
		let items = self.range(range, RangeScope::All, 1024).collect::<Result<Vec<_>>>()?;
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	fn set_one(&mut self, key: &EncodedKey, value: impl Into<EncodedBytes>) -> Result<()> {
		self.pending_mut().insert(key.clone(), value.into());
		Ok(())
	}

	fn remove_one(&mut self, key: &EncodedKey) -> Result<()> {
		self.pending_mut().remove(key.clone());
		Ok(())
	}

	fn remove_unobserved(&mut self, key: &EncodedKey) -> Result<()> {
		self.pending_mut().remove_unobserved(key.clone());
		Ok(())
	}

	fn remove_silent(&mut self, key: &EncodedKey) -> Result<()> {
		self.pending_mut().remove_silent(key.clone());
		Ok(())
	}

	fn replace_batch(
		&mut self,
		pre_keys: Vec<EncodedKey>,
		post_keys: Vec<EncodedKey>,
		rows: Vec<EncodedBytes>,
	) -> Result<()> {
		if pre_keys.len() != post_keys.len() || post_keys.len() != rows.len() {
			return internal_err!(
				"replace_batch got {} pre keys, {} post keys and {} rows",
				pre_keys.len(),
				post_keys.len(),
				rows.len()
			);
		}
		let pending = self.pending_mut();
		for ((pre_key, post_key), row) in pre_keys.into_iter().zip(post_keys).zip(rows) {
			if pre_key != post_key {
				pending.remove(pre_key);
			}
			pending.insert(post_key, row);
		}
		Ok(())
	}

	fn classify(&mut self, key: &EncodedKey, pre: Option<ByteSize>) {
		self.pending_mut().classify(key.clone(), pre);
	}

	#[inline]
	fn lookup_overlays(&self, key: &EncodedKey) -> Option<Option<EncodedBytes>> {
		match self.pending().write_at(key) {
			Some(PendingWrite::Remove {
				..
			}) => Some(None),
			Some(PendingWrite::Set(value)) => Some(Some(value.clone())),
			None => None,
		}
	}

	fn dictionary_allocators(&self) -> DictionaryAllocatorRegistry {
		self.substrate().dictionary.clone()
	}

	fn operator_store(&self) -> OperatorStore {
		self.substrate().operators.clone().expect("flow transaction was built without an operator store")
	}

	fn fetch_group_page(
		&self,
		id: OperatorId,
		groups: &[GroupId],
		batch: u64,
		keyspaces: KeyspaceMask,
	) -> Result<(Vec<(GroupStateKey, EncodedPodRow)>, bool)> {
		let page = self.operator_store().group_page(id, groups, batch, keyspaces)?;
		Ok((page.items, page.has_more))
	}

	fn written_at(&self) -> DateTime {
		match self.change_coordinate().and_then(|coordinate| coordinate.at) {
			Some(at) => at,
			None => self.clock().now(),
		}
	}
}

pub fn pending_get<T: FlowTransaction>(txn: &mut T, key: &EncodedKey) -> Result<Option<EncodedBytes>> {
	match txn.lookup_overlays(key) {
		Some(overlay) => Ok(overlay),
		None => txn.storage_get(key),
	}
}

pub fn pending_get_many<T: FlowTransaction>(txn: &mut T, keys: &[EncodedKey]) -> Result<Vec<Option<EncodedBytes>>> {
	let mut rows = Vec::with_capacity(keys.len());
	let mut misses = Vec::new();
	let mut miss_keys = Vec::new();
	for (slot, key) in keys.iter().enumerate() {
		match txn.lookup_overlays(key) {
			Some(overlay) => rows.push(overlay),
			None => {
				rows.push(None);
				misses.push(slot);
				miss_keys.push(key.clone());
			}
		}
	}
	if !miss_keys.is_empty() {
		for (slot, row) in misses.into_iter().zip(txn.storage_get_many(&miss_keys)?) {
			rows[slot] = row;
		}
	}
	Ok(rows)
}
