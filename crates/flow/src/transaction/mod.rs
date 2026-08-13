// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::bytes::EncodedBytes,
};
use reifydb_core::{
	actors::pending::{Pending, PendingLayers, PendingWrite},
	common::CommitVersion,
	interface::{
		catalog::object::ObjectId,
		change::{Change, ChangeOrigin, Diff},
		store::{MultiVersionBatch, MultiVersionRow},
	},
};
use reifydb_runtime::context::clock::Clock;
use reifydb_store_operator::store::OperatorStore;
use reifydb_transaction::{
	change_accumulator::ChangeAccumulator,
	dictionary::DictionaryAllocatorRegistry,
	interceptor::interceptors::Interceptors,
	multi::{RangeScope, transaction::read::MultiReadTransaction},
	transaction::admin::AdminTransaction,
};
use reifydb_value::{Result, value::datetime::DateTime};

pub mod anchor;
pub mod deferred;
pub mod dictionary;
pub mod frontier;
pub mod group;
#[cfg(test)]
pub(crate) mod mock;
pub mod read;
pub mod reclaim;
pub mod row_number;
pub mod scope;
pub mod state;
pub mod substrate;
pub mod timer;
pub mod watermark;

use crate::{
	operator::sink::DurableSink,
	timer::Timer,
	transaction::{
		read::{flow_merge_pending_iterator, flow_merge_pending_iterator_rev},
		substrate::FlowSubstrate,
		timer::TimerWheel,
		watermark::SourceWatermarks,
	},
};

#[derive(Clone, Copy)]
pub struct ChangeCoordinate {
	pub at: Option<DateTime>,
	pub version: CommitVersion,
}

pub struct DeferredParams {
	pub version: CommitVersion,
	pub pending: PendingLayers,
	pub query: MultiReadTransaction,
	pub state_query: MultiReadTransaction,
	pub catalog: Catalog,
	pub interceptors: Interceptors,
	pub clock: Clock,

	pub substrate: FlowSubstrate,
}

impl DeferredParams {
	pub fn from_parent(
		parent: &AdminTransaction,
		version: CommitVersion,
		catalog: Catalog,
		interceptors: Interceptors,
		clock: Clock,
	) -> Self {
		Self {
			version,
			pending: PendingLayers::empty(),
			query: parent.multi.begin_query().unwrap(),
			state_query: parent.multi.begin_query().unwrap(),
			catalog,
			interceptors,
			clock,
			substrate: FlowSubstrate::new(),
		}
	}
}

pub trait FlowTransaction: Sized + Send + 'static {
	fn version(&self) -> CommitVersion;

	fn clock(&self) -> &Clock;

	fn catalog(&self) -> &Catalog;

	fn query(&self) -> MultiReadTransaction;

	fn substrate(&self) -> &FlowSubstrate;

	fn pending_layers(&self) -> &PendingLayers;

	fn pending_layers_mut(&mut self) -> &mut PendingLayers;

	fn accumulator_mut(&mut self) -> &mut ChangeAccumulator;

	fn change_coordinate(&self) -> Option<ChangeCoordinate>;

	fn set_change_coordinate(&mut self, coordinate: ChangeCoordinate);

	fn flow_watermark(&self) -> Option<DateTime>;

	fn set_flow_watermark(&mut self, watermark: DateTime);

	/// Runs a durable view sink against this transaction, applying the change.
	/// Only [`crate::transaction::deferred::DeferredTransaction`] can carry one; every other
	/// variant must reject the node rather than skip it silently.
	fn run_durable_sink(&mut self, sink: &mut dyn DurableSink, change: Change) -> Result<Change>;

	/// Fires a durable view sink's timer against this transaction.
	fn run_durable_sink_timer(&mut self, sink: &mut dyn DurableSink, timer: Timer) -> Result<Option<Change>>;

	fn storage_get(&mut self, key: &EncodedKey) -> Result<Option<EncodedBytes>>;

	fn storage_contains(&mut self, key: &EncodedKey) -> Result<bool>;

	fn storage_range(
		&mut self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_>;

	fn storage_range_rev(
		&mut self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_>;

	fn fetch_state_external(&mut self, keys: &[EncodedKey], items: &mut Vec<MultiVersionRow>) -> Result<()>;

	fn pending(&self) -> &Pending {
		self.pending_layers().top()
	}

	fn take_pending(&mut self) -> Pending {
		self.pending_layers_mut().take_top()
	}

	fn get(&mut self, key: &EncodedKey) -> Result<Option<EncodedBytes>> {
		if self.pending_layers().is_removed(key) {
			return Ok(None);
		}
		if let Some(value) = self.pending_layers().get(key) {
			return Ok(Some(value.clone()));
		}
		self.storage_get(key)
	}

	fn contains_key(&mut self, key: &EncodedKey) -> Result<bool> {
		if self.pending_layers().is_removed(key) {
			return Ok(false);
		}
		if self.pending_layers().get(key).is_some() {
			return Ok(true);
		}
		self.storage_contains(key)
	}

	fn range(
		&mut self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_> {
		let mut merged = BTreeMap::new();
		self.pending_layers().collect_range((range.start.as_ref(), range.end.as_ref()), &mut merged);
		let pending_vec: Vec<(EncodedKey, PendingWrite)> = merged.into_iter().collect();
		let version = self.version();
		let storage_iter = self.storage_range(range, scope, batch_size);
		Box::new(flow_merge_pending_iterator(pending_vec, storage_iter, version))
	}

	fn range_rev(
		&mut self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_> {
		let mut merged = BTreeMap::new();
		self.pending_layers().collect_range((range.start.as_ref(), range.end.as_ref()), &mut merged);
		let pending_vec: Vec<(EncodedKey, PendingWrite)> = merged.into_iter().rev().collect();
		let version = self.version();
		let storage_iter = self.storage_range_rev(range, scope, batch_size);
		Box::new(flow_merge_pending_iterator_rev(pending_vec, storage_iter, version))
	}

	fn prefix(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		let range = EncodedKeyRange::prefix(prefix);
		let items = self.range(range, RangeScope::All, 1024).collect::<Result<Vec<_>>>()?;
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	fn set(&mut self, key: &EncodedKey, value: impl Into<EncodedBytes>) -> Result<()> {
		self.pending_layers_mut().insert(key.clone(), value.into());
		Ok(())
	}

	fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		self.pending_layers_mut().remove(key.clone());
		Ok(())
	}

	fn remove_silent(&mut self, key: &EncodedKey) -> Result<()> {
		self.pending_layers_mut().remove_silent(key.clone());
		Ok(())
	}

	fn set_batch(&mut self, keys: &[EncodedKey], values: &[EncodedBytes]) -> Result<()> {
		self.pending_layers_mut().insert_batch(keys, values);
		Ok(())
	}

	fn remove_batch(&mut self, keys: &[EncodedKey]) -> Result<()> {
		self.pending_layers_mut().remove_batch(keys);
		Ok(())
	}

	#[inline]
	fn lookup_overlays(&self, key: &EncodedKey) -> Option<Option<EncodedBytes>> {
		let pending = self.pending_layers();
		if pending.is_removed(key) {
			return Some(None);
		}
		pending.get(key).map(|row| Some(row.clone()))
	}

	fn dictionary_allocators(&self) -> DictionaryAllocatorRegistry {
		self.substrate().dictionary.clone()
	}

	fn source_watermarks(&self) -> SourceWatermarks {
		self.substrate().watermarks.clone()
	}

	fn timer_wheel(&self) -> TimerWheel {
		self.substrate().timers.clone()
	}

	fn operator_store(&self) -> OperatorStore {
		self.substrate().operators.clone().expect("flow transaction was built without an operator store")
	}

	fn written_at(&self) -> DateTime {
		match self.change_coordinate().and_then(|coordinate| coordinate.at) {
			Some(at) => at,
			None => self.clock().now(),
		}
	}

	fn track_flow_change(&mut self, change: Change) {
		if let ChangeOrigin::Object(id) = change.origin {
			let accumulator = self.accumulator_mut();
			for diff in change.diffs {
				accumulator.track(id, diff);
			}
		}
	}

	fn take_accumulator_entries(&mut self) -> Vec<(ObjectId, Diff)> {
		let acc = self.accumulator_mut();
		let entries: Vec<_> = acc.entries_from(0).to_vec();
		acc.clear();
		entries
	}
}
