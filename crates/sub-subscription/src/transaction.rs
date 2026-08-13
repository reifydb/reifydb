// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cmp::Ordering,
	collections::HashMap,
	mem,
	ops::{
		Bound::{Excluded, Included, Unbounded},
		RangeBounds,
	},
};

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::bytes::EncodedBytes,
};
use reifydb_core::{
	actors::pending::{PendingLayers, PendingWrite},
	common::CommitVersion,
	interface::{change::Change, store::MultiVersionRow},
};
use reifydb_flow::{
	error::FlowGraphError,
	operator::sink::DurableSink,
	timer::Timer,
	transaction::{
		ChangeCoordinate, FlowTransaction,
		read::{ReadFrom, read_from},
		substrate::FlowSubstrate,
	},
};
use reifydb_runtime::context::clock::Clock;
use reifydb_transaction::{
	change_accumulator::ChangeAccumulator,
	multi::{RangeScope, transaction::read::MultiReadTransaction},
};
use reifydb_value::{Result, error::Error, value::datetime::DateTime};

pub struct EphemeralTransaction {
	pub version: CommitVersion,
	pub pending: PendingLayers,
	pub query: MultiReadTransaction,
	pub catalog: Catalog,
	pub accumulator: ChangeAccumulator,
	pub clock: Clock,

	pub change_coordinate: Option<ChangeCoordinate>,

	pub flow_watermark: Option<DateTime>,

	pub substrate: FlowSubstrate,

	pub state: HashMap<EncodedKey, EncodedBytes>,
}

impl EphemeralTransaction {
	pub fn new(
		version: CommitVersion,
		query: MultiReadTransaction,
		catalog: Catalog,
		state: HashMap<EncodedKey, EncodedBytes>,
		clock: Clock,
	) -> Self {
		let mut query = query;
		query.read_as_of_version_inclusive(version);

		Self {
			version,
			pending: PendingLayers::empty(),
			query,
			catalog,
			accumulator: ChangeAccumulator::new(),
			clock,
			change_coordinate: None,
			flow_watermark: None,
			substrate: FlowSubstrate::new(),
			state,
		}
	}

	pub fn merge_state(&mut self) {
		let own = self.pending.take_top();
		for (key, write) in own.iter_sorted() {
			if matches!(read_from(key), ReadFrom::OperatorState | ReadFrom::StateQuery) {
				match write {
					PendingWrite::Set(row) => {
						self.state.insert(key.clone(), row.clone());
					}
					PendingWrite::Remove {
						..
					} => {
						self.state.remove(key);
					}
				}
			}
		}
	}

	pub fn take_state(&mut self) -> HashMap<EncodedKey, EncodedBytes> {
		mem::take(&mut self.state)
	}
}

fn is_state_range(range: &EncodedKeyRange) -> bool {
	match range.start.as_ref() {
		Included(start) | Excluded(start) => {
			matches!(read_from(start), ReadFrom::OperatorState | ReadFrom::StateQuery)
		}
		Unbounded => false,
	}
}

fn state_items(
	state: &HashMap<EncodedKey, EncodedBytes>,
	range: &EncodedKeyRange,
	version: CommitVersion,
) -> Vec<Result<MultiVersionRow>> {
	state.iter()
		.filter(|(key, _)| range.contains(key))
		.map(|(key, bytes)| {
			Ok(MultiVersionRow {
				key: key.clone(),
				bytes: bytes.clone(),
				version,
			})
		})
		.collect()
}

fn ephemeral_storage_get(
	state: &HashMap<EncodedKey, EncodedBytes>,
	query: &MultiReadTransaction,
	key: &EncodedKey,
) -> Result<Option<EncodedBytes>> {
	match read_from(key) {
		ReadFrom::OperatorState | ReadFrom::StateQuery => Ok(state.get(key).cloned()),
		ReadFrom::Query | ReadFrom::OwnedRow => match query.get(key)? {
			Some(multi) => Ok(Some(multi.bytes().clone())),
			None => Ok(None),
		},
	}
}

fn ephemeral_storage_contains(
	state: &HashMap<EncodedKey, EncodedBytes>,
	query: &MultiReadTransaction,
	key: &EncodedKey,
) -> Result<bool> {
	match read_from(key) {
		ReadFrom::OperatorState | ReadFrom::StateQuery => Ok(state.contains_key(key)),
		ReadFrom::Query | ReadFrom::OwnedRow => query.contains_key(key),
	}
}

fn ephemeral_storage_range<'a>(
	state: &HashMap<EncodedKey, EncodedBytes>,
	query: &'a MultiReadTransaction,
	version: CommitVersion,
	range: EncodedKeyRange,
	scope: RangeScope,
	batch_size: usize,
) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + 'a> {
	if is_state_range(&range) {
		let mut items = state_items(state, &range, version);
		items.sort_by(|a, b| match (a, b) {
			(Ok(a), Ok(b)) => a.key.cmp(&b.key),
			_ => Ordering::Equal,
		});
		return Box::new(items.into_iter());
	}
	Box::new(query.range(range, scope, batch_size))
}

fn ephemeral_storage_range_rev<'a>(
	state: &HashMap<EncodedKey, EncodedBytes>,
	query: &'a MultiReadTransaction,
	version: CommitVersion,
	range: EncodedKeyRange,
	scope: RangeScope,
	batch_size: usize,
) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + 'a> {
	if is_state_range(&range) {
		let mut items = state_items(state, &range, version);
		items.sort_by(|a, b| match (a, b) {
			(Ok(a), Ok(b)) => b.key.cmp(&a.key),
			_ => Ordering::Equal,
		});
		return Box::new(items.into_iter());
	}
	Box::new(query.range_rev(range, scope, batch_size))
}

fn ephemeral_fetch_state_external(
	state: &HashMap<EncodedKey, EncodedBytes>,
	version: CommitVersion,
	keys: &[EncodedKey],
	items: &mut Vec<MultiVersionRow>,
) {
	for key in keys {
		if let Some(bytes) = state.get(key) {
			items.push(MultiVersionRow {
				key: key.clone(),
				bytes: bytes.clone(),
				version,
			});
		}
	}
}

impl FlowTransaction for EphemeralTransaction {
	fn version(&self) -> CommitVersion {
		self.version
	}

	fn clock(&self) -> &Clock {
		&self.clock
	}

	fn catalog(&self) -> &Catalog {
		&self.catalog
	}

	fn query(&self) -> MultiReadTransaction {
		self.query.clone()
	}

	fn substrate(&self) -> &FlowSubstrate {
		&self.substrate
	}

	fn pending_layers(&self) -> &PendingLayers {
		&self.pending
	}

	fn pending_layers_mut(&mut self) -> &mut PendingLayers {
		&mut self.pending
	}

	fn accumulator_mut(&mut self) -> &mut ChangeAccumulator {
		&mut self.accumulator
	}

	fn change_coordinate(&self) -> Option<ChangeCoordinate> {
		self.change_coordinate
	}

	fn set_change_coordinate(&mut self, coordinate: ChangeCoordinate) {
		self.change_coordinate = Some(coordinate);
	}

	fn flow_watermark(&self) -> Option<DateTime> {
		self.flow_watermark
	}

	fn set_flow_watermark(&mut self, watermark: DateTime) {
		self.flow_watermark = Some(watermark);
	}

	fn run_durable_sink(&mut self, _sink: &mut dyn DurableSink, _change: Change) -> Result<Change> {
		Err(Error::from(FlowGraphError::UnsupportedNode {
			kind: "DurableSink",
		}))
	}

	fn run_durable_sink_timer(&mut self, _sink: &mut dyn DurableSink, _timer: Timer) -> Result<Option<Change>> {
		Err(Error::from(FlowGraphError::UnsupportedNode {
			kind: "DurableSink",
		}))
	}

	fn storage_get(&mut self, key: &EncodedKey) -> Result<Option<EncodedBytes>> {
		ephemeral_storage_get(&self.state, &self.query, key)
	}

	fn storage_contains(&mut self, key: &EncodedKey) -> Result<bool> {
		ephemeral_storage_contains(&self.state, &self.query, key)
	}

	fn storage_range(
		&mut self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_> {
		ephemeral_storage_range(&self.state, &self.query, self.version, range, scope, batch_size)
	}

	fn storage_range_rev(
		&mut self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_> {
		ephemeral_storage_range_rev(&self.state, &self.query, self.version, range, scope, batch_size)
	}

	fn fetch_state_external(&mut self, keys: &[EncodedKey], items: &mut Vec<MultiVersionRow>) -> Result<()> {
		ephemeral_fetch_state_external(&self.state, self.version, keys, items);
		Ok(())
	}
}
