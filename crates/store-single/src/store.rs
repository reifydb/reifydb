// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::BTreeMap,
	ops::{Bound, Deref},
	sync::Arc,
};

use reifydb_core::{
	delta::Delta,
	encoded::{
		key::{EncodedKey, EncodedKeyRange},
		row::EncodedRow,
	},
	event::EventBus,
	interface::store::SingleVersionRow,
};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::clock::Clock,
	pool::{PoolConfig, Pools},
};
use reifydb_type::util::{cowvec::CowVec, hex};
use tracing::instrument;

use crate::{
	BufferConfig, Result, SingleVersionBatch, SingleVersionCommit, SingleVersionContains, SingleVersionGet,
	SingleVersionRange, SingleVersionRangeRev, SingleVersionRemove, SingleVersionSet, SingleVersionStore,
	buffer::tier::BufferTier,
	config::SingleStoreConfig,
	tier::{RangeCursor, TierStorage},
};

#[derive(Clone)]
pub struct StandardSingleStore(Arc<StandardSingleStoreInner>);

pub struct StandardSingleStoreInner {
	pub(crate) buffer: Option<BufferTier>,
}

impl StandardSingleStore {
	#[instrument(name = "store::single::new", level = "debug", skip(config), fields(
		has_hot = config.buffer.is_some(),
	))]
	pub fn new(config: SingleStoreConfig) -> Result<Self> {
		let buffer = config.buffer.map(|c| c.storage);

		Ok(Self(Arc::new(StandardSingleStoreInner {
			buffer,
		})))
	}

	pub fn buffer(&self) -> Option<&BufferTier> {
		self.buffer.as_ref()
	}
}

impl Deref for StandardSingleStore {
	type Target = StandardSingleStoreInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl StandardSingleStore {
	pub fn testing_memory() -> Self {
		let pools = Pools::new(PoolConfig::sync_only());
		let actor_system = ActorSystem::new(pools, Clock::Real);
		Self::testing_memory_with_eventbus(EventBus::new(&actor_system))
	}

	pub fn testing_memory_with_eventbus(event_bus: EventBus) -> Self {
		Self::new(SingleStoreConfig {
			buffer: Some(BufferConfig {
				storage: BufferTier::memory(),
			}),
			event_bus,
		})
		.unwrap()
	}
}

impl SingleVersionGet for StandardSingleStore {
	#[instrument(name = "store::single::get", level = "trace", skip(self), fields(key_hex = %hex::display(key.as_ref())))]
	fn get(&self, key: &EncodedKey) -> Result<Option<SingleVersionRow>> {
		if let Some(buffer) = &self.buffer
			&& let Some(value) = buffer.get(key.as_ref())?
		{
			return Ok(Some(SingleVersionRow {
				key: key.clone(),
				row: EncodedRow(value),
			}));
		}

		Ok(None)
	}
}

impl SingleVersionContains for StandardSingleStore {
	#[instrument(name = "store::single::contains", level = "trace", skip(self), fields(key_hex = %hex::display(key.as_ref())), ret)]
	fn contains(&self, key: &EncodedKey) -> Result<bool> {
		if let Some(buffer) = &self.buffer
			&& buffer.contains(key.as_ref())?
		{
			return Ok(true);
		}

		Ok(false)
	}
}

impl SingleVersionCommit for StandardSingleStore {
	#[instrument(name = "store::single::commit", level = "debug", skip(self, deltas), fields(delta_count = deltas.len()))]
	fn commit(&mut self, deltas: CowVec<Delta>) -> Result<()> {
		let Some(storage) = &self.buffer else {
			return Ok(());
		};

		let entries: Vec<_> = deltas
			.iter()
			.map(|delta| match delta {
				Delta::Set {
					key,
					row,
				} => (CowVec::new(key.as_ref().to_vec()), Some(CowVec::new(row.as_ref().to_vec()))),
				Delta::Unset {
					key,
					..
				}
				| Delta::Remove {
					key,
				}
				| Delta::Drop {
					key,
				} => (CowVec::new(key.as_ref().to_vec()), None),
			})
			.collect();

		storage.set(entries)?;

		Ok(())
	}
}

impl SingleVersionSet for StandardSingleStore {}
impl SingleVersionRemove for StandardSingleStore {}

impl SingleVersionRange for StandardSingleStore {
	#[instrument(name = "store::single::range_batch", level = "debug", skip(self), fields(batch_size = batch_size))]
	fn range_batch(&self, range: EncodedKeyRange, batch_size: u64) -> Result<SingleVersionBatch> {
		let mut all_entries: BTreeMap<CowVec<u8>, Option<CowVec<u8>>> = BTreeMap::new();

		let (start, end) = make_range_bounds(&range);

		if let Some(buffer) = &self.buffer {
			let mut cursor = RangeCursor::new();

			loop {
				let batch =
					buffer.range_next(&mut cursor, bound_as_ref(&start), bound_as_ref(&end), 4096)?;

				for entry in batch.entries {
					all_entries.entry(entry.key).or_insert(entry.value);
				}

				if cursor.exhausted {
					break;
				}
			}
		}

		let items: Vec<SingleVersionRow> = all_entries
			.into_iter()
			.filter_map(|(key_bytes, value)| {
				value.map(|val| SingleVersionRow {
					key: EncodedKey(key_bytes),
					row: EncodedRow(val),
				})
			})
			.take(batch_size as usize)
			.collect();

		let has_more = items.len() >= batch_size as usize;

		Ok(SingleVersionBatch {
			items,
			has_more,
		})
	}
}

impl SingleVersionRangeRev for StandardSingleStore {
	#[instrument(name = "store::single::range_rev_batch", level = "debug", skip(self), fields(batch_size = batch_size))]
	fn range_rev_batch(&self, range: EncodedKeyRange, batch_size: u64) -> Result<SingleVersionBatch> {
		let mut all_entries: BTreeMap<CowVec<u8>, Option<CowVec<u8>>> = BTreeMap::new();

		let (start, end) = make_range_bounds(&range);

		if let Some(buffer) = &self.buffer {
			let mut cursor = RangeCursor::new();

			loop {
				let batch = buffer.range_rev_next(
					&mut cursor,
					bound_as_ref(&start),
					bound_as_ref(&end),
					4096,
				)?;

				for entry in batch.entries {
					all_entries.entry(entry.key).or_insert(entry.value);
				}

				if cursor.exhausted {
					break;
				}
			}
		}

		let items: Vec<SingleVersionRow> = all_entries
			.into_iter()
			.rev()
			.filter_map(|(key_bytes, value)| {
				value.map(|val| SingleVersionRow {
					key: EncodedKey(key_bytes),
					row: EncodedRow(val),
				})
			})
			.take(batch_size as usize)
			.collect();

		let has_more = items.len() >= batch_size as usize;

		Ok(SingleVersionBatch {
			items,
			has_more,
		})
	}
}

impl SingleVersionStore for StandardSingleStore {}

fn bound_as_ref(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
	match bound {
		Bound::Included(v) => Bound::Included(v.as_slice()),
		Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
		Bound::Unbounded => Bound::Unbounded,
	}
}

fn make_range_bounds(range: &EncodedKeyRange) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
	let start = match &range.start {
		Bound::Included(key) => Bound::Included(key.as_ref().to_vec()),
		Bound::Excluded(key) => Bound::Excluded(key.as_ref().to_vec()),
		Bound::Unbounded => Bound::Unbounded,
	};

	let end = match &range.end {
		Bound::Included(key) => Bound::Included(key.as_ref().to_vec()),
		Bound::Excluded(key) => Bound::Excluded(key.as_ref().to_vec()),
		Bound::Unbounded => Bound::Unbounded,
	};

	(start, end)
}
