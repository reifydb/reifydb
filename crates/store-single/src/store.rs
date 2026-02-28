// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	collections::BTreeMap,
	ops::{Bound, Deref},
	sync::Arc,
};

use reifydb_core::{
	delta::Delta,
	encoded::{
		encoded::EncodedValues,
		key::{EncodedKey, EncodedKeyRange},
	},
	event::EventBus,
	interface::store::SingleVersionValues,
};
use reifydb_runtime::{SharedRuntimeConfig, actor::system::ActorSystem};
use reifydb_type::util::{cowvec::CowVec, hex};
use tracing::instrument;

use crate::{
	HotConfig, Result, SingleVersionBatch, SingleVersionCommit, SingleVersionContains, SingleVersionGet,
	SingleVersionRange, SingleVersionRangeRev, SingleVersionRemove, SingleVersionSet, SingleVersionStore,
	config::SingleStoreConfig,
	hot::tier::HotTier,
	tier::{RangeCursor, TierStorage},
};

#[derive(Clone)]
pub struct StandardSingleStore(Arc<StandardSingleStoreInner>);

pub struct StandardSingleStoreInner {
	pub(crate) hot: Option<HotTier>,
}

impl StandardSingleStore {
	#[instrument(name = "store::single::new", level = "debug", skip(config), fields(
		has_hot = config.hot.is_some(),
	))]
	pub fn new(config: SingleStoreConfig) -> Result<Self> {
		let hot = config.hot.map(|c| c.storage);

		Ok(Self(Arc::new(StandardSingleStoreInner {
			hot,
		})))
	}

	/// Get access to the hot storage tier.
	///
	/// Returns `None` if the hot tier is not configured.
	pub fn hot(&self) -> Option<&HotTier> {
		self.hot.as_ref()
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
		let actor_system = ActorSystem::new(SharedRuntimeConfig::default().actor_system_config());
		Self::testing_memory_with_eventbus(EventBus::new(&actor_system))
	}

	pub fn testing_memory_with_eventbus(event_bus: EventBus) -> Self {
		Self::new(SingleStoreConfig {
			hot: Some(HotConfig {
				storage: HotTier::memory(),
			}),
			event_bus,
		})
		.unwrap()
	}
}

impl SingleVersionGet for StandardSingleStore {
	#[instrument(name = "store::single::get", level = "trace", skip(self), fields(key_hex = %hex::encode(key.as_ref())))]
	fn get(&self, key: &EncodedKey) -> Result<Option<SingleVersionValues>> {
		if let Some(hot) = &self.hot {
			if let Some(value) = hot.get(key.as_ref())? {
				return Ok(Some(SingleVersionValues {
					key: key.clone(),
					values: EncodedValues(value),
				}));
			}
		}

		Ok(None)
	}
}

impl SingleVersionContains for StandardSingleStore {
	#[instrument(name = "store::single::contains", level = "trace", skip(self), fields(key_hex = %hex::encode(key.as_ref())), ret)]
	fn contains(&self, key: &EncodedKey) -> Result<bool> {
		if let Some(hot) = &self.hot {
			if hot.contains(key.as_ref())? {
				return Ok(true);
			}
		}

		Ok(false)
	}
}

impl SingleVersionCommit for StandardSingleStore {
	#[instrument(name = "store::single::commit", level = "debug", skip(self, deltas), fields(delta_count = deltas.len()))]
	fn commit(&mut self, deltas: CowVec<Delta>) -> Result<()> {
		// Get the hot storage tier (warm and cold are placeholders for now)
		let Some(storage) = &self.hot else {
			return Ok(());
		};

		// Process deltas as a batch
		let entries: Vec<_> = deltas
			.iter()
			.map(|delta| match delta {
				Delta::Set {
					key,
					values,
				} => (CowVec::new(key.as_ref().to_vec()), Some(CowVec::new(values.as_ref().to_vec()))),
				Delta::Unset {
					key,
					..
				}
				| Delta::Remove {
					key,
				}
				| Delta::Drop {
					key,
					..
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

		// Process hot tier
		if let Some(hot) = &self.hot {
			let mut cursor = RangeCursor::new();

			loop {
				let batch =
					hot.range_next(&mut cursor, bound_as_ref(&start), bound_as_ref(&end), 4096)?;

				for entry in batch.entries {
					all_entries.entry(entry.key).or_insert(entry.value);
				}

				if cursor.exhausted {
					break;
				}
			}
		}

		// Convert to SingleVersionValues, filtering out tombstones
		let items: Vec<SingleVersionValues> = all_entries
			.into_iter()
			.filter_map(|(key_bytes, value)| {
				value.map(|val| SingleVersionValues {
					key: EncodedKey(key_bytes),
					values: EncodedValues(val),
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

		// Process hot tier
		if let Some(hot) = &self.hot {
			let mut cursor = RangeCursor::new();

			loop {
				let batch = hot.range_rev_next(
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

		// Convert to SingleVersionValues in reverse order, filtering out tombstones
		let items: Vec<SingleVersionValues> = all_entries
			.into_iter()
			.rev() // Reverse for descending order
			.filter_map(|(key_bytes, value)| {
				value.map(|val| SingleVersionValues {
					key: EncodedKey(key_bytes),
					values: EncodedValues(val),
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

// ===== Helper functions =====

/// Helper to convert owned Bound to ref
fn bound_as_ref(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
	match bound {
		Bound::Included(v) => Bound::Included(v.as_slice()),
		Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
		Bound::Unbounded => Bound::Unbounded,
	}
}

/// Convert EncodedKeyRange to primitive storage bounds (owned for )
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
