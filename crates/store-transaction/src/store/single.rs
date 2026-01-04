// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	collections::{BTreeMap, HashMap},
	ops::Bound,
};

use async_trait::async_trait;
use reifydb_core::{
	CowVec, EncodedKey, EncodedKeyRange, delta::Delta, interface::SingleVersionValues,
	value::encoded::EncodedValues,
};
use reifydb_type::util::hex;
use tracing::instrument;

use super::StandardTransactionStore;
use crate::{
	SingleVersionBatch, SingleVersionCommit, SingleVersionContains, SingleVersionGet, SingleVersionRange,
	SingleVersionRangeRev, SingleVersionRemove, SingleVersionSet, SingleVersionStore,
	tier::{RangeCursor, Store, TierStorage},
};

#[async_trait]
impl SingleVersionGet for StandardTransactionStore {
	#[instrument(name = "store::single::get", level = "trace", skip(self), fields(key_hex = %hex::encode(key.as_ref())))]
	async fn get(&self, key: &EncodedKey) -> crate::Result<Option<SingleVersionValues>> {
		// Single-version storage uses TableId::Single for all keys
		let table = Store::Single;

		// Try hot tier first
		if let Some(hot) = &self.hot {
			if let Some(value) = hot.get(table, key.as_ref()).await? {
				return Ok(Some(SingleVersionValues {
					key: key.clone(),
					values: EncodedValues(CowVec::new(value)),
				}));
			}
		}

		// Try warm tier
		if let Some(warm) = &self.warm {
			if let Some(value) = warm.get(table, key.as_ref()).await? {
				return Ok(Some(SingleVersionValues {
					key: key.clone(),
					values: EncodedValues(CowVec::new(value)),
				}));
			}
		}

		// Try cold tier
		if let Some(cold) = &self.cold {
			if let Some(value) = cold.get(table, key.as_ref()).await? {
				return Ok(Some(SingleVersionValues {
					key: key.clone(),
					values: EncodedValues(CowVec::new(value)),
				}));
			}
		}

		Ok(None)
	}
}

#[async_trait]
impl SingleVersionContains for StandardTransactionStore {
	#[instrument(name = "store::single::contains", level = "trace", skip(self), fields(key_hex = %hex::encode(key.as_ref())), ret)]
	async fn contains(&self, key: &EncodedKey) -> crate::Result<bool> {
		let table = Store::Single;

		if let Some(hot) = &self.hot {
			if hot.contains(table, key.as_ref()).await? {
				return Ok(true);
			}
		}

		if let Some(warm) = &self.warm {
			if warm.contains(table, key.as_ref()).await? {
				return Ok(true);
			}
		}

		if let Some(cold) = &self.cold {
			if cold.contains(table, key.as_ref()).await? {
				return Ok(true);
			}
		}

		Ok(false)
	}
}

#[async_trait]
impl SingleVersionCommit for StandardTransactionStore {
	#[instrument(name = "store::single::commit", level = "debug", skip(self, deltas), fields(delta_count = deltas.len()))]
	async fn commit(&mut self, deltas: CowVec<Delta>) -> crate::Result<()> {
		let table = Store::Single;

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
				} => (key.as_ref().to_vec(), Some(values.as_ref().to_vec())),
				Delta::Remove {
					key,
				} => (key.as_ref().to_vec(), None),
				Delta::Drop {
					key,
					..
				} => (key.as_ref().to_vec(), None),
			})
			.collect();

		storage.set(HashMap::from([(table, entries)])).await?;

		Ok(())
	}
}

impl SingleVersionSet for StandardTransactionStore {}
impl SingleVersionRemove for StandardTransactionStore {}

#[async_trait]
impl SingleVersionRange for StandardTransactionStore {
	#[instrument(name = "store::single::range_batch", level = "debug", skip(self), fields(batch_size = batch_size))]
	async fn range_batch(&self, range: EncodedKeyRange, batch_size: u64) -> crate::Result<SingleVersionBatch> {
		let table = Store::Single;
		let mut all_entries: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();

		let (start, end) = make_range_bounds(&range);

		// Helper to process all batches from a tier until exhausted
		async fn process_tier<S: TierStorage>(
			storage: &S,
			table: Store,
			start: &Bound<Vec<u8>>,
			end: &Bound<Vec<u8>>,
			all_entries: &mut BTreeMap<Vec<u8>, Option<Vec<u8>>>,
		) -> crate::Result<()> {
			let mut cursor = RangeCursor::new();

			loop {
				let batch = storage
					.range_next(table, &mut cursor, bound_as_ref(start), bound_as_ref(end), 4096)
					.await?;

				for entry in batch.entries {
					// Only insert if not already present (first tier wins)
					all_entries.entry(entry.key).or_insert(entry.value);
				}

				if cursor.exhausted {
					break;
				}
			}

			Ok(())
		}

		// Process each tier (first one with a value for a key wins)
		if let Some(hot) = &self.hot {
			process_tier(hot, table, &start, &end, &mut all_entries).await?;
		}
		if let Some(warm) = &self.warm {
			process_tier(warm, table, &start, &end, &mut all_entries).await?;
		}
		if let Some(cold) = &self.cold {
			process_tier(cold, table, &start, &end, &mut all_entries).await?;
		}

		// Convert to SingleVersionValues, filtering out tombstones
		let items: Vec<SingleVersionValues> = all_entries
			.into_iter()
			.filter_map(|(key_bytes, value)| {
				value.map(|val| SingleVersionValues {
					key: EncodedKey(CowVec::new(key_bytes)),
					values: EncodedValues(CowVec::new(val)),
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

#[async_trait]
impl SingleVersionRangeRev for StandardTransactionStore {
	#[instrument(name = "store::single::range_rev_batch", level = "debug", skip(self), fields(batch_size = batch_size))]
	async fn range_rev_batch(&self, range: EncodedKeyRange, batch_size: u64) -> crate::Result<SingleVersionBatch> {
		let table = Store::Single;
		let mut all_entries: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();

		let (start, end) = make_range_bounds(&range);

		// Helper to process all reverse batches from a tier until exhausted
		async fn process_tier_rev<S: TierStorage>(
			storage: &S,
			table: Store,
			start: &Bound<Vec<u8>>,
			end: &Bound<Vec<u8>>,
			all_entries: &mut BTreeMap<Vec<u8>, Option<Vec<u8>>>,
		) -> crate::Result<()> {
			let mut cursor = RangeCursor::new();

			loop {
				let batch = storage
					.range_rev_next(
						table,
						&mut cursor,
						bound_as_ref(start),
						bound_as_ref(end),
						4096,
					)
					.await?;

				for entry in batch.entries {
					// Only insert if not already present (first tier wins)
					all_entries.entry(entry.key).or_insert(entry.value);
				}

				if cursor.exhausted {
					break;
				}
			}

			Ok(())
		}

		// Process each tier (first one with a value for a key wins)
		if let Some(hot) = &self.hot {
			process_tier_rev(hot, table, &start, &end, &mut all_entries).await?;
		}
		if let Some(warm) = &self.warm {
			process_tier_rev(warm, table, &start, &end, &mut all_entries).await?;
		}
		if let Some(cold) = &self.cold {
			process_tier_rev(cold, table, &start, &end, &mut all_entries).await?;
		}

		// Convert to SingleVersionValues in reverse order, filtering out tombstones
		let items: Vec<SingleVersionValues> = all_entries
			.into_iter()
			.rev() // Reverse for descending order
			.filter_map(|(key_bytes, value)| {
				value.map(|val| SingleVersionValues {
					key: EncodedKey(CowVec::new(key_bytes)),
					values: EncodedValues(CowVec::new(val)),
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

impl SingleVersionStore for StandardTransactionStore {}

/// Helper to convert owned Bound to ref
fn bound_as_ref(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
	match bound {
		Bound::Included(v) => Bound::Included(v.as_slice()),
		Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
		Bound::Unbounded => Bound::Unbounded,
	}
}

/// Convert EncodedKeyRange to primitive storage bounds (owned for async)
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
