// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	collections::{BTreeMap, HashMap, HashSet},
	ops::{Bound, RangeBounds},
};

use async_trait::async_trait;
use drop::find_keys_to_drop;
use reifydb_core::{
	CommitVersion, CowVec, EncodedKey, EncodedKeyRange, delta::Delta, interface::MultiVersionValues,
	util::clock::now_millis, value::encoded::EncodedValues,
};
use reifydb_type::util::hex;
use tracing::instrument;

use super::{
	StandardTransactionStore, drop,
	router::{classify_key, is_single_version_semantics_key},
	version::{VERSION_SIZE, VersionedGetResult, encode_versioned_key, get_at_version},
};
use crate::{
	MultiVersionBatch, MultiVersionCommit, MultiVersionContains, MultiVersionGet, MultiVersionRange,
	MultiVersionRangeRev, MultiVersionStore,
	cdc::{InternalCdc, codec::encode_internal_cdc, process_deltas_for_cdc},
	hot::{Store::Multi, delta_optimizer::optimize_deltas},
	stats::{PreVersionInfo, Tier},
	tier::{Store, TierStorage},
};

#[async_trait]
impl MultiVersionGet for StandardTransactionStore {
	#[instrument(name = "store::multi::get", level = "trace", skip(self), fields(key_hex = %hex::encode(key.as_ref()), version = version.0))]
	async fn get(&self, key: &EncodedKey, version: CommitVersion) -> crate::Result<Option<MultiVersionValues>> {
		let table = classify_key(key);

		// Try hot tier first
		if let Some(hot) = &self.hot {
			match get_at_version(hot, table, key.as_ref(), version).await? {
				VersionedGetResult::Value {
					value,
					version: v,
				} => {
					return Ok(Some(MultiVersionValues {
						key: key.clone(),
						values: EncodedValues(CowVec::new(value)),
						version: v,
					}));
				}
				VersionedGetResult::Tombstone => return Ok(None),
				VersionedGetResult::NotFound => {}
			}
		}

		// Try warm tier
		if let Some(warm) = &self.warm {
			match get_at_version(warm, table, key.as_ref(), version).await? {
				VersionedGetResult::Value {
					value,
					version: v,
				} => {
					return Ok(Some(MultiVersionValues {
						key: key.clone(),
						values: EncodedValues(CowVec::new(value)),
						version: v,
					}));
				}
				VersionedGetResult::Tombstone => return Ok(None),
				VersionedGetResult::NotFound => {}
			}
		}

		// Try cold tier
		if let Some(cold) = &self.cold {
			match get_at_version(cold, table, key.as_ref(), version).await? {
				VersionedGetResult::Value {
					value,
					version: v,
				} => {
					return Ok(Some(MultiVersionValues {
						key: key.clone(),
						values: EncodedValues(CowVec::new(value)),
						version: v,
					}));
				}
				VersionedGetResult::Tombstone => return Ok(None),
				VersionedGetResult::NotFound => {}
			}
		}

		Ok(None)
	}
}

#[async_trait]
impl MultiVersionContains for StandardTransactionStore {
	#[instrument(name = "store::multi::contains", level = "trace", skip(self), fields(key_hex = %hex::encode(key.as_ref()), version = version.0), ret)]
	async fn contains(&self, key: &EncodedKey, version: CommitVersion) -> crate::Result<bool> {
		Ok(MultiVersionGet::get(self, key, version).await?.is_some())
	}
}

#[async_trait]
impl MultiVersionCommit for StandardTransactionStore {
	#[instrument(name = "store::multi::commit", level = "info", skip(self, deltas), fields(delta_count = deltas.len(), version = version.0))]
	async fn commit(&self, deltas: CowVec<Delta>, version: CommitVersion) -> crate::Result<()> {
		// Get the hot storage tier (warm and cold are placeholders for now)
		let Some(storage) = &self.hot else {
			return Ok(());
		};

		// Optimize deltas first (cancel insert+delete pairs, coalesce updates)
		let optimized_deltas = optimize_deltas(deltas.iter().cloned());

		// For flow state keys (single-version semantics), inject Drop deltas to clean up old versions.
		// Track which keys have pending Set operations so Drop can account for the version being written.
		let (all_deltas, pending_set_keys): (Vec<Delta>, HashSet<Vec<u8>>) = {
			let mut result = Vec::with_capacity(optimized_deltas.len() * 2);
			let mut pending_keys = HashSet::new();
			for delta in optimized_deltas.iter() {
				result.push(delta.clone());
				if let Delta::Set {
					key,
					..
				} = delta
				{
					if is_single_version_semantics_key(key) {
						pending_keys.insert(key.as_ref().to_vec());
						result.push(Delta::Drop {
							key: key.clone(),
							up_to_version: None,
							keep_last_versions: Some(1),
						});
					}
				}
			}
			(result, pending_keys)
		};

		let previous_versions = self.collect_previous_versions(&optimized_deltas).await;

		// Track storage statistics
		// TODO this should happen in the background
		for delta in optimized_deltas.iter() {
			let key = delta.key();
			let key_bytes = key.as_ref();
			let table = classify_key(key);

			// Look up previous value to calculate size delta
			let pre_version_info = self.get_previous_value_info(table, key_bytes).await;

			// Versioned key size = original key + VERSION_SIZE
			let versioned_key_bytes = (key_bytes.len() + VERSION_SIZE) as u64;

			match delta {
				Delta::Set {
					values,
					..
				} => {
					self.stats_tracker.record_write(
						Tier::Hot,
						key_bytes,
						versioned_key_bytes,
						values.len() as u64,
						pre_version_info,
					);
				}
				Delta::Remove {
					..
				} => {
					self.stats_tracker.record_delete(
						Tier::Hot,
						key_bytes,
						versioned_key_bytes,
						pre_version_info,
					);
				}
				Delta::Drop {
					..
				} => {
					// Drop operations are internal cleanup - stats tracked per deleted version
				}
			}
		}

		let cdc_changes = process_deltas_for_cdc(optimized_deltas, version, |key| {
			previous_versions.get(key.as_ref()).copied()
		})?;

		// Batch deltas by table for efficient storage writes
		let mut batches: HashMap<Store, Vec<(Vec<u8>, Option<Vec<u8>>)>> = HashMap::new();

		for delta in all_deltas.iter() {
			let table = classify_key(delta.key());

			match delta {
				Delta::Set {
					key,
					values,
				} => {
					let versioned_key = encode_versioned_key(key.as_ref(), version);
					batches.entry(table)
						.or_default()
						.push((versioned_key, Some(values.as_ref().to_vec())));
				}
				Delta::Remove {
					key,
				} => {
					let versioned_key = encode_versioned_key(key.as_ref(), version);
					batches.entry(table).or_default().push((versioned_key, None));
				}
				Delta::Drop {
					key,
					up_to_version,
					keep_last_versions,
				} => {
					// Drop scans for versioned entries and deletes them based on constraints.
					// For single-version keys with a pending Set in this commit, pass the version
					// so find_keys_to_drop knows about the new version being written.
					let pending_version = if pending_set_keys.contains(key.as_ref()) {
						Some(version)
					} else {
						None
					};
					let entries_to_drop = find_keys_to_drop(
						storage,
						table,
						key.as_ref(),
						*up_to_version,
						*keep_last_versions,
						pending_version,
					)
					.await?;
					for entry in entries_to_drop {
						// Track storage reduction for each dropped entry
						self.stats_tracker.record_drop(
							Tier::Hot,
							key.as_ref(),
							entry.versioned_key.len() as u64,
							entry.value_bytes,
						);
						batches.entry(table).or_default().push((entry.versioned_key, None));
					}
				}
			}
		}

		// Add CDC to batches
		let cdc_data = if !cdc_changes.is_empty() {
			let internal_cdc = InternalCdc {
				version,
				timestamp: now_millis(),
				changes: cdc_changes,
			};
			let encoded = encode_internal_cdc(&internal_cdc)?;
			let cdc_key = version.0.to_be_bytes();

			// Track CDC bytes per source object
			let num_changes = internal_cdc.changes.len() as u64;
			let total_key_bytes: u64 =
				internal_cdc.changes.iter().map(|c| c.change.key().len() as u64).sum();
			let overhead_bytes = (encoded.len() as u64).saturating_sub(total_key_bytes);
			let per_change_overhead = overhead_bytes / num_changes.max(1);

			for change in &internal_cdc.changes {
				let change_key = change.change.key();
				self.stats_tracker.record_cdc_for_change(Tier::Hot, change_key, per_change_overhead, 1);
			}

			Some((cdc_key.to_vec(), encoded.to_vec()))
		} else {
			None
		};

		if let Some((cdc_key, encoded)) = cdc_data {
			batches.entry(Store::Cdc).or_default().push((cdc_key, Some(encoded)));
		}

		storage.set(batches).await?;

		// Checkpoint stats if needed
		if self.stats_tracker.should_checkpoint() {
			self.stats_tracker.checkpoint(storage).await?;
		}

		Ok(())
	}
}

impl StandardTransactionStore {
	/// Get information about the previous value of a key for stats tracking .
	async fn get_previous_value_info(&self, table: Store, key: &[u8]) -> Option<PreVersionInfo> {
		// Try to get the latest version from any tier
		async fn get_value<S: TierStorage>(storage: &S, table: Store, key: &[u8]) -> Option<(u64, u64)> {
			match get_at_version(storage, table, key, CommitVersion(u64::MAX)).await {
				Ok(VersionedGetResult::Value {
					value,
					..
				}) => {
					let versioned_key_bytes = (key.len() + VERSION_SIZE) as u64;
					Some((versioned_key_bytes, value.len() as u64))
				}
				_ => None,
			}
		}

		// Check tiers in order
		if let Some(hot) = &self.hot {
			if let Some((key_bytes, value_bytes)) = get_value(hot, table, key).await {
				return Some(PreVersionInfo {
					key_bytes,
					value_bytes,
				});
			}
		}
		if let Some(warm) = &self.warm {
			if let Some((key_bytes, value_bytes)) = get_value(warm, table, key).await {
				return Some(PreVersionInfo {
					key_bytes,
					value_bytes,
				});
			}
		}
		if let Some(cold) = &self.cold {
			if let Some((key_bytes, value_bytes)) = get_value(cold, table, key).await {
				return Some(PreVersionInfo {
					key_bytes,
					value_bytes,
				});
			}
		}

		None
	}

	/// Collect previous versions for all keys in the delta list.
	/// Used for CDC to determine Insert vs Update operations.
	async fn collect_previous_versions(&self, deltas: &[Delta]) -> HashMap<Vec<u8>, CommitVersion> {
		use super::version::get_latest_version;

		let mut version_map = HashMap::new();

		for delta in deltas {
			let key = delta.key();
			let key_bytes = key.as_ref();
			let table = classify_key(key);

			// Only need to check for Set and Remove operations
			match delta {
				Delta::Set {
					..
				}
				| Delta::Remove {
					..
				} => {
					// Check all tiers for the latest version
					if let Some(hot) = &self.hot {
						if let Ok(Some(version)) =
							get_latest_version(hot, table, key_bytes).await
						{
							version_map.insert(key_bytes.to_vec(), version);
							continue;
						}
					}
					if let Some(warm) = &self.warm {
						if let Ok(Some(version)) =
							get_latest_version(warm, table, key_bytes).await
						{
							version_map.insert(key_bytes.to_vec(), version);
							continue;
						}
					}
					if let Some(cold) = &self.cold {
						if let Ok(Some(version)) =
							get_latest_version(cold, table, key_bytes).await
						{
							version_map.insert(key_bytes.to_vec(), version);
						}
					}
				}
				Delta::Drop {
					..
				} => {
					// Drop operations don't need CDC tracking
				}
			}
		}

		version_map
	}
}

#[async_trait]
impl MultiVersionRange for StandardTransactionStore {
	#[instrument(name = "store::multi::range", level = "debug", skip(self), fields(version = version.0, batch_size = batch_size))]
	async fn range_batch(
		&self,
		range: EncodedKeyRange,
		version: CommitVersion,
		batch_size: u64,
	) -> crate::Result<MultiVersionBatch> {
		use super::version::{extract_key, extract_version};

		let mut all_entries: BTreeMap<Vec<u8>, (CommitVersion, Option<Vec<u8>>)> = BTreeMap::new();

		// Helper to process a batch from a tier
		async fn process_tier_batch<S: TierStorage>(
			storage: &S,
			range: &EncodedKeyRange,
			version: CommitVersion,
			batch_size: u64,
			all_entries: &mut BTreeMap<Vec<u8>, (CommitVersion, Option<Vec<u8>>)>,
		) -> crate::Result<()> {
			let table = classify_key_range(range);
			let (start, end) = make_versioned_range_bounds(range);

			// With descending version encoding, forward scan returns newest versions first.
			let batch = storage
				.range_batch(table, Bound::Included(start), Bound::Included(end), batch_size as usize)
				.await?;

			for entry in batch.entries {
				if let (Some(original_key), Some(entry_version)) =
					(extract_key(&entry.key), extract_version(&entry.key))
				{
					// Skip if version is greater than requested
					if entry_version > version {
						continue;
					}

					// Skip if key is not within the requested range
					let original_key_encoded = EncodedKey(CowVec::new(original_key.clone()));
					if !range.contains(&original_key_encoded) {
						continue;
					}

					// Update if no entry exists or this is a higher version
					let should_update = match all_entries.get(&original_key) {
						None => true,
						Some((existing_version, _)) => entry_version > *existing_version,
					};

					if should_update {
						all_entries.insert(original_key, (entry_version, entry.value));
					}
				}
			}

			Ok(())
		}

		// Process each tier
		if let Some(hot) = &self.hot {
			process_tier_batch(hot, &range, version, batch_size, &mut all_entries).await?;
		}
		if let Some(warm) = &self.warm {
			process_tier_batch(warm, &range, version, batch_size, &mut all_entries).await?;
		}
		if let Some(cold) = &self.cold {
			process_tier_batch(cold, &range, version, batch_size, &mut all_entries).await?;
		}

		// Convert to MultiVersionValues, filtering out tombstones
		let items: Vec<MultiVersionValues> = all_entries
			.into_iter()
			.filter_map(|(key_bytes, (v, value))| {
				value.map(|val| MultiVersionValues {
					key: EncodedKey(CowVec::new(key_bytes)),
					values: EncodedValues(CowVec::new(val)),
					version: v,
				})
			})
			.take(batch_size as usize)
			.collect();

		let has_more = items.len() >= batch_size as usize;

		Ok(MultiVersionBatch {
			items,
			has_more,
		})
	}
}

#[async_trait]
impl MultiVersionRangeRev for StandardTransactionStore {
	#[instrument(name = "store::multi::range_rev", level = "debug", skip(self), fields(version = version.0, batch_size = batch_size))]
	async fn range_rev_batch(
		&self,
		range: EncodedKeyRange,
		version: CommitVersion,
		batch_size: u64,
	) -> crate::Result<MultiVersionBatch> {
		use super::version::{extract_key, extract_version};

		let mut all_entries: BTreeMap<Vec<u8>, (CommitVersion, Option<Vec<u8>>)> = BTreeMap::new();

		// Helper to process a batch from a tier (reverse)
		async fn process_tier_batch_rev<S: TierStorage>(
			storage: &S,
			range: &EncodedKeyRange,
			version: CommitVersion,
			batch_size: u64,
			all_entries: &mut BTreeMap<Vec<u8>, (CommitVersion, Option<Vec<u8>>)>,
		) -> crate::Result<()> {
			let table = classify_key_range(range);
			let (start, end) = make_versioned_range_bounds(range);

			// With descending version encoding, forward scan returns newest versions first.
			let batch = storage
				.range_batch(table, Bound::Included(start), Bound::Included(end), batch_size as usize)
				.await?;

			for entry in batch.entries {
				if let (Some(original_key), Some(entry_version)) =
					(extract_key(&entry.key), extract_version(&entry.key))
				{
					// Skip if version is greater than requested
					if entry_version > version {
						continue;
					}

					// Skip if key is not within the requested range
					let original_key_encoded = EncodedKey(CowVec::new(original_key.clone()));
					if !range.contains(&original_key_encoded) {
						continue;
					}

					// Update if no entry exists or this is a higher version
					let should_update = match all_entries.get(&original_key) {
						None => true,
						Some((existing_version, _)) => entry_version > *existing_version,
					};

					if should_update {
						all_entries.insert(original_key, (entry_version, entry.value));
					}
				}
			}

			Ok(())
		}

		// Process each tier
		if let Some(hot) = &self.hot {
			process_tier_batch_rev(hot, &range, version, batch_size, &mut all_entries).await?;
		}
		if let Some(warm) = &self.warm {
			process_tier_batch_rev(warm, &range, version, batch_size, &mut all_entries).await?;
		}
		if let Some(cold) = &self.cold {
			process_tier_batch_rev(cold, &range, version, batch_size, &mut all_entries).await?;
		}

		// Convert to MultiVersionValues in reverse order, filtering out tombstones
		let items: Vec<MultiVersionValues> = all_entries
			.into_iter()
			.rev() // Reverse for descending order
			.filter_map(|(key_bytes, (v, value))| {
				value.map(|val| MultiVersionValues {
					key: EncodedKey(CowVec::new(key_bytes)),
					values: EncodedValues(CowVec::new(val)),
					version: v,
				})
			})
			.take(batch_size as usize)
			.collect();

		let has_more = items.len() >= batch_size as usize;

		Ok(MultiVersionBatch {
			items,
			has_more,
		})
	}
}

impl MultiVersionStore for StandardTransactionStore {}

/// Classify a range to determine which table it belongs to.
fn classify_key_range(range: &EncodedKeyRange) -> Store {
	use super::router::classify_range;

	classify_range(range).unwrap_or(Multi)
}

/// - Version MAX encodes to 0x00..00 (smallest bytes)
/// - Version 0 encodes to 0xFF..FF (largest bytes)
///
/// For range queries, we need start <= end in byte order:
/// - Start uses version MAX to get the smallest encoded value
/// - End uses version 0 to get the largest encoded value
/// The actual key range and version filtering happens after retrieval.
fn make_versioned_range_bounds(range: &EncodedKeyRange) -> (Vec<u8>, Vec<u8>) {
	let start = match &range.start {
		// Version MAX encodes smallest, capturing all versions of this key
		Bound::Included(key) => encode_versioned_key(key.as_ref(), CommitVersion(u64::MAX)),
		Bound::Excluded(key) => encode_versioned_key(key.as_ref(), CommitVersion(u64::MAX)),
		Bound::Unbounded => encode_versioned_key(&[], CommitVersion(u64::MAX)),
	};

	let end = match &range.end {
		// Version 0 encodes largest, capturing all versions of this key
		Bound::Included(key) => encode_versioned_key(key.as_ref(), CommitVersion(0)),
		Bound::Excluded(key) => encode_versioned_key(key.as_ref(), CommitVersion(0)),
		Bound::Unbounded => encode_versioned_key(&[0xFFu8; 256], CommitVersion(0)),
	};

	(start, end)
}
