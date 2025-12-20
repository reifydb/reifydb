// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	collections::HashMap,
	ops::{Bound, RangeBounds},
};

use reifydb_core::{
	CommitVersion, CowVec, EncodedKey, EncodedKeyRange, delta::Delta, interface::MultiVersionValues,
	util::clock::now_millis, value::encoded::EncodedValues,
};
use reifydb_type::util::hex;
use tracing::instrument;

use super::{
	StandardTransactionStore, drop,
	router::{classify_key, is_single_version_semantics_key},
	version_manager::{VERSION_SIZE, VersionedGetResult, encode_versioned_key, get_at_version, get_latest_version},
};
use crate::{
	MultiVersionCommit, MultiVersionContains, MultiVersionGet, MultiVersionIter, MultiVersionRange,
	MultiVersionRangeRev, MultiVersionStore,
	backend::{PrimitiveStorage, TableId, delta_optimizer::optimize_deltas, result::MultiVersionIterResult},
	cdc::{InternalCdc, codec::encode_internal_cdc, process_deltas_for_cdc},
	stats::{PreVersionInfo, Tier},
	store::multi_iterator::{MultiVersionMergingIterator, MultiVersionMergingRevIterator},
};

impl MultiVersionGet for StandardTransactionStore {
	#[instrument(name = "store::multi::get", level = "trace", skip(self), fields(key_hex = %hex::encode(key.as_ref()), version = version.0))]
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> crate::Result<Option<MultiVersionValues>> {
		let table = classify_key(key);

		// Try hot tier first
		if let Some(hot) = &self.hot {
			match get_at_version(hot, table, key.as_ref(), version)? {
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
			match get_at_version(warm, table, key.as_ref(), version)? {
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
			match get_at_version(cold, table, key.as_ref(), version)? {
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

impl MultiVersionContains for StandardTransactionStore {
	#[instrument(name = "store::multi::contains", level = "trace", skip(self), fields(key_hex = %hex::encode(key.as_ref()), version = version.0), ret)]
	fn contains(&self, key: &EncodedKey, version: CommitVersion) -> crate::Result<bool> {
		Ok(MultiVersionGet::get(self, key, version)?.is_some())
	}
}

impl MultiVersionCommit for StandardTransactionStore {
	#[instrument(name = "store::multi::commit", level = "info", skip(self, deltas), fields(delta_count = deltas.len(), version = version.0))]
	fn commit(&self, deltas: CowVec<Delta>, version: CommitVersion) -> crate::Result<()> {
		// Get the first available storage tier
		let storage = if let Some(hot) = &self.hot {
			hot
		} else if let Some(warm) = &self.warm {
			warm
		} else if let Some(cold) = &self.cold {
			cold
		} else {
			return Ok(());
		};

		// Helper to check if key exists in storage
		let key_exists = |key: &EncodedKey| -> bool {
			let table = classify_key(key);
			if let Some(hot) = &self.hot {
				if let Ok(Some(_)) = get_latest_version(hot, table, key.as_ref()) {
					return true;
				}
			}
			if let Some(warm) = &self.warm {
				if let Ok(Some(_)) = get_latest_version(warm, table, key.as_ref()) {
					return true;
				}
			}
			if let Some(cold) = &self.cold {
				if let Ok(Some(_)) = get_latest_version(cold, table, key.as_ref()) {
					return true;
				}
			}
			false
		};

		// Optimize deltas first (cancel insert+delete pairs, coalesce updates)
		let optimized_deltas = optimize_deltas(deltas.iter().cloned(), key_exists);

		// For flow state keys (single-version semantics), inject Drop deltas to clean up old versions.
		// This is done after optimization but before CDC processing since:
		// - Flow state keys are excluded from CDC anyway
		// - We want atomic cleanup in the same commit batch
		let all_deltas: Vec<Delta> = {
			let mut result = Vec::with_capacity(optimized_deltas.len() * 2);
			for delta in optimized_deltas.iter() {
				result.push(delta.clone());
				if let Delta::Set {
					key,
					..
				} = delta
				{
					if is_single_version_semantics_key(key) {
						result.push(Delta::Drop {
							key: key.clone(),
							up_to_version: None,
							keep_last_versions: Some(1),
						});
					}
				}
			}
			result
		};

		// Generate CDC changes from optimized deltas
		let cdc_changes = process_deltas_for_cdc(optimized_deltas.iter().cloned(), version, |key| {
			let table = classify_key(key);
			// Look up existing version in all tiers
			if let Some(hot) = &self.hot {
				if let Ok(Some(v)) = get_latest_version(hot, table, key.as_ref()) {
					return Some(v);
				}
			}
			if let Some(warm) = &self.warm {
				if let Ok(Some(v)) = get_latest_version(warm, table, key.as_ref()) {
					return Some(v);
				}
			}
			if let Some(cold) = &self.cold {
				if let Ok(Some(v)) = get_latest_version(cold, table, key.as_ref()) {
					return Some(v);
				}
			}
			None
		})?;

		// Track storage statistics
		for delta in optimized_deltas.iter() {
			let key = delta.key();
			let key_bytes = key.as_ref();
			let table = classify_key(key);

			// Look up previous value to calculate size delta
			let pre_version_info = self.get_previous_value_info(table, key_bytes);

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
					// below
				}
			}
		}

		// Batch deltas by table for efficient storage writes
		// Use all_deltas (includes injected Drop ops for single-version-semantics keys)
		let mut batches: HashMap<TableId, Vec<(Vec<u8>, Option<Vec<u8>>)>> = HashMap::new();

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
					// Drop scans for versioned entries and deletes them based on constraints
					let entries_to_drop = find_keys_to_drop(
						storage,
						table,
						key.as_ref(),
						*up_to_version,
						*keep_last_versions,
					)?;

					if !entries_to_drop.is_empty() {
						// Aggregate all bytes for this logical key's dropped versions
						let total_key_bytes: u64 = entries_to_drop
							.iter()
							.map(|e| e.versioned_key.len() as u64)
							.sum();
						let total_value_bytes: u64 =
							entries_to_drop.iter().map(|e| e.value_bytes).sum();
						let count = entries_to_drop.len() as u64;

						// Single accounting update for all dropped versions
						self.stats_tracker.record_drop(
							Tier::Hot,
							key.as_ref(),
							total_key_bytes,
							total_value_bytes,
							count,
						);

						// Queue deletions
						for entry in entries_to_drop {
							batches.entry(table)
								.or_default()
								.push((entry.versioned_key, None));
						}
					}
				}
			}
		}

		// Write each batch to storage
		for (table, entries) in batches {
			let refs: Vec<(&[u8], Option<&[u8]>)> =
				entries.iter().map(|(k, v)| (k.as_slice(), v.as_deref())).collect();
			storage.put(table, &refs)?;
		}

		// Store CDC if there are any changes
		if !cdc_changes.is_empty() {
			let internal_cdc = InternalCdc {
				version,
				timestamp: now_millis(),
				changes: cdc_changes,
			};

			let encoded = encode_internal_cdc(&internal_cdc)?;
			let cdc_key = version.0.to_be_bytes();
			storage.put(TableId::Cdc, &[(&cdc_key[..], Some(encoded.as_ref()))])?;

			// Track CDC bytes per source object
			// Distribute the encoded size proportionally among changes
			let num_changes = internal_cdc.changes.len() as u64;
			let total_key_bytes: u64 =
				internal_cdc.changes.iter().map(|c| c.change.key().len() as u64).sum();
			let overhead_bytes = (encoded.len() as u64).saturating_sub(total_key_bytes);
			let per_change_overhead = overhead_bytes / num_changes.max(1);

			for change in &internal_cdc.changes {
				let change_key = change.change.key();
				self.stats_tracker.record_cdc_for_change(Tier::Hot, change_key, per_change_overhead, 1);
			}
		}

		// Checkpoint stats if needed
		if self.stats_tracker.should_checkpoint() {
			self.stats_tracker.checkpoint(storage)?;
		}

		Ok(())
	}
}

impl StandardTransactionStore {
	/// Get information about the previous value of a key for stats tracking.
	///
	/// Returns the key and value sizes of the latest version if the key exists.
	/// Key size includes the VERSION_SIZE suffix for versioned keys.
	fn get_previous_value_info(&self, table: TableId, key: &[u8]) -> Option<PreVersionInfo> {
		// Try to get the latest version from any tier
		let get_value = |storage: &BackendStorage| -> Option<(u64, u64)> {
			match get_at_version(storage, table, key, CommitVersion(u64::MAX)) {
				Ok(VersionedGetResult::Value {
					value,
					..
				}) => {
					// Return versioned key size (key + VERSION_SIZE)
					let versioned_key_bytes = (key.len() + VERSION_SIZE) as u64;
					Some((versioned_key_bytes, value.len() as u64))
				}
				_ => None,
			}
		};

		// Check tiers in order
		if let Some(hot) = &self.hot {
			if let Some((key_bytes, value_bytes)) = get_value(hot) {
				return Some(PreVersionInfo {
					key_bytes,
					value_bytes,
				});
			}
		}
		if let Some(warm) = &self.warm {
			if let Some((key_bytes, value_bytes)) = get_value(warm) {
				return Some(PreVersionInfo {
					key_bytes,
					value_bytes,
				});
			}
		}
		if let Some(cold) = &self.cold {
			if let Some((key_bytes, value_bytes)) = get_value(cold) {
				return Some(PreVersionInfo {
					key_bytes,
					value_bytes,
				});
			}
		}

		None
	}
}

use std::{collections::BTreeMap, marker::PhantomData, vec::IntoIter};

use drop::find_keys_to_drop;

use crate::backend::BackendStorage;

/// Iterator over multi-version range results from primitive storage.
/// Collects all entries, deduplicates by key (keeping latest version per key),
/// then sorts by original key for proper ordering.
/// Also filters keys to ensure they fall within the requested range.
pub struct PrimitiveMultiVersionRangeIter<'a> {
	/// Collected and sorted entries (original key -> (version, value))
	entries: IntoIter<(Vec<u8>, CommitVersion, Option<Vec<u8>>)>,
	_phantom: PhantomData<&'a ()>,
}

impl<'a> PrimitiveMultiVersionRangeIter<'a> {
	fn new(
		iter: <BackendStorage as PrimitiveStorage>::RangeIter<'a>,
		version: CommitVersion,
		key_range: &EncodedKeyRange,
	) -> Self {
		use super::version_manager::{extract_key, extract_version};

		// Collect all entries and find latest version for each key
		let mut key_map: BTreeMap<Vec<u8>, (CommitVersion, Option<Vec<u8>>)> = BTreeMap::new();

		for entry_result in iter {
			if let Ok(entry) = entry_result {
				if let (Some(original_key), Some(entry_version)) =
					(extract_key(&entry.key), extract_version(&entry.key))
				{
					// Skip if version is greater than requested
					if entry_version > version {
						continue;
					}

					// Skip if key is not within the requested range
					// This is necessary because versioned key encoding can cause
					// keys with different lengths to interleave incorrectly
					let original_key_encoded = EncodedKey(CowVec::new(original_key.to_vec()));
					if !key_range.contains(&original_key_encoded) {
						continue;
					}

					// Update if no entry exists or this is a higher version
					let should_update = match key_map.get(original_key) {
						None => true,
						Some((existing_version, _)) => entry_version > *existing_version,
					};

					if should_update {
						key_map.insert(original_key.to_vec(), (entry_version, entry.value));
					}
				}
			}
		}

		// Convert to sorted vector
		let entries: Vec<_> =
			key_map.into_iter().map(|(key, (version, value))| (key, version, value)).collect();

		Self {
			entries: entries.into_iter(),
			_phantom: PhantomData,
		}
	}
}

impl<'a> Iterator for PrimitiveMultiVersionRangeIter<'a> {
	type Item = MultiVersionIterResult;

	fn next(&mut self) -> Option<Self::Item> {
		let (key_bytes, version, value) = self.entries.next()?;
		let key = EncodedKey(CowVec::new(key_bytes));

		Some(match value {
			Some(v) => MultiVersionIterResult::Value(MultiVersionValues {
				key,
				values: EncodedValues(CowVec::new(v)),
				version,
			}),
			None => MultiVersionIterResult::Tombstone {
				key,
				version,
			},
		})
	}
}

impl MultiVersionRange for StandardTransactionStore {
	type RangeIter<'a>
		= Box<dyn MultiVersionIter + 'a>
	where
		Self: 'a;

	#[instrument(name = "store::multi::range", level = "debug", skip(self), fields(version = version.0, batch_size = batch_size))]
	fn range_batched(
		&self,
		range: EncodedKeyRange,
		version: CommitVersion,
		batch_size: u64,
	) -> crate::Result<Self::RangeIter<'_>> {
		let mut iters: Vec<Box<dyn Iterator<Item = MultiVersionIterResult> + Send + '_>> = Vec::new();

		// For each tier, we need to scan all versions up to the requested version
		// and filter to get the latest version per key

		if let Some(hot) = &self.hot {
			let table = classify_key_range(&range);
			let (start, end) = make_versioned_range_bounds(&range, version);
			let iter = hot.range(
				table,
				Bound::Included(start.as_slice()),
				Bound::Included(end.as_slice()),
				batch_size as usize,
			)?;
			iters.push(Box::new(PrimitiveMultiVersionRangeIter::new(iter, version, &range)));
		}

		if let Some(warm) = &self.warm {
			let table = classify_key_range(&range);
			let (start, end) = make_versioned_range_bounds(&range, version);
			let iter = warm.range(
				table,
				Bound::Included(start.as_slice()),
				Bound::Included(end.as_slice()),
				batch_size as usize,
			)?;
			iters.push(Box::new(PrimitiveMultiVersionRangeIter::new(iter, version, &range)));
		}

		if let Some(cold) = &self.cold {
			let table = classify_key_range(&range);
			let (start, end) = make_versioned_range_bounds(&range, version);
			let iter = cold.range(
				table,
				Bound::Included(start.as_slice()),
				Bound::Included(end.as_slice()),
				batch_size as usize,
			)?;
			iters.push(Box::new(PrimitiveMultiVersionRangeIter::new(iter, version, &range)));
		}

		Ok(Box::new(MultiVersionMergingIterator::new(iters)))
	}
}

/// Iterator over multi-version range results in reverse order.
/// Collects all entries, deduplicates by key (keeping latest version per key),
/// then sorts by original key in reverse order.
pub struct PrimitiveMultiVersionRangeRevIter<'a> {
	/// Collected and reverse-sorted entries
	entries: IntoIter<(Vec<u8>, CommitVersion, Option<Vec<u8>>)>,
	_phantom: PhantomData<&'a ()>,
}

impl<'a> PrimitiveMultiVersionRangeRevIter<'a> {
	fn new(
		iter: <BackendStorage as PrimitiveStorage>::RangeRevIter<'a>,
		version: CommitVersion,
		key_range: &EncodedKeyRange,
	) -> Self {
		use super::version_manager::{extract_key, extract_version};

		// Collect all entries and find latest version for each key
		let mut key_map: BTreeMap<Vec<u8>, (CommitVersion, Option<Vec<u8>>)> = BTreeMap::new();

		for entry_result in iter {
			if let Ok(entry) = entry_result {
				if let (Some(original_key), Some(entry_version)) =
					(extract_key(&entry.key), extract_version(&entry.key))
				{
					// Skip if version is greater than requested
					if entry_version > version {
						continue;
					}

					// Skip if key is not within the requested range
					let original_key_encoded = EncodedKey(CowVec::new(original_key.to_vec()));
					if !key_range.contains(&original_key_encoded) {
						continue;
					}

					// Update if no entry exists or this is a higher version
					let should_update = match key_map.get(original_key) {
						None => true,
						Some((existing_version, _)) => entry_version > *existing_version,
					};

					if should_update {
						key_map.insert(original_key.to_vec(), (entry_version, entry.value));
					}
				}
			}
		}

		// Convert to vector and reverse for descending order
		let entries: Vec<_> =
			key_map.into_iter().rev().map(|(key, (version, value))| (key, version, value)).collect();

		Self {
			entries: entries.into_iter(),
			_phantom: PhantomData,
		}
	}
}

impl<'a> Iterator for PrimitiveMultiVersionRangeRevIter<'a> {
	type Item = MultiVersionIterResult;

	fn next(&mut self) -> Option<Self::Item> {
		let (key_bytes, version, value) = self.entries.next()?;
		let key = EncodedKey(CowVec::new(key_bytes));

		Some(match value {
			Some(v) => MultiVersionIterResult::Value(MultiVersionValues {
				key,
				values: EncodedValues(CowVec::new(v)),
				version,
			}),
			None => MultiVersionIterResult::Tombstone {
				key,
				version,
			},
		})
	}
}

impl MultiVersionRangeRev for StandardTransactionStore {
	type RangeIterRev<'a>
		= Box<dyn MultiVersionIter + 'a>
	where
		Self: 'a;

	fn range_rev_batched(
		&self,
		range: EncodedKeyRange,
		version: CommitVersion,
		batch_size: u64,
	) -> crate::Result<Self::RangeIterRev<'_>> {
		let mut iters: Vec<Box<dyn Iterator<Item = MultiVersionIterResult> + Send + '_>> = Vec::new();

		// For reverse iteration, scan in reverse order
		if let Some(hot) = &self.hot {
			let table = classify_key_range(&range);
			let (start, end) = make_versioned_range_bounds(&range, version);
			let iter = hot.range_rev(
				table,
				Bound::Included(start.as_slice()),
				Bound::Included(end.as_slice()),
				batch_size as usize,
			)?;
			iters.push(Box::new(PrimitiveMultiVersionRangeRevIter::new(iter, version, &range)));
		}

		if let Some(warm) = &self.warm {
			let table = classify_key_range(&range);
			let (start, end) = make_versioned_range_bounds(&range, version);
			let iter = warm.range_rev(
				table,
				Bound::Included(start.as_slice()),
				Bound::Included(end.as_slice()),
				batch_size as usize,
			)?;
			iters.push(Box::new(PrimitiveMultiVersionRangeRevIter::new(iter, version, &range)));
		}

		if let Some(cold) = &self.cold {
			let table = classify_key_range(&range);
			let (start, end) = make_versioned_range_bounds(&range, version);
			let iter = cold.range_rev(
				table,
				Bound::Included(start.as_slice()),
				Bound::Included(end.as_slice()),
				batch_size as usize,
			)?;
			iters.push(Box::new(PrimitiveMultiVersionRangeRevIter::new(iter, version, &range)));
		}

		Ok(Box::new(MultiVersionMergingRevIterator::new(iters)))
	}
}

impl MultiVersionStore for StandardTransactionStore {}

/// Classify a range to determine which table it belongs to.
/// Falls back to Multi table if range spans multiple tables.
fn classify_key_range(range: &EncodedKeyRange) -> crate::backend::TableId {
	use super::router::classify_range;

	classify_range(range).unwrap_or(crate::backend::TableId::Multi)
}

/// Create versioned range bounds for primitive storage query.
///
/// For a key range [start, end] at version V, we want to scan:
/// - From: start with version 0
/// - To: end with version V
fn make_versioned_range_bounds(range: &EncodedKeyRange, version: CommitVersion) -> (Vec<u8>, Vec<u8>) {
	let start = match &range.start {
		Bound::Included(key) => encode_versioned_key(key.as_ref(), CommitVersion(0)),
		Bound::Excluded(key) => encode_versioned_key(key.as_ref(), CommitVersion(u64::MAX)),
		Bound::Unbounded => encode_versioned_key(&[], CommitVersion(0)),
	};

	let end = match &range.end {
		Bound::Included(key) => encode_versioned_key(key.as_ref(), version),
		Bound::Excluded(key) => encode_versioned_key(key.as_ref(), CommitVersion(0)),
		Bound::Unbounded => encode_versioned_key(&[0xFFu8; 256], version),
	};

	(start, end)
}
