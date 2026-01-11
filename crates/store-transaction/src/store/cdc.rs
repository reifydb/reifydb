// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{collections::BTreeMap, ops::Bound};

use reifydb_core::{CommitVersion, interface::Cdc, value::encoded::EncodedValues};

use crate::{
	CdcStore, StandardTransactionStore,
	cdc::{CdcBatch, CdcCount, CdcGet, CdcRange, InternalCdc, codec::decode_internal_cdc, converter::CdcConverter},
	tier::{EntryKind, RangeCursor, TierStorage},
};

/// Encode a version as a key for CDC storage
fn version_to_key(version: CommitVersion) -> Vec<u8> {
	version.0.to_be_bytes().to_vec()
}

/// Decode a version from a CDC key
fn key_to_version(key: &[u8]) -> Option<CommitVersion> {
	if key.len() == 8 {
		let bytes: [u8; 8] = key.try_into().ok()?;
		Some(CommitVersion(u64::from_be_bytes(bytes)))
	} else {
		None
	}
}

/// Helper function to get InternalCdc from primitive storage
fn get_internal_cdc<S: TierStorage>(storage: &S, version: CommitVersion) -> reifydb_type::Result<Option<InternalCdc>> {
	let table = EntryKind::Cdc;
	let key = version_to_key(version);

	if let Some(value) = storage.get(table, &key)? {
		let encoded = EncodedValues(value);
		let internal = decode_internal_cdc(&encoded)?;
		Ok(Some(internal))
	} else {
		Ok(None)
	}
}

fn internal_to_public_cdc(internal: InternalCdc, store: &StandardTransactionStore) -> reifydb_type::Result<Cdc> {
	store.convert(internal)
}

impl CdcGet for StandardTransactionStore {
	fn get(&self, version: CommitVersion) -> reifydb_type::Result<Option<Cdc>> {
		// Try hot tier first
		if let Some(hot) = &self.hot {
			if let Some(internal) = get_internal_cdc(hot, version)? {
				return Ok(Some(internal_to_public_cdc(internal, self)?));
			}
		}

		// Try warm tier
		if let Some(warm) = &self.warm {
			if let Some(internal) = get_internal_cdc(warm, version)? {
				return Ok(Some(internal_to_public_cdc(internal, self)?));
			}
		}

		// Try cold tier
		if let Some(cold) = &self.cold {
			if let Some(internal) = get_internal_cdc(cold, version)? {
				return Ok(Some(internal_to_public_cdc(internal, self)?));
			}
		}

		Ok(None)
	}
}

impl CdcRange for StandardTransactionStore {
	fn range_batch(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
		batch_size: u64,
	) -> reifydb_type::Result<CdcBatch> {
		let mut all_entries: BTreeMap<CommitVersion, InternalCdc> = BTreeMap::new();

		let (start_key, end_key) = make_cdc_range_bounds(start, end);
		let batch_size = batch_size as usize;

		// Helper to process batches from a tier until exhausted or enough entries
		fn process_tier<S: TierStorage>(
			storage: &S,
			start_key: Bound<&[u8]>,
			end_key: Bound<&[u8]>,
			all_entries: &mut BTreeMap<CommitVersion, InternalCdc>,
		) -> reifydb_type::Result<()> {
			let mut cursor = RangeCursor::new();

			loop {
				let batch =
					storage.range_next(EntryKind::Cdc, &mut cursor, start_key, end_key, 4096)?;

				for entry in batch.entries {
					if let Some(version) = key_to_version(&entry.key) {
						if let Some(value) = entry.value {
							let encoded = EncodedValues(value);
							if let Ok(internal) = decode_internal_cdc(&encoded) {
								// Only insert if not already present (first tier wins)
								all_entries.entry(version).or_insert(internal);
							}
						}
					}
				}

				if cursor.exhausted {
					break;
				}
			}

			Ok(())
		}

		let start_bound = bound_as_ref(&start_key);
		let end_bound = bound_as_ref(&end_key);

		// Process each tier (first one with a value for a version wins)
		if let Some(hot) = &self.hot {
			process_tier(hot, start_bound, end_bound, &mut all_entries)?;
		}
		if let Some(warm) = &self.warm {
			process_tier(warm, start_bound, end_bound, &mut all_entries)?;
		}
		if let Some(cold) = &self.cold {
			process_tier(cold, start_bound, end_bound, &mut all_entries)?;
		}

		// Convert to public Cdc
		let mut items: Vec<Cdc> = Vec::new();
		for (_, internal) in all_entries.into_iter().take(batch_size) {
			match internal_to_public_cdc(internal, self) {
				Ok(cdc) => items.push(cdc),
				Err(err) => unreachable!("cdc conversion should never fail: {}", err),
			}
		}

		let has_more = items.len() >= batch_size;

		Ok(CdcBatch {
			items,
			has_more,
		})
	}
}

/// Helper to convert owned Bound to ref
fn bound_as_ref(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
	match bound {
		Bound::Included(v) => Bound::Included(v.as_slice()),
		Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
		Bound::Unbounded => Bound::Unbounded,
	}
}

impl CdcCount for StandardTransactionStore {
	fn count(&self, version: CommitVersion) -> reifydb_type::Result<usize> {
		// Get the CDC at this version and count its changes
		if let Some(cdc) = CdcGet::get(self, version)? {
			Ok(cdc.changes.len())
		} else {
			Ok(0)
		}
	}
}

impl CdcStore for StandardTransactionStore {}

/// Convert CommitVersion bounds to byte key bounds
fn make_cdc_range_bounds(start: Bound<CommitVersion>, end: Bound<CommitVersion>) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
	let start_key = match start {
		Bound::Included(v) => Bound::Included(version_to_key(v)),
		Bound::Excluded(v) => Bound::Excluded(version_to_key(v)),
		Bound::Unbounded => Bound::Unbounded,
	};

	let end_key = match end {
		Bound::Included(v) => Bound::Included(version_to_key(v)),
		Bound::Excluded(v) => Bound::Excluded(version_to_key(v)),
		Bound::Unbounded => Bound::Unbounded,
	};

	(start_key, end_key)
}
