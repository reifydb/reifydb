// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{collections::BTreeMap, ops::Bound};

use async_trait::async_trait;
use reifydb_core::{CommitVersion, CowVec, interface::Cdc, value::encoded::EncodedValues};

use crate::{
	CdcStore, StandardTransactionStore,
	backend::{BackendStorage, PrimitiveStorage, TableId},
	cdc::{CdcBatch, CdcCount, CdcGet, CdcRange, InternalCdc, codec::decode_internal_cdc, converter::CdcConverter},
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
async fn get_internal_cdc<S: PrimitiveStorage>(
	storage: &S,
	version: CommitVersion,
) -> reifydb_type::Result<Option<InternalCdc>> {
	let table = TableId::Cdc;
	let key = version_to_key(version);

	if let Some(value) = storage.get(table, &key).await? {
		let encoded = EncodedValues(CowVec::new(value));
		let internal = decode_internal_cdc(&encoded)?;
		Ok(Some(internal))
	} else {
		Ok(None)
	}
}

async fn internal_to_public_cdc(internal: InternalCdc, store: &StandardTransactionStore) -> reifydb_type::Result<Cdc> {
	store.convert(internal).await
}

#[async_trait]
impl CdcGet for StandardTransactionStore {
	async fn get(&self, version: CommitVersion) -> reifydb_type::Result<Option<Cdc>> {
		// Try hot tier first
		if let Some(hot) = &self.hot {
			if let Some(internal) = get_internal_cdc(hot, version).await? {
				return Ok(Some(internal_to_public_cdc(internal, self).await?));
			}
		}

		// Try warm tier
		if let Some(warm) = &self.warm {
			if let Some(internal) = get_internal_cdc(warm, version).await? {
				return Ok(Some(internal_to_public_cdc(internal, self).await?));
			}
		}

		// Try cold tier
		if let Some(cold) = &self.cold {
			if let Some(internal) = get_internal_cdc(cold, version).await? {
				return Ok(Some(internal_to_public_cdc(internal, self).await?));
			}
		}

		Ok(None)
	}
}

#[async_trait]
impl CdcRange for StandardTransactionStore {
	async fn range_batch(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
		batch_size: u64,
	) -> reifydb_type::Result<CdcBatch> {
		let mut all_entries: BTreeMap<CommitVersion, InternalCdc> = BTreeMap::new();

		let (start_key, end_key) = make_cdc_range_bounds(start, end);

		// Helper to process a batch from a tier
		async fn process_tier_batch(
			storage: &BackendStorage,
			start: Bound<Vec<u8>>,
			end: Bound<Vec<u8>>,
			batch_size: u64,
			all_entries: &mut BTreeMap<CommitVersion, InternalCdc>,
		) -> reifydb_type::Result<()> {
			let batch = storage.range_batch(TableId::Cdc, start, end, batch_size as usize).await?;

			for entry in batch.entries {
				if let Some(version) = key_to_version(&entry.key) {
					if let Some(value) = entry.value {
						let encoded = EncodedValues(CowVec::new(value));
						if let Ok(internal) = decode_internal_cdc(&encoded) {
							// Only insert if not already present (first tier wins)
							all_entries.entry(version).or_insert(internal);
						}
					}
				}
			}

			Ok(())
		}

		// Process each tier (first one with a value for a version wins)
		if let Some(hot) = &self.hot {
			process_tier_batch(hot, start_key.clone(), end_key.clone(), batch_size, &mut all_entries)
				.await?;
		}
		if let Some(warm) = &self.warm {
			process_tier_batch(warm, start_key.clone(), end_key.clone(), batch_size, &mut all_entries)
				.await?;
		}
		if let Some(cold) = &self.cold {
			process_tier_batch(cold, start_key, end_key, batch_size, &mut all_entries).await?;
		}

		// Convert to public Cdc
		let mut items: Vec<Cdc> = Vec::new();
		for (_, internal) in all_entries.into_iter().take(batch_size as usize) {
			match internal_to_public_cdc(internal, self).await {
				Ok(cdc) => items.push(cdc),
				Err(err) => unreachable!("cdc conversion should never fail: {}", err),
			}
		}

		let has_more = items.len() >= batch_size as usize;

		Ok(CdcBatch {
			items,
			has_more,
		})
	}
}

#[async_trait]
impl CdcCount for StandardTransactionStore {
	async fn count(&self, version: CommitVersion) -> reifydb_type::Result<usize> {
		// Get the CDC at this version and count its changes
		if let Some(cdc) = CdcGet::get(self, version).await? {
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
