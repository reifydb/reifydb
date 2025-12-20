// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ops::Bound;

use reifydb_core::{CommitVersion, CowVec, interface::Cdc, value::encoded::EncodedValues};

use crate::{
	CdcStore, StandardTransactionStore,
	backend::{BackendStorage, PrimitiveStorage, TableId},
	cdc::{CdcCount, CdcGet, CdcRange, CdcScan, InternalCdc, codec::decode_internal_cdc},
	store::cdc_iterator::CdcMergingIterator,
};

/// Encode a version as a key for CDC storage
fn version_to_key(version: CommitVersion) -> Vec<u8> {
	version.0.to_be_bytes().to_vec()
}

/// Helper function to get InternalCdc from primitive storage
fn get_internal_cdc<S: PrimitiveStorage>(
	storage: &S,
	version: CommitVersion,
) -> reifydb_type::Result<Option<InternalCdc>> {
	let table = TableId::Cdc;
	let key = version_to_key(version);

	if let Some(value) = storage.get(table, &key)? {
		let encoded = EncodedValues(CowVec::new(value));
		let internal = decode_internal_cdc(&encoded)?;
		Ok(Some(internal))
	} else {
		Ok(None)
	}
}

/// Helper function to convert InternalCdc to public Cdc
/// Note: This is a simplified version that doesn't resolve values from multi-version store
fn internal_to_public_cdc(internal: InternalCdc, store: &StandardTransactionStore) -> reifydb_type::Result<Cdc> {
	use crate::cdc::converter::CdcConverter;
	// The store implements MultiVersionGet which CdcConverter uses
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

/// Iterator over CDC entries from primitive storage
pub struct PrimitiveCdcIter<'a> {
	iter: <BackendStorage as PrimitiveStorage>::RangeIter<'a>,
	store: &'a StandardTransactionStore,
}

impl<'a> Iterator for PrimitiveCdcIter<'a> {
	type Item = Cdc;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			let entry = self.iter.next()?.ok()?;

			let value = entry.value?; // Skip tombstones
			let encoded = EncodedValues(CowVec::new(value));
			let internal = decode_internal_cdc(&encoded).ok()?;

			// Convert to public CDC
			match internal_to_public_cdc(internal, self.store) {
				Ok(cdc) => return Some(cdc),
				Err(_) => continue, // Skip on conversion error
			}
		}
	}
}

impl CdcRange for StandardTransactionStore {
	type RangeIter<'a> = Box<dyn Iterator<Item = Cdc> + 'a>;

	fn range(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
	) -> reifydb_type::Result<Self::RangeIter<'_>> {
		let mut iters: Vec<Box<dyn Iterator<Item = Cdc> + '_>> = Vec::new();

		let (start_key, end_key) = make_cdc_range_bounds(start, end);

		if let Some(hot) = &self.hot {
			let iter = hot.range(TableId::Cdc, bound_ref(&start_key), bound_ref(&end_key), 1024)?;
			iters.push(Box::new(PrimitiveCdcIter {
				iter,
				store: self,
			}));
		}

		if let Some(warm) = &self.warm {
			let iter = warm.range(TableId::Cdc, bound_ref(&start_key), bound_ref(&end_key), 1024)?;
			iters.push(Box::new(PrimitiveCdcIter {
				iter,
				store: self,
			}));
		}

		if let Some(cold) = &self.cold {
			let iter = cold.range(TableId::Cdc, bound_ref(&start_key), bound_ref(&end_key), 1024)?;
			iters.push(Box::new(PrimitiveCdcIter {
				iter,
				store: self,
			}));
		}

		Ok(Box::new(CdcMergingIterator::new(iters)))
	}
}

impl CdcScan for StandardTransactionStore {
	type ScanIter<'a> = Box<dyn Iterator<Item = Cdc> + 'a>;

	fn scan(&self) -> reifydb_type::Result<Self::ScanIter<'_>> {
		// Scan is just a range with unbounded start and end
		self.range(Bound::Unbounded, Bound::Unbounded)
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

/// Convert owned Bound<Vec<u8>> to Bound<&[u8]>
fn bound_ref(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
	match bound {
		Bound::Included(v) => Bound::Included(v.as_slice()),
		Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
		Bound::Unbounded => Bound::Unbounded,
	}
}
