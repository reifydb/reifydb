// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ops::Bound;

use reifydb_core::{
	CowVec, EncodedKey, EncodedKeyRange, delta::Delta, interface::SingleVersionValues,
	value::encoded::EncodedValues,
};
use reifydb_type::util::hex;
use tracing::instrument;

use super::{StandardTransactionStore, single_iterator::SingleVersionMergingIterator};
use crate::{
	SingleVersionCommit, SingleVersionContains, SingleVersionGet, SingleVersionIter, SingleVersionRange,
	SingleVersionRangeRev, SingleVersionRemove, SingleVersionSet, SingleVersionStore,
	backend::{PrimitiveStorage, TableId, result::SingleVersionIterResult},
};

impl SingleVersionGet for StandardTransactionStore {
	#[instrument(level = "trace", skip(self), fields(key_hex = %hex::encode(key.as_ref())))]
	fn get(&self, key: &EncodedKey) -> crate::Result<Option<SingleVersionValues>> {
		// Single-version storage uses TableId::Single for all keys
		let table = TableId::Single;

		// Try hot tier first
		if let Some(hot) = &self.hot {
			if let Some(value) = hot.get(table, key.as_ref())? {
				return Ok(Some(SingleVersionValues {
					key: key.clone(),
					values: EncodedValues(CowVec::new(value)),
				}));
			}
		}

		// Try warm tier
		if let Some(warm) = &self.warm {
			if let Some(value) = warm.get(table, key.as_ref())? {
				return Ok(Some(SingleVersionValues {
					key: key.clone(),
					values: EncodedValues(CowVec::new(value)),
				}));
			}
		}

		// Try cold tier
		if let Some(cold) = &self.cold {
			if let Some(value) = cold.get(table, key.as_ref())? {
				return Ok(Some(SingleVersionValues {
					key: key.clone(),
					values: EncodedValues(CowVec::new(value)),
				}));
			}
		}

		Ok(None)
	}
}

impl SingleVersionContains for StandardTransactionStore {
	#[instrument(level = "trace", skip(self), fields(key_hex = %hex::encode(key.as_ref())), ret)]
	fn contains(&self, key: &EncodedKey) -> crate::Result<bool> {
		let table = TableId::Single;

		if let Some(hot) = &self.hot {
			if hot.contains(table, key.as_ref())? {
				return Ok(true);
			}
		}

		if let Some(warm) = &self.warm {
			if warm.contains(table, key.as_ref())? {
				return Ok(true);
			}
		}

		if let Some(cold) = &self.cold {
			if cold.contains(table, key.as_ref())? {
				return Ok(true);
			}
		}

		Ok(false)
	}
}

impl SingleVersionCommit for StandardTransactionStore {
	#[instrument(level = "debug", skip(self, deltas), fields(delta_count = deltas.len()))]
	fn commit(&mut self, deltas: CowVec<Delta>) -> crate::Result<()> {
		let table = TableId::Single;

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

		// Process deltas as a batch
		let entries: Vec<_> = deltas
			.iter()
			.map(|delta| match delta {
				Delta::Set {
					key,
					values,
				} => (key.as_ref() as &[u8], Some(values.as_ref() as &[u8])),
				Delta::Remove {
					key,
				} => (key.as_ref() as &[u8], None),
			})
			.collect();

		storage.put(table, &entries)?;

		Ok(())
	}
}

impl SingleVersionSet for StandardTransactionStore {}
impl SingleVersionRemove for StandardTransactionStore {}

use crate::backend::BackendStorage;

/// Iterator over single-version range results from primitive storage
pub struct PrimitiveSingleVersionRangeIter<'a> {
	iter: <BackendStorage as PrimitiveStorage>::RangeIter<'a>,
}

impl<'a> PrimitiveSingleVersionRangeIter<'a> {
	fn new(iter: <BackendStorage as PrimitiveStorage>::RangeIter<'a>) -> Self {
		Self {
			iter,
		}
	}
}

impl<'a> Iterator for PrimitiveSingleVersionRangeIter<'a> {
	type Item = SingleVersionIterResult;

	fn next(&mut self) -> Option<Self::Item> {
		let entry = self.iter.next()?.ok()?;
		let key = EncodedKey(CowVec::new(entry.key));

		Some(match entry.value {
			Some(value) => SingleVersionIterResult::Value(SingleVersionValues {
				key,
				values: EncodedValues(CowVec::new(value)),
			}),
			None => SingleVersionIterResult::Tombstone {
				key,
			},
		})
	}
}

/// Reverse iterator over single-version range results from primitive storage
pub struct PrimitiveSingleVersionRangeRevIter<'a> {
	iter: <BackendStorage as PrimitiveStorage>::RangeRevIter<'a>,
}

impl<'a> PrimitiveSingleVersionRangeRevIter<'a> {
	fn new(iter: <BackendStorage as PrimitiveStorage>::RangeRevIter<'a>) -> Self {
		Self {
			iter,
		}
	}
}

impl<'a> Iterator for PrimitiveSingleVersionRangeRevIter<'a> {
	type Item = SingleVersionIterResult;

	fn next(&mut self) -> Option<Self::Item> {
		let entry = self.iter.next()?.ok()?;
		let key = EncodedKey(CowVec::new(entry.key));

		Some(match entry.value {
			Some(value) => SingleVersionIterResult::Value(SingleVersionValues {
				key,
				values: EncodedValues(CowVec::new(value)),
			}),
			None => SingleVersionIterResult::Tombstone {
				key,
			},
		})
	}
}

impl SingleVersionRange for StandardTransactionStore {
	type Range<'a>
		= Box<dyn SingleVersionIter + 'a>
	where
		Self: 'a;

	#[instrument(level = "debug", skip(self))]
	fn range(&self, range: EncodedKeyRange) -> crate::Result<Self::Range<'_>> {
		let table = TableId::Single;
		let mut iters: Vec<Box<dyn Iterator<Item = SingleVersionIterResult> + Send + '_>> = Vec::new();

		let (start, end) = make_range_bounds(&range);

		if let Some(hot) = &self.hot {
			let iter = hot.range(table, start.clone(), end.clone(), 1024)?;
			iters.push(Box::new(PrimitiveSingleVersionRangeIter::new(iter)));
		}

		if let Some(warm) = &self.warm {
			let iter = warm.range(table, start.clone(), end.clone(), 1024)?;
			iters.push(Box::new(PrimitiveSingleVersionRangeIter::new(iter)));
		}

		if let Some(cold) = &self.cold {
			let iter = cold.range(table, start, end, 1024)?;
			iters.push(Box::new(PrimitiveSingleVersionRangeIter::new(iter)));
		}

		Ok(Box::new(SingleVersionMergingIterator::new(iters)))
	}
}

impl SingleVersionRangeRev for StandardTransactionStore {
	type RangeRev<'a>
		= Box<dyn SingleVersionIter + 'a>
	where
		Self: 'a;

	#[instrument(level = "debug", skip(self))]
	fn range_rev(&self, range: EncodedKeyRange) -> crate::Result<Self::RangeRev<'_>> {
		let table = TableId::Single;
		let mut iters: Vec<Box<dyn Iterator<Item = SingleVersionIterResult> + Send + '_>> = Vec::new();

		let (start, end) = make_range_bounds(&range);

		if let Some(hot) = &self.hot {
			let iter = hot.range_rev(table, start.clone(), end.clone(), 1024)?;
			iters.push(Box::new(PrimitiveSingleVersionRangeRevIter::new(iter)));
		}

		if let Some(warm) = &self.warm {
			let iter = warm.range_rev(table, start.clone(), end.clone(), 1024)?;
			iters.push(Box::new(PrimitiveSingleVersionRangeRevIter::new(iter)));
		}

		if let Some(cold) = &self.cold {
			let iter = cold.range_rev(table, start, end, 1024)?;
			iters.push(Box::new(PrimitiveSingleVersionRangeRevIter::new(iter)));
		}

		Ok(Box::new(SingleVersionMergingIterator::new(iters)))
	}
}

impl SingleVersionStore for StandardTransactionStore {}

/// Convert EncodedKeyRange to primitive storage bounds
fn make_range_bounds(range: &EncodedKeyRange) -> (Bound<&[u8]>, Bound<&[u8]>) {
	let start = match &range.start {
		Bound::Included(key) => Bound::Included(key.as_ref() as &[u8]),
		Bound::Excluded(key) => Bound::Excluded(key.as_ref() as &[u8]),
		Bound::Unbounded => Bound::Unbounded,
	};

	let end = match &range.end {
		Bound::Included(key) => Bound::Included(key.as_ref() as &[u8]),
		Bound::Excluded(key) => Bound::Excluded(key.as_ref() as &[u8]),
		Bound::Unbounded => Bound::Unbounded,
	};

	(start, end)
}
