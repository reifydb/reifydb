// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	CowVec, EncodedKey, EncodedKeyRange, delta::Delta, interface::SingleVersionValues,
	value::encoded::EncodedValues,
};

/// Composite trait for single-version storage capabilities.
pub trait SingleVersionStore:
	Send
	+ Sync
	+ Clone
	+ SingleVersionCommit
	+ SingleVersionGet
	+ SingleVersionContains
	+ SingleVersionSet
	+ SingleVersionRemove
	+ SingleVersionRange
	+ SingleVersionRangeRev
	+ 'static
{
}

/// A batch of single-version range results with continuation info.
#[derive(Debug, Clone)]
pub struct SingleVersionBatch {
	/// The values in this batch.
	pub items: Vec<SingleVersionValues>,
	/// Whether there are more items after this batch.
	pub has_more: bool,
}

impl SingleVersionBatch {
	/// Creates an empty batch with no more results.
	pub fn empty() -> Self {
		Self {
			items: Vec::new(),
			has_more: false,
		}
	}

	/// Returns true if this batch contains no items.
	pub fn is_empty(&self) -> bool {
		self.items.is_empty()
	}
}

/// Trait for committing deltas to single-version storage.

pub trait SingleVersionCommit: Send + Sync {
	/// Commit a batch of deltas.
	fn commit(&mut self, deltas: CowVec<Delta>) -> crate::Result<()>;
}

/// Trait for getting values from single-version storage.

pub trait SingleVersionGet: Send + Sync {
	/// Get the value for a key.
	fn get(&self, key: &EncodedKey) -> crate::Result<Option<SingleVersionValues>>;
}

/// Trait for checking key existence in single-version storage.

pub trait SingleVersionContains: Send + Sync {
	/// Check if a key exists.
	fn contains(&self, key: &EncodedKey) -> crate::Result<bool>;
}

/// Trait for setting values in single-version storage.

pub trait SingleVersionSet: SingleVersionCommit {
	/// Set a value for a key.
	fn set(&mut self, key: &EncodedKey, values: EncodedValues) -> crate::Result<()> {
		Self::commit(
			self,
			CowVec::new(vec![Delta::Set {
				key: key.clone(),
				values: values.clone(),
			}]),
		)
	}
}

/// Trait for removing values from single-version storage.

pub trait SingleVersionRemove: SingleVersionCommit {
	/// Remove a key.
	fn remove(&mut self, key: &EncodedKey) -> crate::Result<()> {
		Self::commit(
			self,
			CowVec::new(vec![Delta::Remove {
				key: key.clone(),
			}]),
		)
	}
}

/// Trait for forward range queries with batch-fetch pattern.

pub trait SingleVersionRange: Send + Sync {
	/// Fetch a batch of values in key order (ascending).
	fn range_batch(&self, range: EncodedKeyRange, batch_size: u64) -> crate::Result<SingleVersionBatch>;

	/// Convenience method with default batch size.
	fn range(&self, range: EncodedKeyRange) -> crate::Result<SingleVersionBatch> {
		self.range_batch(range, 1024)
	}

	/// Range query with prefix.
	fn prefix(&self, prefix: &EncodedKey) -> crate::Result<SingleVersionBatch> {
		self.range(EncodedKeyRange::prefix(prefix))
	}
}

/// Trait for reverse range queries with batch-fetch pattern.

pub trait SingleVersionRangeRev: Send + Sync {
	/// Fetch a batch of values in reverse key order (descending).
	fn range_rev_batch(&self, range: EncodedKeyRange, batch_size: u64) -> crate::Result<SingleVersionBatch>;

	/// Convenience method with default batch size.
	fn range_rev(&self, range: EncodedKeyRange) -> crate::Result<SingleVersionBatch> {
		self.range_rev_batch(range, 1024)
	}

	/// Reverse range query with prefix.
	fn prefix_rev(&self, prefix: &EncodedKey) -> crate::Result<SingleVersionBatch> {
		self.range_rev(EncodedKeyRange::prefix(prefix))
	}
}
