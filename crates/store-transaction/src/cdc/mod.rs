// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub(crate) mod codec;
pub(crate) mod converter;
mod exclude;
mod layout;

use std::collections::Bound;

use exclude::should_exclude_from_cdc;
pub(crate) use reifydb_core::delta::Delta;
use reifydb_core::{CommitVersion, EncodedKey, interface::Cdc, key::Key};
use reifydb_type::diagnostic::internal::internal;

pub trait CdcStore: Send + Sync + Clone + 'static + CdcGet + CdcRange + CdcCount {}

/// A batch of CDC range results with continuation info.
#[derive(Debug, Clone)]
pub struct CdcBatch {
	/// The CDC entries in this batch.
	pub items: Vec<Cdc>,
	/// Whether there are more items after this batch.
	pub has_more: bool,
}

impl CdcBatch {
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

pub trait CdcGet: Send + Sync {
	fn get(&self, version: CommitVersion) -> reifydb_type::Result<Option<Cdc>>;
}

pub trait CdcRange: Send + Sync {
	/// Fetch a batch of CDC entries in version order (ascending).
	fn range_batch(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
		batch_size: u64,
	) -> reifydb_type::Result<CdcBatch>;

	/// Convenience method with default batch size.
	fn range(&self, start: Bound<CommitVersion>, end: Bound<CommitVersion>) -> reifydb_type::Result<CdcBatch> {
		self.range_batch(start, end, 1024)
	}

	/// Scan all CDC entries.
	fn scan(&self, batch_size: u64) -> reifydb_type::Result<CdcBatch> {
		self.range_batch(Bound::Unbounded, Bound::Unbounded, batch_size)
	}
}

pub trait CdcCount: Send + Sync {
	fn count(&self, version: CommitVersion) -> reifydb_type::Result<usize>;
}

/// Internal representation of CDC change with version references
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum InternalCdcChange {
	Insert {
		key: EncodedKey,
		post_version: CommitVersion,
	},
	Update {
		key: EncodedKey,
		pre_version: CommitVersion,
		post_version: CommitVersion,
	},
	Delete {
		key: EncodedKey,
		pre_version: CommitVersion,
	},
}

impl InternalCdcChange {
	/// Get the key for this change.
	pub fn key(&self) -> &EncodedKey {
		match self {
			InternalCdcChange::Insert {
				key,
				..
			} => key,
			InternalCdcChange::Update {
				key,
				..
			} => key,
			InternalCdcChange::Delete {
				key,
				..
			} => key,
		}
	}
}

/// Internal representation of CDC with version references
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct InternalCdc {
	pub version: CommitVersion,
	pub timestamp: u64,
	pub changes: Vec<InternalCdcSequencedChange>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct InternalCdcSequencedChange {
	pub sequence: u16,
	pub change: InternalCdcChange,
}

/// Generate an internal CDC change from a Delta
fn generate_internal_cdc_change(
	delta: Delta,
	pre_version: Option<CommitVersion>,
	post_version: CommitVersion,
) -> Option<InternalCdcChange> {
	match delta {
		Delta::Set {
			key,
			values: _,
		} => {
			if let Some(kind) = Key::kind(&key) {
				if should_exclude_from_cdc(kind) {
					return None;
				}
			}

			if let Some(pre_version) = pre_version {
				Some(InternalCdcChange::Update {
					key,
					pre_version,
					post_version,
				})
			} else {
				Some(InternalCdcChange::Insert {
					key,
					post_version,
				})
			}
		}

		Delta::Remove {
			key,
		} => {
			if let Some(kind) = Key::kind(&key) {
				if should_exclude_from_cdc(kind) {
					return None;
				}
			}

			if let Some(pre_version) = pre_version {
				Some(InternalCdcChange::Delete {
					key,
					pre_version,
				})
			} else {
				None
			}
		}

		// Drop operations never generate CDC events - they are for internal cleanup
		Delta::Drop {
			..
		} => None,
	}
}

/// Process optimized deltas and generate CDC changes
///
/// NOTE: This function expects deltas that are ALREADY OPTIMIZED at the delta level.
/// All cancellation (Insert+Delete) and coalescing (Update+Update) has already been done.
/// This function simply converts each optimized delta to the appropriate CDC change
/// without any additional optimization or collapsing.
pub(crate) fn process_deltas_for_cdc<F>(
	deltas: impl IntoIterator<Item = Delta>,
	version: CommitVersion,
	mut get_storage_version: F,
) -> reifydb_type::Result<Vec<InternalCdcSequencedChange>>
where
	F: FnMut(&EncodedKey) -> Option<CommitVersion>,
{
	let mut cdc_changes: Vec<InternalCdcSequencedChange> = Vec::new();

	for (idx, delta) in deltas.into_iter().enumerate() {
		let sequence = match u16::try_from(idx + 1) {
			Ok(seq) => seq,
			Err(_) => return Err(reifydb_type::error!(internal("CDC sequence number exhausted"))),
		};

		let key = delta.key().clone();

		// Get the pre-version from storage (if it exists)
		let pre_version = get_storage_version(&key);

		// Generate CDC change based on the optimized delta
		if let Some(cdc_change) = generate_internal_cdc_change(delta, pre_version, version) {
			cdc_changes.push(InternalCdcSequencedChange {
				sequence,
				change: cdc_change,
			});
		}
	}

	Ok(cdc_changes)
}
