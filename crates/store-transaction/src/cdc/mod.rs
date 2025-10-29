// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub(crate) mod codec;
pub(crate) mod converter;
mod layout;

use std::collections::Bound;

use reifydb_core::{CommitVersion, EncodedKey, interface::Cdc};
pub(crate) use reifydb_core::delta::Delta;

pub trait CdcStore: Send + Sync + Clone + 'static + CdcGet + CdcRange + CdcScan + CdcCount {}

pub trait CdcGet: Send + Sync {
	fn get(&self, version: CommitVersion) -> reifydb_type::Result<Option<Cdc>>;
}

pub trait CdcRange: Send + Sync {
	type RangeIter<'a>: Iterator<Item = Cdc> + 'a
	where
		Self: 'a;

	fn range(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
	) -> reifydb_type::Result<Self::RangeIter<'_>>;
}

pub trait CdcScan: Send + Sync {
	type ScanIter<'a>: Iterator<Item = Cdc> + 'a
	where
		Self: 'a;

	fn scan(&self) -> reifydb_type::Result<Self::ScanIter<'_>>;
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
pub(crate) fn generate_internal_cdc_change(
	delta: Delta,
	pre_version: Option<CommitVersion>,
	post_version: CommitVersion,
) -> InternalCdcChange {
	match delta {
		Delta::Set {
			key,
			values: _,
		} => {
			if let Some(pre_v) = pre_version {
				InternalCdcChange::Update {
					key,
					pre_version: pre_v,
					post_version,
				}
			} else {
				InternalCdcChange::Insert {
					key,
					post_version,
				}
			}
		}

		Delta::Remove {
			key,
		} => {
			// If there's no pre_version, use the post_version (current transaction version)
			// This happens when a key is inserted and deleted in the same transaction
			InternalCdcChange::Delete {
				key,
				pre_version: pre_version.unwrap_or(post_version),
			}
		}
	}
}

/// Process deltas and generate CDC changes with cancellation logic
pub(crate) fn process_deltas_for_cdc<F>(
	deltas: impl IntoIterator<Item = Delta>,
	version: CommitVersion,
	mut get_storage_version: F,
) -> reifydb_type::Result<Vec<InternalCdcSequencedChange>>
where
	F: FnMut(&EncodedKey) -> Option<CommitVersion>,
{
	use std::collections::HashMap;

	// Track CDC changes per key to handle cancellations
	let mut cdc_tracker: HashMap<EncodedKey, Vec<InternalCdcSequencedChange>> = HashMap::new();

	for (idx, delta) in deltas.into_iter().enumerate() {
		let sequence = match u16::try_from(idx + 1) {
			Ok(seq) => seq,
			Err(_) => return Err(reifydb_type::Error(crate::backend::diagnostic::sequence_exhausted())),
		};

		let key = delta.key().clone();
		let changes_for_key = cdc_tracker.get(&key);

		// Check if this key has been modified earlier in this transaction
		let pre_version = if changes_for_key.is_some() {
			// Key was modified earlier in this transaction
			// For CDC purposes, use the current version as pre_version
			Some(version)
		} else {
			// First time seeing this key in transaction, check storage
			get_storage_version(&key)
		};

		// Track CDC change for this key
		let changes = cdc_tracker.entry(key.clone()).or_insert_with(Vec::new);

		// Apply cancellation logic
		match &delta {
			Delta::Set { .. } => {
				// Add the change
				let cdc_change = generate_internal_cdc_change(delta, pre_version, version);
				changes.push(InternalCdcSequencedChange {
					sequence,
					change: cdc_change,
				});
			}
			Delta::Remove { .. } => {
				// Check if we should cancel with a previous insert
				let should_cancel = if let Some(first_change) = changes.first() {
					matches!(&first_change.change, InternalCdcChange::Insert { .. })
				} else {
					false
				};

				if should_cancel {
					// Cancel out insert + delete
					changes.clear();
				} else {
					// Add the delete
					let cdc_change = generate_internal_cdc_change(delta, pre_version, version);
					changes.push(InternalCdcSequencedChange {
						sequence,
						change: cdc_change,
					});
				}
			}
		}
	}

	// Collect all non-cancelled changes and renumber sequences
	let mut all_changes: Vec<InternalCdcSequencedChange> = cdc_tracker
		.into_values()
		.flatten()
		.collect();
	all_changes.sort_by_key(|c| c.sequence);

	// Renumber sequences
	let mut seq = 1u16;
	for change in &mut all_changes {
		change.sequence = seq;
		seq += 1;
	}

	Ok(all_changes)
}
