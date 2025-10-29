// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub(crate) mod codec;
pub(crate) mod converter;
mod layout;

use std::collections::Bound;

pub(crate) use reifydb_core::delta::Delta;
use reifydb_core::{interface::Cdc, CommitVersion, EncodedKey};
use reifydb_core::interface::KeyKind;
use reifydb_core::key::Key;

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

			// operators do not generate cdc events
			if let Some(kind) = Key::kind(&key) {
				if kind == KeyKind::FlowNodeState{
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
			// operators do not produce cdc events
			if let Some(kind) = Key::kind(&key) {
				if kind == KeyKind::FlowNodeState{
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

		// Check if we had previous changes and if they were all cancelled
		let had_cancelled_changes = cdc_tracker.get(&key).map_or(false, |changes| changes.is_empty());
		let has_existing_changes = cdc_tracker.get(&key).map_or(false, |changes| !changes.is_empty());

		// Check if this key has been modified earlier in this transaction
		// and still has active changes (not all cancelled out)
		let pre_version = if has_existing_changes {
			// Key was modified earlier in this transaction and has active changes
			// For CDC purposes, use the current version as pre_version
			Some(version)
		} else if had_cancelled_changes {
			// All previous changes were cancelled, check storage
			get_storage_version(&key)
		} else {
			// First time seeing this key in transaction, check storage
			get_storage_version(&key)
		};

		// Track CDC change for this key
		let changes = cdc_tracker.entry(key.clone()).or_insert_with(Vec::new);

		// Apply cancellation and coalescing logic
		match &delta {
			Delta::Set {
				..
			} => {
				// Check if we need to coalesce with an existing change
				if let Some(last_change) = changes.last_mut() {
					match &mut last_change.change {
						InternalCdcChange::Insert {
							post_version,
							..
						} => {
							// Coalesce: Update the Insert's post_version
							*post_version = version;
						}
						InternalCdcChange::Update {
							post_version,
							..
						} => {
							// Coalesce: Update the Update's post_version
							*post_version = version;
						}
						InternalCdcChange::Delete {
							pre_version: delete_pre_version,
							..
						} => {
							// Delete followed by Insert
							// Check if the Delete is from storage or from this transaction
							if *delete_pre_version != version {
								// Delete is from storage (different version), keep both Delete and Insert
								let cdc_change = InternalCdcChange::Insert {
									key: key.clone(),
									post_version: version,
								};
								changes.push(InternalCdcSequencedChange {
									sequence,
									change: cdc_change,
								});
							} else {
								// Delete is from this transaction (same version)
								// This means we had Insert+Delete+Insert in same transaction
								// The Delete cancelled the first Insert, now we have a new Insert
								let cdc_change = InternalCdcChange::Insert {
									key: key.clone(),
									post_version: version,
								};
								changes.push(InternalCdcSequencedChange {
									sequence,
									change: cdc_change,
								});
							}
						}
					}
				} else {
					// First change for this key or after complete cancellation
					// Always use None as pre_version after cancellation to ensure Insert
					let effective_pre_version = if had_cancelled_changes {
						// After cancellation, treat as new insert
						None
					} else {
						pre_version
					};
					if let Some(cdc_change) =
						generate_internal_cdc_change(delta, effective_pre_version, version)
					{
						changes.push(InternalCdcSequencedChange {
							sequence,
							change: cdc_change,
						});
					}
				}
			}
			Delta::Remove {
				..
			} => {
				// Check what type of change we have so far
				if let Some(last_change) = changes.last_mut() {
					match &last_change.change {
						InternalCdcChange::Insert {
							..
						} => {
							// Insert + Delete = Complete cancellation
							changes.clear();
						}
						InternalCdcChange::Update {
							pre_version,
							..
						} => {
							// Update + Delete = Delete with original pre_version
							let saved_pre = *pre_version;
							last_change.change = InternalCdcChange::Delete {
								key: key.clone(),
								pre_version: saved_pre,
							};
						}
						InternalCdcChange::Delete {
							..
						} => {
							// Delete + Delete shouldn't happen, but if it does, keep the first
							// Do nothing - keep the existing Delete
						}
					}
				} else {
					// First change for this key is a Delete
					if let Some(cdc_change) =
						generate_internal_cdc_change(delta, pre_version, version)
					{
						changes.push(InternalCdcSequencedChange {
							sequence,
							change: cdc_change,
						});
					}
				}
			}
		}
	}

	// Collect all non-cancelled changes and renumber sequences
	let mut all_changes: Vec<InternalCdcSequencedChange> = cdc_tracker.into_values().flatten().collect();
	all_changes.sort_by_key(|c| c.sequence);

	// Renumber sequences
	let mut seq = 1u16;
	for change in &mut all_changes {
		change.sequence = seq;
		seq += 1;
	}

	Ok(all_changes)
}
