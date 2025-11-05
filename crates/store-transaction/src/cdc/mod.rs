// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub(crate) mod codec;
pub(crate) mod converter;
mod layout;

use std::collections::Bound;

pub(crate) use reifydb_core::delta::Delta;
use reifydb_core::{
	CommitVersion, EncodedKey,
	interface::{Cdc, KeyKind},
	key::Key,
};

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
				if kind == KeyKind::FlowNodeState || kind == KeyKind::CdcConsumer {
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
				if kind == KeyKind::FlowNodeState || kind == KeyKind::CdcConsumer {
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

/// Process optimized deltas and generate CDC changes
///
/// NOTE: This function expects deltas that are ALREADY OPTIMIZED at the delta level.
/// All cancellation (Insert+Delete) and coalescing (Update+Update) has already been done.
/// This function converts each optimized delta to the appropriate CDC change, with one
/// exception: it collapses Delete→Insert patterns in the same transaction into just Insert.
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
			Err(_) => return Err(reifydb_type::Error(crate::backend::diagnostic::sequence_exhausted())),
		};

		let key = delta.key().clone();

		// Get the pre-version from storage (if it exists)
		let pre_version = get_storage_version(&key);

		// Generate CDC change based on the optimized delta
		if let Some(cdc_change) = generate_internal_cdc_change(delta, pre_version, version) {
			// Check if this is an Insert or Update following a Delete in the same transaction
			if let Some(last_change) = cdc_changes.last_mut() {
				if let InternalCdcChange::Delete {
					key: last_key,
					pre_version: last_pre_version,
				} = &last_change.change
				{
					if last_key == &key && *last_pre_version != version {
						// Delete (from storage) + Insert/Update (new) in same transaction
						// Convert to Insert (complete replacement)
						match cdc_change {
							InternalCdcChange::Insert {
								..
							} => {
								last_change.change = cdc_change;
								continue;
							}
							InternalCdcChange::Update {
								key,
								pre_version: _,
								post_version,
							} => {
								// Convert Update to Insert
								last_change.change = InternalCdcChange::Insert {
									key,
									post_version,
								};
								continue;
							}
							_ => {}
						}
					}
				}
			}

			cdc_changes.push(InternalCdcSequencedChange {
				sequence,
				change: cdc_change,
			});
		}
	}

	Ok(cdc_changes)
}
