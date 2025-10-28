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
		} => InternalCdcChange::Delete {
			key,
			pre_version: pre_version.expect("Delete must have pre_version"),
		},
	}
}
