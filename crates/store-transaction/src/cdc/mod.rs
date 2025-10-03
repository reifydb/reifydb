// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub(crate) mod codec;
mod layout;

use std::collections::Bound;

use reifydb_core::{CommitVersion, interface::Cdc};
pub(crate) use reifydb_core::{delta::Delta, interface::CdcChange, value::encoded::EncodedValues};

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

/// Generate a CDC change from a Delta
pub(crate) fn generate_cdc_change(delta: Delta, pre: Option<EncodedValues>) -> CdcChange {
	match delta {
		Delta::Set {
			key,
			values,
		} => {
			if let Some(pre) = pre {
				CdcChange::Update {
					key,
					pre,
					post: values,
				}
			} else {
				CdcChange::Insert {
					key,
					post: values,
				}
			}
		}

		Delta::Remove {
			key,
		} => CdcChange::Delete {
			key,
			pre,
		},
	}
}
