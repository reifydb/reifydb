// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ops::Bound;

use reifydb_type::Result;

use super::CdcEvent;
use crate::CommitVersion;

/// Combined trait for all CDC storage operations
pub trait CdcStorage: Send + Sync + Clone + 'static + CdcGet + CdcRange + CdcScan + CdcCount {}

/// Retrieve CDC events for a specific version
pub trait CdcGet: Send + Sync {
	fn get(&self, version: CommitVersion) -> Result<Vec<CdcEvent>>;
}

/// Query CDC events within a version range
pub trait CdcRange: Send + Sync {
	type RangeIter<'a>: Iterator<Item = CdcEvent> + 'a
	where
		Self: 'a;

	fn range(&self, start: Bound<CommitVersion>, end: Bound<CommitVersion>) -> Result<Self::RangeIter<'_>>;
}

/// Scan all CDC events
pub trait CdcScan: Send + Sync {
	type ScanIter<'a>: Iterator<Item = CdcEvent> + 'a
	where
		Self: 'a;

	fn scan(&self) -> Result<Self::ScanIter<'_>>;
}

/// Count CDC events for a specific version
pub trait CdcCount: Send + Sync {
	fn count(&self, version: CommitVersion) -> Result<usize>;
}
