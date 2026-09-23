// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::log::{LogIndex, LogVersion};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn(u64);

impl Lsn {
	pub const FIRST: Self = Self(1);

	pub const fn new(value: u64) -> Self {
		Self(value)
	}

	pub const fn as_u64(self) -> u64 {
		self.0
	}

	pub fn from_version(version: LogVersion) -> Option<Self> {
		match version {
			LogVersion::ZERO => None,
			found => Some(Self(found.as_u64())),
		}
	}
}

impl From<Lsn> for LogVersion {
	fn from(lsn: Lsn) -> Self {
		LogVersion::new(lsn.as_u64())
	}
}

impl From<Lsn> for LogIndex {
	fn from(lsn: Lsn) -> Self {
		LogIndex::new(lsn.as_u64())
	}
}
