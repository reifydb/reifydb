// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::{Bound, ControlFlow};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::common::CommitVersion;
use reifydb_value::{byte_size::ByteSize, util::cowvec::CowVec};

use crate::store::EvictedVersion;

pub trait Rows: Send + Sync + 'static {
	fn empty(&self) -> Self;
}

pub trait Read: Rows {
	fn get(&self, key: &[u8], version: CommitVersion) -> Option<(CommitVersion, Option<CowVec<u8>>)>;

	fn scan(
		&self,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		reverse: bool,
		visit: impl FnMut(&EncodedKey, &[(CommitVersion, &Option<CowVec<u8>>)]) -> ControlFlow<()>,
	);

	fn scan_closed(
		&self,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		cutoff: CommitVersion,
		visit: impl FnMut(&EncodedKey, &[(CommitVersion, &Option<CowVec<u8>>)]) -> ControlFlow<()>,
	);

	fn versions(&self, key: &[u8]) -> Vec<CommitVersion>;

	fn stats(&self) -> RowStats;
}

pub trait Write: Rows {
	fn insert(&self, version: CommitVersion, rows: Vec<(EncodedKey, Option<CowVec<u8>>)>) -> ByteSize;

	fn close(&self);
}

pub trait Remove: Rows {
	fn remove(&self, pairs: Vec<(EncodedKey, CommitVersion)>) -> (Vec<EvictedVersion>, Vec<EncodedKey>);
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RowStats {
	pub current_bytes: ByteSize,
	pub historical_bytes: ByteSize,
	pub keys: u64,
	pub oldest: Option<CommitVersion>,
	pub active_oldest: Option<CommitVersion>,
}
