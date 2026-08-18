// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{row::operator::EncodedOperatorRow, key::encoded::EncodedKey};
use reifydb_core::{common::CommitVersion, interface::store::MultiVersionRow};
use reifydb_value::{Result, error::Error as ValueError};

pub mod counter;
pub mod keyed;
pub mod raw;
pub mod row;
pub mod single;
#[cfg(test)]
pub mod test_utils;
pub mod utils;
pub mod window;

use reifydb_core::key::{EncodableKey, operator_state::OperatorStateKey};

pub struct StateIterator<'a> {
	inner: Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + 'a>,
}

impl<'a> StateIterator<'a> {
	pub fn new(inner: Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + 'a>) -> Self {
		Self {
			inner,
		}
	}
}

impl Iterator for StateIterator<'_> {
	type Item = Result<(EncodedKey, EncodedOperatorRow)>;

	fn next(&mut self) -> Option<Self::Item> {
		match self.inner.next()? {
			Ok(multi) => {
				let pair = if let Some(state_key) = OperatorStateKey::decode(&multi.key) {
					(EncodedKey::new(state_key.suffix), multi.bytes)
				} else {
					(multi.key, multi.bytes)
				};
				Some(EncodedOperatorRow::try_from(pair.1)
					.map_err(ValueError::from)
					.map(|row| (pair.0, row)))
			}
			Err(e) => Some(Err(e)),
		}
	}
}

/// Like [`StateIterator`] but also yields the per-key `CommitVersion`. Used by TTL eviction, which
/// is version-anchored: an entry is expired once its version is at or below the epoch cutoff.
pub struct StateIteratorVersioned<'a> {
	inner: Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + 'a>,
}

impl<'a> StateIteratorVersioned<'a> {
	pub fn new(inner: Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + 'a>) -> Self {
		Self {
			inner,
		}
	}
}

impl Iterator for StateIteratorVersioned<'_> {
	type Item = Result<(EncodedKey, CommitVersion, EncodedOperatorRow)>;

	fn next(&mut self) -> Option<Self::Item> {
		match self.inner.next()? {
			Ok(multi) => {
				let version = multi.version;
				let key = if let Some(state_key) = OperatorStateKey::decode(&multi.key) {
					EncodedKey::new(state_key.suffix)
				} else {
					multi.key
				};
				Some(EncodedOperatorRow::try_from(multi.bytes)
					.map_err(ValueError::from)
					.map(|row| (key, version, row)))
			}
			Err(e) => Some(Err(e)),
		}
	}
}
