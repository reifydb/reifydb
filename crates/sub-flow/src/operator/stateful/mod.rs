// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::interface::store::MultiVersionRow;
use reifydb_value::Result;

pub mod raw;
#[cfg(test)]
pub mod test_utils;
pub mod utils;

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
	type Item = Result<(EncodedKey, EncodedBytes)>;

	fn next(&mut self) -> Option<Self::Item> {
		match self.inner.next()? {
			Ok(multi) => {
				let pair = if let Some(state_key) = OperatorStateKey::decode(&multi.key) {
					(EncodedKey::new(state_key.key), multi.bytes)
				} else if let Some(internal_key) = OperatorStateKey::decode(&multi.key) {
					(EncodedKey::new(internal_key.key), multi.bytes)
				} else {
					(multi.key, multi.bytes)
				};
				Some(Ok(pair))
			}
			Err(e) => Some(Err(e)),
		}
	}
}
