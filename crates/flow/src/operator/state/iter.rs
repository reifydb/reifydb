// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	interface::store::MultiVersionRow,
	key::{EncodableKey, operator_state::OperatorStateKey},
};
use reifydb_value::Result;

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
					(state_key.inner(), multi.bytes)
				} else {
					(multi.key, multi.bytes)
				};
				Some(Ok(pair))
			}
			Err(e) => Some(Err(e)),
		}
	}
}
