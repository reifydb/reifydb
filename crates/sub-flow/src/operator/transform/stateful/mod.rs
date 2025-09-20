// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	EncodedKey,
	interface::{
		BoxedVersionedIter,
		key::{EncodableKey, FlowNodeStateKey},
	},
	row::EncodedRow,
};

mod keyed;
mod raw;
mod row_number;
mod single;
mod utils;
#[cfg(test)]
mod utils_test;
mod window;

pub use keyed::KeyedStateful;
pub use raw::RawStatefulOperator;
pub use row_number::RowNumberProvider;
pub use single::SingleStateful;
pub use window::WindowStateful;

// Iterator wrapper for state entries
pub struct StateIterator<'a> {
	pub(crate) inner: BoxedVersionedIter<'a>,
}

impl<'a> Iterator for StateIterator<'a> {
	type Item = (EncodedKey, EncodedRow);

	fn next(&mut self) -> Option<Self::Item> {
		self.inner.next().map(|versioned| {
			if let Some(state_key) = FlowNodeStateKey::decode(&versioned.key) {
				(EncodedKey::new(state_key.key), versioned.row)
			} else {
				(versioned.key, versioned.row)
			}
		})
	}
}
