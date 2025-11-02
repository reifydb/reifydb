// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{EncodedKey, interface::BoxedMultiVersionIter, value::encoded::EncodedValues};

mod keyed;
mod raw;
mod row_number;
mod single;
#[cfg(test)]
pub mod test_utils;
mod utils;
mod window;

pub use keyed::KeyedStateful;
pub use raw::RawStatefulOperator;
use reifydb_core::key::{EncodableKey, FlowNodeStateKey};
pub use row_number::RowNumberProvider;
pub use single::SingleStateful;
pub use utils::*;
pub use window::WindowStateful;

// Iterator wrapper for state entries
pub struct StateIterator<'a> {
	pub(crate) inner: BoxedMultiVersionIter<'a>,
}

impl<'a> Iterator for StateIterator<'a> {
	type Item = (EncodedKey, EncodedValues);

	fn next(&mut self) -> Option<Self::Item> {
		self.inner.next().map(|multi| {
			if let Some(state_key) = FlowNodeStateKey::decode(&multi.key) {
				(EncodedKey::new(state_key.key), multi.values)
			} else {
				(multi.key, multi.values)
			}
		})
	}
}
