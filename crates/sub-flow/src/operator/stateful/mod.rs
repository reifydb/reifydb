// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{EncodedKey, value::encoded::EncodedValues};
use reifydb_store_transaction::MultiVersionBatch;

mod keyed;
mod raw;
mod row;
mod single;
#[cfg(test)]
pub mod test_utils;
mod utils;
mod window;

pub use keyed::KeyedStateful;
pub use raw::RawStatefulOperator;
use reifydb_core::key::{EncodableKey, FlowNodeStateKey};
pub use row::RowNumberProvider;
pub use single::SingleStateful;
pub use utils::*;
pub use window::WindowStateful;

/// Iterator wrapper for state entries
///
/// Wraps a MultiVersionBatch and provides an iterator over decoded state keys.
/// The batch is eagerly decoded during construction for efficiency.
pub struct StateIterator {
	items: Vec<(EncodedKey, EncodedValues)>,
	position: usize,
}

impl StateIterator {
	/// Create a new StateIterator from a MultiVersionBatch
	pub fn new(batch: MultiVersionBatch) -> Self {
		let items = batch
			.items
			.into_iter()
			.map(|multi| {
				if let Some(state_key) = FlowNodeStateKey::decode(&multi.key) {
					(EncodedKey::new(state_key.key), multi.values)
				} else {
					(multi.key, multi.values)
				}
			})
			.collect();

		Self {
			items,
			position: 0,
		}
	}
}

impl Iterator for StateIterator {
	type Item = (EncodedKey, EncodedValues);

	fn next(&mut self) -> Option<Self::Item> {
		if self.position < self.items.len() {
			let item = self.items[self.position].clone();
			self.position += 1;
			Some(item)
		} else {
			None
		}
	}
}
