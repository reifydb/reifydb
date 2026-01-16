// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{encoded::EncodedValues, key::EncodedKey},
	interface::store::MultiVersionBatch,
};

pub mod keyed;
pub mod raw;
pub mod row;
pub mod single;
#[cfg(test)]
pub mod test_utils;
pub mod utils;
pub mod window;

use reifydb_core::key::{EncodableKey, flow_node_state::FlowNodeStateKey};

// All types are accessed directly from their submodules:
// - crate::operator::stateful::keyed::KeyedStateful
// - crate::operator::stateful::raw::RawStatefulOperator
// - crate::operator::stateful::row::RowNumberProvider
// - crate::operator::stateful::single::SingleStateful
// - crate::operator::stateful::utils::*
// - crate::operator::stateful::window::WindowStateful

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

	/// Create a new StateIterator from pre-decoded items
	pub fn from_items(items: Vec<(EncodedKey, EncodedValues)>) -> Self {
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
