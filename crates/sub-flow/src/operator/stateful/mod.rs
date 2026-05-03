// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::store::MultiVersionBatch,
};

pub mod counter;
pub mod keyed;
pub mod raw;
pub mod row;
pub mod single;
pub mod test_utils;
pub mod utils;
pub mod window;

use reifydb_core::key::{EncodableKey, flow_node_state::FlowNodeStateKey};

pub struct StateIterator {
	items: Vec<(EncodedKey, EncodedRow)>,
	position: usize,
}

impl StateIterator {
	pub fn new(batch: MultiVersionBatch) -> Self {
		let items = batch
			.items
			.into_iter()
			.map(|multi| {
				if let Some(state_key) = FlowNodeStateKey::decode(&multi.key) {
					(EncodedKey::new(state_key.key), multi.row)
				} else {
					(multi.key, multi.row)
				}
			})
			.collect();

		Self {
			items,
			position: 0,
		}
	}

	pub fn from_items(items: Vec<(EncodedKey, EncodedRow)>) -> Self {
		Self {
			items,
			position: 0,
		}
	}
}

impl Iterator for StateIterator {
	type Item = (EncodedKey, EncodedRow);

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
