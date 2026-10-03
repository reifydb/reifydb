// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;

#[operator_state]
#[derive(Debug, Clone, PartialEq)]
pub struct PublishState<O> {
	pub last_publish: Option<u64>,
	pub dirty: bool,
	pub row: Option<O>,
}

impl<O: HeapSize> HeapSize for PublishState<O> {
	fn heap_size(&self) -> usize {
		self.last_publish.heap_size() + self.dirty.heap_size() + self.row.heap_size()
	}
}

impl<O> Default for PublishState<O> {
	fn default() -> Self {
		Self {
			last_publish: None,
			dirty: false,
			row: None,
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::{
		key::encoded::EncodedKey,
		row::operator::state::{OperatorState, decode},
	};
	use reifydb_core::{
		key::operator::state::{GroupId, IntoGroupStateKey, KeyspaceId, OperatorStateKey, group_data_of_inner},
		metrics::heap::HeapSize,
	};
	use reifydb_macro::operator_state;
	use reifydb_value::{util::hash::Hash128, value::datetime::DateTime};

	use super::PublishState;
	use crate::window::engine::PublishKey;

	#[operator_state]
	#[derive(Debug, Clone, PartialEq, HeapSize)]
	struct Published {
		group: String,
		count: u64,
		mean: f64,
		start: DateTime,
		best: Option<i64>,
	}

	#[test]
	fn publish_state_round_trips_every_field() {
		// A field lost in storage would publish a stale pre row or re-publish a window that was never dirty.
		let state = PublishState {
			last_publish: Some(1_700_000_000_000),
			dirty: true,
			row: Some(Published {
				group: "BTC".into(),
				count: 7,
				mean: 1.5,
				start: DateTime::from_epoch_millis(1_700_000_000_123).unwrap(),
				best: None,
			}),
		};
		let bytes = state.encode_state().unwrap();
		assert_eq!(decode::<PublishState<Published>>(&bytes).unwrap(), state);
		let empty = PublishState::<Published>::default();
		assert_eq!(decode::<PublishState<Published>>(&empty.encode_state().unwrap()).unwrap(), empty);
	}

	#[test]
	fn a_publish_key_is_data_of_its_window_group() {
		// A publish row outside its window group's data would survive the reap and leak one row per window.
		let group = GroupId::hashed(Hash128(42));
		let key = (&PublishKey::new(group, EncodedKey::new(vec![1, 2, 3]))).into_group_state_key();
		let (_, keyspace, _) = OperatorStateKey::decode_inner(key.as_bytes()).unwrap();
		assert_eq!(keyspace, KeyspaceId::GUEST_WINDOW_PUBLISH);
		assert_eq!(group_data_of_inner(key.as_bytes()), Some(group));
	}
}
