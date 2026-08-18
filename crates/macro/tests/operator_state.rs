// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_codec::row::pod::state::OperatorState;
use reifydb_macro::operator_state;

#[operator_state]
#[derive(Debug, PartialEq)]
struct FlatState {
	count: u64,
	sum: f64,
}

#[operator_state]
#[derive(Debug, PartialEq)]
struct MapState {
	counts: BTreeMap<u64, u64>,
	names: Vec<String>,
}

#[operator_state]
#[derive(Debug, PartialEq)]
struct GenericState<V: Clone> {
	slots: Vec<V>,
}

#[test]
fn test_flat_state_round_trips_through_trait() {
	// The generated impl must survive encode then decode exactly, or every flush loses state.
	let state = FlatState {
		count: 9,
		sum: 2.5,
	};
	let bytes = state.encode_state().unwrap();
	let restored = FlatState::decode_state(&bytes).unwrap();
	assert_eq!(restored, state);
}

#[test]
fn test_map_state_round_trips_every_entry() {
	// A BTreeMap encodes as an ordered map, so a broken key encoding loses entries silently.
	let mut counts = BTreeMap::new();
	counts.insert(2u64, 20u64);
	counts.insert(3u64, 30u64);
	let state = MapState {
		counts,
		names: vec!["w".to_string()],
	};

	let bytes = state.encode_state().unwrap();
	let restored = MapState::decode_state(&bytes).unwrap();

	assert_eq!(restored, state);
	assert_eq!(restored.counts.len(), 2);
	assert_eq!(restored.counts.get(&3), Some(&30));
	assert_eq!(restored.counts.get(&9), None);
	assert_eq!(restored.names[0].as_str(), "w");
}

#[test]
fn test_generic_state_round_trips() {
	// A bound the macro fails to propagate must surface here as a compile error, never at runtime.
	let state = GenericState {
		slots: vec![1u32, 2, 3],
	};
	let bytes = state.encode_state().unwrap();
	let restored = GenericState::<u32>::decode_state(&bytes).unwrap();
	assert_eq!(restored, state);
}

#[test]
fn test_decode_rejects_a_corrupted_body() {
	// This is the disk-corruption trust boundary; a mangled body must error, never decode.
	let state = FlatState {
		count: 1,
		sum: 0.5,
	};
	let mut bytes = state.encode_state().unwrap();
	bytes.body_mut().fill(0xFF);

	assert!(FlatState::decode_state(&bytes).is_err());
}
