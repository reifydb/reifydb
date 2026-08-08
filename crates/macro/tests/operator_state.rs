// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_codec::operator::{OperatorState, SealMutableState};
use reifydb_macro::operator_state;
use reifydb_value::value::datetime::DateTime;
use rkyv::{munge::munge, primitive::ArchivedU64};

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
	// The macro must generate a working OperatorState impl: encode at
	// flush, validate at the trust boundary, materialize on promotion.
	let state = FlatState {
		count: 9,
		sum: 2.5,
	};
	let bytes = state.encode_state(DateTime::from_nanos(11)).unwrap();
	let archived = FlatState::archived(&bytes).unwrap();
	assert_eq!(archived.count, 9);
	let restored = FlatState::materialize(archived).unwrap();
	assert_eq!(restored, state);
}

#[test]
fn test_map_state_archived_lookup_without_decode() {
	// The zero-copy read path: a BTreeMap lookup on the archived form
	// must work straight from the stored bytes, with no materialize.
	let mut counts = BTreeMap::new();
	counts.insert(2u64, 20u64);
	counts.insert(3u64, 30u64);
	let state = MapState {
		counts,
		names: vec!["w".to_string()],
	};

	let bytes = state.encode_state(DateTime::EPOCH).unwrap();
	let archived = MapState::archived(&bytes).unwrap();

	assert_eq!(archived.counts.len(), 2);
	assert_eq!(archived.counts.get(&ArchivedU64::from_native(3)).map(|v| v.to_native()), Some(30));
	assert_eq!(archived.counts.get(&ArchivedU64::from_native(9)), None);
	assert_eq!(archived.names[0].as_str(), "w");
}

#[test]
fn test_generic_state_round_trips() {
	// Generic state types (Multiset, GroupMeta and friends) rely on the
	// macro's propagated bounds; a bound bug shows up here as a compile
	// failure rather than a runtime one.
	let state = GenericState {
		slots: vec![1u32, 2, 3],
	};
	let bytes = state.encode_state(DateTime::EPOCH).unwrap();
	let archived = GenericState::<u32>::archived(&bytes).unwrap();
	assert_eq!(archived.slots.len(), 3);
	let restored = GenericState::<u32>::materialize(archived).unwrap();
	assert_eq!(restored, state);
}

#[test]
fn test_trusted_access_after_validation() {
	let state = FlatState {
		count: 1,
		sum: 0.5,
	};
	let bytes = state.encode_state(DateTime::EPOCH).unwrap();
	FlatState::archived(&bytes).unwrap();
	// SAFETY: bytes passed FlatState::archived validation on the line
	// above and is an archive of exactly FlatState.
	let trusted = unsafe { FlatState::archived_trusted(&bytes) };
	assert_eq!(trusted.count, 1);
}

#[operator_state(seal)]
#[derive(Debug, PartialEq)]
struct SealedState {
	count: u64,
}

#[test]
fn test_seal_marked_state_writes_archived_bytes_in_place() {
	// #[operator_state(seal)] must emit the SealMutableState marker, and the
	// sealed accessor must write a fixed-size field directly into the stored
	// bytes: no re-encode, and a subsequent validated read sees the new value.
	fn assert_seal_mutable<T: SealMutableState>() {}
	assert_seal_mutable::<SealedState>();

	let state = SealedState {
		count: 1,
	};
	let mut bytes = state.encode_state(DateTime::from_nanos(5)).unwrap();
	// SAFETY: bytes were produced by encode_state for exactly SealedState.
	let seal = unsafe { SealedState::archived_seal_trusted(&mut bytes) };
	munge!(let ArchivedSealedState { mut count } = seal);
	*count = ArchivedU64::from_native(7);

	let archived = SealedState::archived(&bytes).unwrap();
	assert_eq!(archived.count, 7);
	assert_eq!(
		SealedState::materialize(archived).unwrap(),
		SealedState {
			count: 7
		}
	);
}
