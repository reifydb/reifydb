// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{common::CommitVersion, lifecycle::watermark::CheckpointFloor};
use reifydb_runtime::sync::mutex::Mutex;

use super::{FLOW_A, FLOW_B, flush, store_fixture};
use crate::store::{OperatorStore, StandardOperatorStore};

fn floor(store: &OperatorStore) -> Option<CommitVersion> {
	store.floor().unwrap()
}

fn durable_and_buffered(store: &StandardOperatorStore, durable: u64, buffered: u64) {
	store.checkpoint_set(FLOW_A, CommitVersion(durable)).unwrap();
	flush(store);
	store.checkpoint_set(FLOW_B, CommitVersion(buffered)).unwrap();
	assert_eq!(
		store.resident.checkpoint_floor(),
		Some(CommitVersion(buffered)),
		"precondition: only flow B may still sit in the buffer, flow A must have been flushed"
	);
}

#[test]
fn the_floor_is_the_lowest_of_the_pins_the_buffered_and_the_durable_checkpoints() {
	// A floor that skips any one of the three lets retention reap versions that one still needs.
	for (durable, buffered, pinned, want) in [(100, 80, 50, 50), (100, 50, 80, 50), (50, 100, 80, 50)] {
		let (standard, _guard) = store_fixture();
		durable_and_buffered(&standard, durable, buffered);
		let store = OperatorStore::Standard(standard);
		let pin = store.checkpoint_pin(CommitVersion(pinned));
		assert_eq!(
			floor(&store),
			Some(CommitVersion(want)),
			"durable {durable}, buffered {buffered}, pinned {pinned}: the floor must be the lowest of the three"
		);
		drop(pin);
		assert_eq!(
			floor(&store),
			Some(CommitVersion(durable.min(buffered))),
			"durable {durable}, buffered {buffered}: dropping the pin must restore the checkpoint floor"
		);
	}
}

#[test]
fn a_pin_above_every_checkpoint_never_raises_the_floor() {
	// A pin that replaced the checkpoint floor instead of joining it would let retention pass a lagging flow.
	let (standard, _guard) = store_fixture();
	durable_and_buffered(&standard, 100, 80);
	let store = OperatorStore::Standard(standard);
	let pin = store.checkpoint_pin(CommitVersion(200));
	assert_eq!(floor(&store), Some(CommitVersion(80)), "a pin above the checkpoints must leave the floor at 80");
	drop(pin);
	assert_eq!(floor(&store), Some(CommitVersion(80)), "dropping a pin above the floor must not move it");
}

#[test]
fn a_pin_sets_the_floor_when_no_flow_has_a_checkpoint_yet() {
	// A new flow's first backfill has no checkpoint anywhere, so without its pin nothing holds the cdc at V.
	let (standard, _guard) = store_fixture();
	let store = OperatorStore::Standard(standard);
	assert_eq!(floor(&store), None, "precondition: a store with no checkpoint has no floor");
	let pin = store.checkpoint_pin(CommitVersion(7));
	assert_eq!(floor(&store), Some(CommitVersion(7)), "the pin alone must set the floor");
	drop(pin);
	assert_eq!(floor(&store), None, "a dropped pin must not leave a floor behind");
}

#[test]
fn dropping_the_lower_of_two_pins_hands_the_floor_to_the_higher_one() {
	// A drop that cleared every pin, or kept the dropped one, frees or holds versions another backfill needs.
	let (standard, _guard) = store_fixture();
	durable_and_buffered(&standard, 100, 90);
	let store = OperatorStore::Standard(standard);
	let low = store.checkpoint_pin(CommitVersion(30));
	let high = store.checkpoint_pin(CommitVersion(40));
	assert_eq!(floor(&store), Some(CommitVersion(30)), "the lower pin must set the floor");
	drop(low);
	assert_eq!(floor(&store), Some(CommitVersion(40)), "the higher pin must hold the floor once the lower is gone");
	drop(high);
	assert_eq!(floor(&store), Some(CommitVersion(90)), "with both pins gone the checkpoints set the floor again");
}

#[test]
fn dropping_the_higher_of_two_pins_keeps_the_lower_one() {
	// A drop that removed the lowest pin instead of its own would release a backfill still in flight.
	let (standard, _guard) = store_fixture();
	let store = OperatorStore::Standard(standard);
	let low = store.checkpoint_pin(CommitVersion(30));
	let high = store.checkpoint_pin(CommitVersion(40));
	drop(high);
	assert_eq!(floor(&store), Some(CommitVersion(30)), "the lower pin must still hold the floor");
	drop(low);
	assert_eq!(floor(&store), None, "with both pins gone there is no floor");
}

#[test]
fn two_pins_at_one_version_hold_the_floor_until_both_are_dropped() {
	// Pins keyed by version would let the first drop release a second backfill running at the same V.
	let (standard, _guard) = store_fixture();
	let store = OperatorStore::Standard(standard);
	let first = store.checkpoint_pin(CommitVersion(30));
	let second = store.checkpoint_pin(CommitVersion(30));
	drop(first);
	assert_eq!(floor(&store), Some(CommitVersion(30)), "the second pin at 30 must still hold the floor");
	drop(second);
	assert_eq!(floor(&store), None, "with both pins gone there is no floor");
}

#[test]
fn a_pin_taken_through_one_handle_holds_the_floor_read_through_another() {
	// The flow actor pins on its own clone of the store; a pin kept per clone never reaches the cdc floor reader.
	let (standard, _guard) = store_fixture();
	let reader = OperatorStore::Standard(standard.clone());
	let actor = OperatorStore::Standard(standard.clone());
	let through_enum = actor.checkpoint_pin(CommitVersion(20));
	assert_eq!(floor(&reader), Some(CommitVersion(20)), "a pin on another enum handle must set the floor");
	let through_standard = standard.clone().checkpoint_pin(CommitVersion(10));
	assert_eq!(floor(&reader), Some(CommitVersion(10)), "a pin on another standard handle must set the floor");
	drop(through_standard);
	drop(through_enum);
	assert_eq!(floor(&reader), None, "pins dropped on other handles must leave this one's floor too");
}

#[test]
fn a_pin_released_mid_merge_right_after_its_checkpoint_is_buffered_still_holds_that_merge() {
	// Pins read after the tiers miss both the released pin and a checkpoint buffered after the buffer read.
	let (standard, _guard) = store_fixture();
	standard.checkpoint_set(FLOW_B, CommitVersion(100)).unwrap();
	flush(&standard);
	let store = OperatorStore::Standard(standard.clone());
	let pin = Mutex::new(Some(store.checkpoint_pin(CommitVersion(50))));
	standard.attach_checkpoint_interlock(Box::new(move |store| {
		if let Some(pin) = pin.lock().take() {
			store.checkpoint_set(FLOW_A, CommitVersion(50)).unwrap();
			drop(pin);
		}
	}));
	assert_eq!(
		floor(&store),
		Some(CommitVersion(50)),
		"the checkpoint at 50 lands in the buffer and the pin goes between the two tier reads; the merge must \
		 still see one of them, or retention passes 50 and the flow loses its cdc from 51"
	);
	assert_eq!(
		floor(&store),
		Some(CommitVersion(50)),
		"once the pin is gone the buffered checkpoint must hold the floor at 50"
	);
}
