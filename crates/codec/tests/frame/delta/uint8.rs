// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::frame::format::Encoding;
use reifydb_value::value::{container::number::NumberContainer, frame::data::FrameColumnData};

fn make(v: Vec<u64>) -> FrameColumnData {
	FrameColumnData::Uint8(NumberContainer::new(v))
}

crate::delta_tests! {
	ascending: (0..200u64).map(|i| i * 1000).collect::<Vec<_>>(),
	descending: (0..200u64).rev().map(|i| i * 1000).collect::<Vec<_>>(),
	unsorted: (0..200).map(|i| ((i * 7 + 13) % 97) as u64).collect::<Vec<_>>(),
}

#[test]
fn ascending_across_the_i64_sign_boundary_wraps() {
	// a u64 step up across 2^63 must wrap to a one byte delta, never overflow or fall back to plain
	crate::common::assert_forced_round_trip_beats_plain(
		"test",
		make(vec![i64::MAX as u64, i64::MIN as u64]),
		Encoding::Delta,
	);
}

#[test]
fn descending_across_the_i64_sign_boundary_wraps() {
	// a u64 step down across 2^63 must wrap to a one byte delta, never overflow or fall back to plain
	crate::common::assert_forced_round_trip_beats_plain(
		"test",
		make(vec![i64::MIN as u64, i64::MAX as u64]),
		Encoding::Delta,
	);
}
