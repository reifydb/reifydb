// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::frame::format::Encoding;
use reifydb_value::value::{container::temporal::TemporalContainer, datetime::DateTime, frame::data::FrameColumnData};

fn make(v: Vec<DateTime>) -> FrameColumnData {
	FrameColumnData::DateTime(TemporalContainer::new(v))
}

crate::delta_rle_tests! {
	constant_stride: {
		let base = 1_700_000_000_000_000_000u64;
		(0..500).map(|i| DateTime::from_nanos(base + i * 1_000_000_000)).collect::<Vec<_>>()
	},
	descending_stride: {
		let base = 1_700_000_000_000_000_000u64;
		(0..500).rev().map(|i| DateTime::from_nanos(base + i * 1_000_000_000)).collect::<Vec<_>>()
	},
}

#[test]
fn ascending_across_the_i64_sign_boundary_wraps() {
	// nanos stepping up across 2^63 must wrap to a one byte delta run, never overflow or fall back to plain
	crate::common::assert_forced_round_trip_beats_plain(
		"test",
		make(vec![DateTime::from_nanos(i64::MAX as u64), DateTime::from_nanos(i64::MIN as u64)]),
		Encoding::DeltaRle,
	);
}

#[test]
fn descending_across_the_i64_sign_boundary_wraps() {
	// nanos stepping down across 2^63 must wrap to a one byte delta run, never overflow or fall back to plain
	crate::common::assert_forced_round_trip_beats_plain(
		"test",
		make(vec![DateTime::from_nanos(i64::MIN as u64), DateTime::from_nanos(i64::MAX as u64)]),
		Encoding::DeltaRle,
	);
}
