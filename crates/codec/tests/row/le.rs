// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::{
	bytes::{RowBuilder, SHAPE_HEADER_SIZE},
	shape::{RowFamily, RowShape},
};
use reifydb_value::{
	encoding::LeBytes,
	value::{date::Date, datetime::DateTime, duration::Duration, value_type::ValueType},
};

#[test]
fn one_generic_path_writes_every_width_into_its_own_slot() {
	// Every fixed-width accessor funnels through set_le, so one wrong bound corrupts all of
	// them at once. The four widths sit at non-zero field indices because a slot written at
	// the data section start still round-trips as field 0, hiding a dropped offset.
	let shape = RowShape::testing(
		RowFamily::Table,
		&[
			ValueType::Uint8,
			ValueType::Boolean,
			ValueType::Date,
			ValueType::DateTime,
			ValueType::Duration,
			ValueType::Uint8,
		],
	);
	let mut row = shape.allocate_table();

	let date = Date::from_days_since_epoch(19_000).unwrap();
	let datetime = DateTime::from_nanos(0x0102_0304_0506_0708);
	let duration = Duration::new(13, 7, 1_234_567_890).unwrap();

	shape.set::<u64>(&mut row, 0, 0xAAAA_AAAA_AAAA_AAAAu64);
	shape.set::<u64>(&mut row, 5, 0xBBBB_BBBB_BBBB_BBBBu64);
	let before = row.to_vec();

	shape.set::<bool>(&mut row, 1, true);
	shape.set::<Date>(&mut row, 2, date);
	shape.set::<DateTime>(&mut row, 3, datetime);
	shape.set::<Duration>(&mut row, 4, duration);

	let written = [
		(1usize, LeBytes::to_le_bytes(&true).as_ref().to_vec()),
		(2, LeBytes::to_le_bytes(&date).as_ref().to_vec()),
		(3, LeBytes::to_le_bytes(&datetime).as_ref().to_vec()),
		(4, LeBytes::to_le_bytes(&duration).as_ref().to_vec()),
	];

	for (index, bytes) in &written {
		let offset = shape.fields()[*index].offset as usize;
		assert_eq!(bytes.len(), shape.fields()[*index].size as usize, "field {index} slot width");
		assert_eq!(
			&row[offset..offset + bytes.len()],
			bytes.as_slice(),
			"field {index} does not hold its own little-endian form"
		);
	}

	for (position, (old, new)) in before.iter().zip(row.as_slice()).enumerate() {
		let in_slot = written.iter().any(|(index, bytes)| {
			let offset = shape.fields()[*index].offset as usize;
			position >= offset && position < offset + bytes.len()
		});
		let in_bitvec = position >= SHAPE_HEADER_SIZE && position < shape.data_offset();
		if !in_slot && !in_bitvec {
			assert_eq!(old, new, "byte {position} lies outside every written slot but changed");
		}
	}

	assert_eq!(shape.get::<u64>(&row, 0), 0xAAAA_AAAA_AAAA_AAAAu64);
	assert_eq!(shape.get::<u64>(&row, 5), 0xBBBB_BBBB_BBBB_BBBBu64);
	assert_eq!(shape.get::<bool>(&row, 1), true);
	assert_eq!(shape.get::<Date>(&row, 2), date);
	assert_eq!(shape.get::<DateTime>(&row, 3), datetime);
	assert_eq!(shape.get::<Duration>(&row, 4), duration);
}
