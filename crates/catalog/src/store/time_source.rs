// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::encoded::{row::EncodedRow, shape::RowShape};
use reifydb_core::common::TimeSource;

pub(crate) fn write_time_source(shape: &RowShape, row: &mut EncodedRow, index: usize, time: &TimeSource) {
	match time.ts() {
		Some(ts) => shape.set_utf8(row, index, ts),
		None => shape.set_none(row, index),
	}
}

pub(crate) fn read_time_source(shape: &RowShape, row: &EncodedRow, index: usize) -> TimeSource {
	match shape.try_get_utf8(row, index) {
		Some(ts) if !ts.is_empty() => TimeSource::Event {
			ts: ts.to_string(),
		},
		_ => TimeSource::Processing,
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::encoded::shape::RowShapeField;
	use reifydb_value::value::value_type::ValueType;

	use super::*;

	fn shape() -> RowShape {
		RowShape::new(vec![
			RowShapeField::unconstrained("name", ValueType::Utf8),
			RowShapeField::unconstrained("ts", ValueType::Utf8),
		])
	}

	#[test]
	// Intent: the populator survives the byte round trip, because it is what the write boundary reads to stamp
	// #time on every row. A declaration accepted at DDL and then lost here would silently downgrade an event-time
	// object to processing time.
	fn an_event_source_round_trips_its_populator() {
		let shape = shape();
		let mut row = shape.allocate();

		write_time_source(
			&shape,
			&mut row,
			1,
			&TimeSource::Event {
				ts: "block_time".to_string(),
			},
		);

		assert_eq!(
			read_time_source(&shape, &row, 1),
			TimeSource::Event {
				ts: "block_time".to_string()
			}
		);
	}

	#[test]
	// Intent: processing time writes no populator and reads back as processing. Mutation: write an empty string
	// instead of none and the read comes back as Event{ts: ""} - a populator naming no column, which is exactly
	// the state this encoding exists to make unrepresentable.
	fn a_processing_source_round_trips_with_no_populator() {
		let shape = shape();
		let mut row = shape.allocate();

		write_time_source(&shape, &mut row, 1, &TimeSource::Processing);

		let read = read_time_source(&shape, &row, 1);
		assert_eq!(read, TimeSource::Processing);
		assert_eq!(read.ts(), None);
		assert!(!read.domain().is_event());
	}

	#[test]
	// Intent: the domain is DERIVED on read, never stored, so no stored byte pattern can produce an event-time
	// object that names no column. This walks the only two states the field can hold and asserts the derivation
	// agrees with the populator in both.
	fn the_domain_always_agrees_with_the_populator() {
		let shape = shape();

		for time in [
			TimeSource::Processing,
			TimeSource::Event {
				ts: "at".to_string(),
			},
		] {
			let mut row = shape.allocate();
			write_time_source(&shape, &mut row, 1, &time);
			let read = read_time_source(&shape, &row, 1);

			assert_eq!(read.domain().is_event(), read.ts().is_some(), "domain and populator disagree");
			assert_eq!(read, time);
		}
	}

	#[test]
	// Intent: writing the field must not disturb its neighbours. The ts field is appended after every existing
	// field of each object's shape, so an encoding that overran would corrupt the column immediately before it.
	fn writing_the_populator_leaves_other_fields_intact() {
		let shape = shape();
		let mut row = shape.allocate();
		shape.set_utf8(&mut row, 0, "trades");

		write_time_source(
			&shape,
			&mut row,
			1,
			&TimeSource::Event {
				ts: "block_time".to_string(),
			},
		);

		assert_eq!(shape.get_utf8(&row, 0), "trades");
	}
}
