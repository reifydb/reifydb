// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::encoded::{row::EncodedRowBuilder, shape::RowShape};
use reifydb_core::common::TimeSource;

pub(crate) fn write_time_source(shape: &RowShape, row: &mut EncodedRowBuilder, index: usize, time: &TimeSource) {
	match time.ts() {
		Some(ts) => shape.set_utf8(row, index, ts),
		None => shape.set_none(row, index),
	}
}

pub(crate) fn read_time_source(shape: &RowShape, row: &[u8], index: usize) -> TimeSource {
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
	use reifydb_core::common::TimeDomain;
	use reifydb_value::value::value_type::ValueType;

	use super::*;

	fn shape() -> RowShape {
		RowShape::new(vec![
			RowShapeField::unconstrained("name", ValueType::Utf8),
			RowShapeField::unconstrained("ts", ValueType::Utf8),
		])
	}

	#[test]
	fn an_event_source_round_trips_its_populator() {
		// The write boundary reads the populator to stamp #time on every row, so losing it
		// between DDL and storage silently downgrades an event-time object to processing time.
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
	fn a_processing_source_round_trips_with_no_populator() {
		// Writing an empty string instead of none reads back as Event{ts: ""} - a populator
		// naming no column, the state this encoding exists to make unrepresentable.
		let shape = shape();
		let mut row = shape.allocate();

		write_time_source(&shape, &mut row, 1, &TimeSource::Processing);

		let read = read_time_source(&shape, &row, 1);
		assert_eq!(read, TimeSource::Processing);
		assert_eq!(read.ts(), None);
		assert_eq!(read.domain(), TimeDomain::Processing);
	}

	#[test]
	fn the_domain_always_agrees_with_the_populator() {
		// The domain is derived on read, never stored, so no byte pattern can produce an
		// event-time object that names no column.
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

			assert_eq!(
				read.domain() == TimeDomain::Event,
				read.ts().is_some(),
				"domain and populator disagree"
			);
			assert_eq!(read, time);
		}
	}

	#[test]
	fn writing_the_populator_leaves_other_fields_intact() {
		// The ts field is appended after every existing field of each object's shape, so an
		// encoding that overran would corrupt the column immediately before it.
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
