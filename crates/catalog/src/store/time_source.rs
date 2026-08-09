// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::{bytes::RowBuilder, shape::RowShape};
use reifydb_core::common::{TimeDomain, TimeSource};

pub(crate) fn write_time_source(
	shape: &RowShape,
	row: &mut impl RowBuilder,
	domain_index: usize,
	ts_index: usize,
	time: &TimeSource,
) {
	shape.set::<u8>(row, domain_index, time.domain().to_u8());
	match time.ts() {
		Some(ts) => shape.set_utf8(row, ts_index, ts),
		None => shape.set_none(row, ts_index),
	}
}

pub(crate) fn read_time_source(shape: &RowShape, row: &[u8], domain_index: usize, ts_index: usize) -> TimeSource {
	match TimeDomain::from_u8(shape.try_get::<u8>(row, domain_index).unwrap_or(0)) {
		TimeDomain::None => TimeSource::None,
		TimeDomain::Processing => TimeSource::Processing,
		TimeDomain::Event => match shape.try_get_utf8(row, ts_index) {
			Some(ts) if !ts.is_empty() => TimeSource::Event {
				ts: ts.to_string(),
			},
			_ => TimeSource::None,
		},
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::row::shape::{RowFamily, RowShapeField};
	use reifydb_value::value::value_type::ValueType;

	use super::*;

	const DOMAIN: usize = 1;
	const TS: usize = 2;

	fn shape() -> RowShape {
		RowShape::new(
			RowFamily::Catalog,
			vec![
				RowShapeField::unconstrained("name", ValueType::Utf8),
				RowShapeField::unconstrained("time_domain", ValueType::Uint1),
				RowShapeField::unconstrained("ts", ValueType::Utf8),
			],
		)
	}

	fn round_trip(time: &TimeSource) -> TimeSource {
		let shape = shape();
		let mut row = shape.allocate_catalog();
		write_time_source(&shape, &mut row, DOMAIN, TS, time);
		read_time_source(&shape, &row, DOMAIN, TS)
	}

	#[test]
	fn an_event_source_round_trips_its_populator() {
		// The write boundary reads the populator to stamp #time on every row, so losing it
		// between DDL and storage silently downgrades an event-time object to processing time.
		assert_eq!(
			round_trip(&TimeSource::Event {
				ts: "block_time".to_string(),
			}),
			TimeSource::Event {
				ts: "block_time".to_string()
			}
		);
	}

	#[test]
	fn none_and_processing_are_distinguishable_despite_sharing_an_absent_populator() {
		// Both variants store no ts, so the populator field alone cannot tell them apart. This is
		// the entire reason the domain byte exists: without it every explicitly declared
		// processing-time object would read back as time: none and stop stamping #time at all.
		assert_eq!(round_trip(&TimeSource::None), TimeSource::None);
		assert_eq!(round_trip(&TimeSource::Processing), TimeSource::Processing);
		assert_ne!(round_trip(&TimeSource::None), round_trip(&TimeSource::Processing));
	}

	#[test]
	fn an_unwritten_domain_reads_as_none() {
		// A catalog row predating the domain byte carries no value there, and the default for an
		// undeclared object is none. Reading it as Processing instead would resurrect the wall
		// clock stamping this redesign removes.
		let shape = shape();
		let row = shape.allocate_catalog();

		assert_eq!(read_time_source(&shape, &row, DOMAIN, TS), TimeSource::None);
	}

	#[test]
	fn an_event_domain_naming_no_column_degrades_to_none() {
		// Event time with no populator names no column to read, so stamping would fall back to
		// the arrival clock. None withholds #time instead, which is the failure this design
		// prefers: a missing stamp is visible, a wall-clock stamp is not.
		let shape = shape();
		let mut row = shape.allocate_catalog();
		shape.set::<u8>(&mut row, DOMAIN, TimeDomain::Event.to_u8());
		shape.set_none(&mut row, TS);

		assert_eq!(read_time_source(&shape, &row, DOMAIN, TS), TimeSource::None);
	}

	#[test]
	fn the_domain_always_agrees_with_the_populator() {
		// The populator is only meaningful under Event, so no byte pattern may produce an
		// event-time object that names no column, nor a non-event object that names one.
		for time in [
			TimeSource::None,
			TimeSource::Processing,
			TimeSource::Event {
				ts: "at".to_string(),
			},
		] {
			let read = round_trip(&time);

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
		// The domain and ts fields are appended after every existing field of each object's shape,
		// so an encoding that overran would corrupt the column immediately before them.
		let shape = shape();
		let mut row = shape.allocate_catalog();
		shape.set_utf8(&mut row, 0, "trades");

		write_time_source(
			&shape,
			&mut row,
			DOMAIN,
			TS,
			&TimeSource::Event {
				ts: "block_time".to_string(),
			},
		);

		assert_eq!(shape.get_utf8(&row, 0), "trades");
	}
}
