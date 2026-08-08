// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::shape::RowShape;
use reifydb_core::{common::TimeSource, interface::catalog::column::Column};
use reifydb_value::{
	Result,
	value::{Value, datetime::DateTime},
};

use crate::error::EngineError;

pub(crate) fn resolve_time(
	object: &str,
	columns: &[Column],
	time: &TimeSource,
	shape: &RowShape,
	row: &[u8],
	arrival: DateTime,
) -> Result<Option<DateTime>> {
	match time {
		TimeSource::None => Ok(None),
		TimeSource::Processing => Ok(Some(arrival)),
		TimeSource::Event {
			ts,
		} => resolve_populator(object, columns, ts, shape, row).map(Some),
	}
}

pub(crate) fn resolve_time_for_update(
	object: &str,
	columns: &[Column],
	time: &TimeSource,
	shape: &RowShape,
	row: &[u8],
	previous_time: Option<DateTime>,
) -> Result<Option<DateTime>> {
	match time {
		TimeSource::None => Ok(None),
		TimeSource::Processing => Ok(previous_time),
		TimeSource::Event {
			ts,
		} => resolve_populator(object, columns, ts, shape, row).map(Some),
	}
}

fn resolve_populator(object: &str, columns: &[Column], ts: &str, shape: &RowShape, row: &[u8]) -> Result<DateTime> {
	let index = columns.iter().position(|c| c.name == ts).ok_or_else(|| EngineError::TimePopulatorMissing {
		object: object.to_string(),
		column: ts.to_string(),
	})?;

	match shape.get_value(row, index) {
		Value::DateTime(dt) => Ok(dt),
		found => Err(EngineError::TimePopulatorNotDateTime {
			object: object.to_string(),
			column: ts.to_string(),
			found: format!("{found:?}"),
		}
		.into()),
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::row::{
		bytes::EncodedBytes,
		shape::{RowFamily, RowShapeField},
	};
	use reifydb_core::interface::catalog::{
		column::{Column, ColumnIndex},
		id::ColumnId,
	};
	use reifydb_value::{
		factory::time::at_nanos,
		value::{constraint::TypeConstraint, datetime::DateTime, value_type::ValueType},
	};

	use super::*;

	const ARRIVAL: u64 = 1_900_000_000_000_000_000;
	const BLOCK_TIME: u64 = 1_700_000_000_000_000_000;

	fn column(name: &str, ty: ValueType, index: u8) -> Column {
		Column {
			id: ColumnId(index as u64 + 1),
			name: name.to_string(),
			constraint: TypeConstraint::unconstrained(ty),
			properties: vec![],
			index: ColumnIndex(index),
			auto_increment: false,
			dictionary_id: None,
		}
	}

	fn columns() -> Vec<Column> {
		vec![column("signature", ValueType::Utf8, 0), column("block_time", ValueType::DateTime, 1)]
	}

	fn shape() -> RowShape {
		RowShape::new(
			RowFamily::Deprecated,
			vec![
				RowShapeField::unconstrained("signature", ValueType::Utf8),
				RowShapeField::unconstrained("block_time", ValueType::DateTime),
			],
		)
	}

	fn encoded_bytes(shape: &RowShape, block_time_nanos: u64) -> EncodedBytes {
		let mut row = shape.allocate();
		shape.set_value(&mut row, 0, &Value::Utf8("sig".to_string()));
		shape.set_value(&mut row, 1, &Value::DateTime(DateTime::from_nanos(block_time_nanos)));
		row.freeze()
	}

	fn event() -> TimeSource {
		TimeSource::Event {
			ts: "block_time".to_string(),
		}
	}

	fn unwrapped(
		object: &str,
		columns: &[Column],
		time: &TimeSource,
		shape: &RowShape,
		row: &[u8],
		arrival_nanos: u64,
	) -> u64 {
		resolve_time(object, columns, time, shape, row, at_nanos(arrival_nanos))
			.expect("resolution must succeed")
			.expect("a timed object must produce a #time")
			.to_nanos()
	}

	#[test]
	fn a_time_less_object_stamps_no_time_at_all() {
		// Falling back to the arrival clock stamps a reference row with the instant it was loaded, so
		// a join against an old event row drags the result forward to now and jumps the watermark by
		// the age of the corpus.
		let shape = shape();

		let resolved = resolve_time(
			"tokens",
			&columns(),
			&TimeSource::None,
			&shape,
			&encoded_bytes(&shape, BLOCK_TIME),
			at_nanos(ARRIVAL),
		)
		.expect("resolution must succeed");

		assert_eq!(resolved, None, "a time-less object must withhold #time rather than borrow the wall clock");
	}

	#[test]
	fn an_event_time_object_stamps_time_from_the_declared_populator() {
		// Stamping from the clock would make a replay of an old corpus re-date every row to now.
		let shape = shape();

		let stamped =
			unwrapped("trades", &columns(), &event(), &shape, &encoded_bytes(&shape, BLOCK_TIME), ARRIVAL);

		assert_eq!(stamped, BLOCK_TIME, "#time must come from block_time, not from the write clock");
	}

	#[test]
	fn a_processing_time_object_stamps_time_from_arrival() {
		// An object that declares processing time wants ingest time as its clock, so the arrival
		// instant is the stamp rather than a fallback for having found no populator.
		let shape = shape();

		let stamped = unwrapped(
			"audit",
			&columns(),
			&TimeSource::Processing,
			&shape,
			&encoded_bytes(&shape, BLOCK_TIME),
			ARRIVAL,
		);

		assert_eq!(stamped, ARRIVAL);
	}

	#[test]
	fn time_diverges_from_arrival_when_the_event_predates_the_write() {
		// A backfill of week-old data must land at its own event time, or every windowed
		// rollup over it buckets into today.
		let shape = shape();

		let stamped =
			unwrapped("trades", &columns(), &event(), &shape, &encoded_bytes(&shape, BLOCK_TIME), ARRIVAL);

		assert!(stamped < ARRIVAL, "a backfilled row's #time must predate its arrival");
		assert_eq!(ARRIVAL - stamped, 200_000_000_000_000_000);
	}

	#[test]
	fn the_populator_is_resolved_by_name_not_by_position() {
		// Resolving by position would pick the wrong column once another shares its type.
		let shape = RowShape::new(
			RowFamily::Deprecated,
			vec![
				RowShapeField::unconstrained("block_time", ValueType::DateTime),
				RowShapeField::unconstrained("recorded_at", ValueType::DateTime),
			],
		);
		let columns = vec![
			column("block_time", ValueType::DateTime, 0),
			column("recorded_at", ValueType::DateTime, 1),
		];

		let mut r = shape.allocate();
		shape.set_value(&mut r, 0, &Value::DateTime(DateTime::from_nanos(BLOCK_TIME)));
		shape.set_value(&mut r, 1, &Value::DateTime(DateTime::from_nanos(ARRIVAL)));

		assert_eq!(unwrapped("trades", &columns, &event(), &shape, &r, 0), BLOCK_TIME);
	}

	#[test]
	fn the_resolution_does_not_depend_on_the_object_kind() {
		// Table, series, ringbuffer and queue share this resolver so a declaration cannot be
		// honoured for one object kind and dropped for another.
		let shape = shape();
		let r = encoded_bytes(&shape, BLOCK_TIME);

		for object in ["trades", "prices", "recent", "jobs"] {
			assert_eq!(
				unwrapped(object, &columns(), &event(), &shape, &r, ARRIVAL),
				BLOCK_TIME,
				"{object} resolved differently"
			);
		}
	}

	#[test]
	fn an_absent_populator_column_fails_the_write_instead_of_falling_back() {
		// Falling back to the arrival clock keeps writing rows stamped with now, and a windowed
		// rollup over them looks plausible while being wrong.
		let shape = shape();
		let time = TimeSource::Event {
			ts: "no_such_column".to_string(),
		};

		let err = resolve_time(
			"trades",
			&columns(),
			&time,
			&shape,
			&encoded_bytes(&shape, BLOCK_TIME),
			at_nanos(ARRIVAL),
		)
		.expect_err("an absent populator must not resolve");

		assert_eq!(err.diagnostic().code, "TIME_001");
	}

	#[test]
	fn a_populator_that_is_not_a_datetime_fails_the_write() {
		// Falling back would date the row to now while the object claims to be event-time.
		let shape = shape();
		let time = TimeSource::Event {
			ts: "signature".to_string(),
		};

		let err = resolve_time(
			"trades",
			&columns(),
			&time,
			&shape,
			&encoded_bytes(&shape, BLOCK_TIME),
			at_nanos(ARRIVAL),
		)
		.expect_err("a utf8 populator must not resolve");

		assert_eq!(err.diagnostic().code, "TIME_002");
	}

	#[test]
	fn a_none_populator_fails_the_write() {
		// Pinned separately because none travels a different path through get_value than a
		// wrong-typed value does.
		let shape = shape();
		let mut r = shape.allocate();
		shape.set_value(&mut r, 0, &Value::Utf8("sig".to_string()));
		shape.set_none(&mut r, 1);

		let err = resolve_time("trades", &columns(), &event(), &shape, &r, at_nanos(ARRIVAL))
			.expect_err("a none populator must not resolve");

		assert_eq!(err.diagnostic().code, "TIME_002");
	}

	const CORRECTED_TIME: u64 = 1_650_000_000_000_000_000;

	fn unwrapped_update(
		object: &str,
		columns: &[Column],
		time: &TimeSource,
		shape: &RowShape,
		row: &[u8],
		previous_time_nanos: u64,
	) -> u64 {
		resolve_time_for_update(object, columns, time, shape, row, Some(at_nanos(previous_time_nanos)))
			.expect("resolution must succeed")
			.expect("a timed object must keep a #time across an update")
			.to_nanos()
	}

	#[test]
	fn a_time_less_update_stays_time_less() {
		// An update must not be a back door into acquiring a clock. Carrying the previous instant
		// forward would be harmless only if there were one; on a time-less object it would mean
		// inventing one on first edit.
		let shape = shape();

		assert_eq!(
			resolve_time_for_update(
				"tokens",
				&columns(),
				&TimeSource::None,
				&shape,
				&encoded_bytes(&shape, CORRECTED_TIME),
				None
			)
			.unwrap(),
			None
		);
	}

	#[test]
	fn the_two_domains_diverge_on_what_an_update_does_to_time() {
		// Re-stamping on processing time would re-date a row into a later window on any edit;
		// on event time the update must re-read what the author just changed. The row carries a
		// populator value unequal to the previous instant so the two arms cannot agree by accident.
		let shape = shape();
		let corrected = encoded_bytes(&shape, CORRECTED_TIME);

		assert_eq!(
			unwrapped_update("audit", &columns(), &TimeSource::Processing, &shape, &corrected, BLOCK_TIME),
			BLOCK_TIME,
			"a processing-time update must carry the original #time forward, not re-stamp it"
		);
		assert_eq!(
			unwrapped_update("trades", &columns(), &event(), &shape, &corrected, BLOCK_TIME),
			CORRECTED_TIME,
			"an event-time update must re-read the populator the author just edited"
		);
	}

	#[test]
	fn an_event_time_update_may_move_time_backwards() {
		// A row wrongly dated to next year has to be draggable back into the window it belongs to.
		let shape = shape();
		let earlier = encoded_bytes(&shape, BLOCK_TIME);

		assert_eq!(
			unwrapped_update("trades", &columns(), &event(), &shape, &earlier, ARRIVAL),
			BLOCK_TIME,
			"a correction to an earlier instant must be honoured"
		);
		assert!(BLOCK_TIME < ARRIVAL, "the corrected instant is genuinely earlier than what it replaces");
	}

	#[test]
	fn an_update_that_leaves_the_populator_alone_leaves_time_alone() {
		// A routine edit to an unrelated column must not walk a row across window boundaries;
		// the two domains may part ways only when the populator itself moved.
		let shape = shape();
		let untouched = encoded_bytes(&shape, BLOCK_TIME);

		for (label, time) in [("processing", TimeSource::Processing), ("event", event())] {
			assert_eq!(
				unwrapped_update("trades", &columns(), &time, &shape, &untouched, BLOCK_TIME),
				BLOCK_TIME,
				"{label}: an unrelated edit must not move #time"
			);
		}
	}

	#[test]
	fn an_unusable_populator_fails_the_update_instead_of_keeping_the_previous_time() {
		// A populator that no longer resolves means catalog and contents disagree; keeping the
		// stale instant would hide that while the object still claims to be event-time.
		let shape = shape();

		let absent = TimeSource::Event {
			ts: "no_such_column".to_string(),
		};
		let err = resolve_time_for_update(
			"trades",
			&columns(),
			&absent,
			&shape,
			&encoded_bytes(&shape, CORRECTED_TIME),
			Some(at_nanos(BLOCK_TIME)),
		)
		.expect_err("an absent populator must not resolve on update");
		assert_eq!(err.diagnostic().code, "TIME_001");

		let mut none_row = shape.allocate();
		shape.set_value(&mut none_row, 0, &Value::Utf8("sig".to_string()));
		shape.set_none(&mut none_row, 1);
		let err = resolve_time_for_update(
			"trades",
			&columns(),
			&event(),
			&shape,
			&none_row,
			Some(at_nanos(BLOCK_TIME)),
		)
		.expect_err("a none populator must not resolve on update");
		assert_eq!(err.diagnostic().code, "TIME_002");
	}

	#[test]
	fn the_update_resolution_does_not_depend_on_the_object_kind() {
		// Table, series and ringbuffer share the update resolver, so a domain's update semantics
		// cannot be honoured for one object kind and dropped for another.
		let shape = shape();
		let r = encoded_bytes(&shape, CORRECTED_TIME);

		for object in ["trades", "prices", "recent"] {
			assert_eq!(
				unwrapped_update(object, &columns(), &event(), &shape, &r, BLOCK_TIME),
				CORRECTED_TIME,
				"{object} resolved differently"
			);
			assert_eq!(
				unwrapped_update(object, &columns(), &TimeSource::Processing, &shape, &r, BLOCK_TIME),
				BLOCK_TIME,
				"{object} resolved differently"
			);
		}
	}
}
