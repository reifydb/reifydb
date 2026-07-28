// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Resolution of a row's `#time` at the write boundary.
//!
//! Invariant: `#time` is substrate-owned. Every source object kind (table, series, ringbuffer, queue) resolves it
//! here, so an event-time declaration cannot be honoured for one object kind and silently ignored for another. An
//! event-time object reads the column it declared; a processing-time object takes the arrival clock. A row is never
//! left unstamped, because a row without a time is unrepresentable.
//!
//! An unusable populator fails the write rather than falling back to the arrival clock. Definition-time validation
//! already rejects both cases, so reaching them means the catalog disagrees with the row; substituting arrival time
//! there would silently re-date a replayed row to now, which is the exact failure event time exists to prevent.

use reifydb_codec::encoded::{row::EncodedRow, shape::RowShape};
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
	row: &EncodedRow,
	arrival: DateTime,
) -> Result<DateTime> {
	let Some(ts_column) = time.ts() else {
		return Ok(arrival);
	};

	let index =
		columns.iter().position(|c| c.name == ts_column).ok_or_else(|| EngineError::TimePopulatorMissing {
			object: object.to_string(),
			column: ts_column.to_string(),
		})?;

	match shape.get_value(row, index) {
		Value::DateTime(dt) => Ok(dt),
		found => Err(EngineError::TimePopulatorNotDateTime {
			object: object.to_string(),
			column: ts_column.to_string(),
			found: format!("{found:?}"),
		}
		.into()),
	}
}

pub(crate) fn resolve_time_for_update(
	object: &str,
	columns: &[Column],
	time: &TimeSource,
	shape: &RowShape,
	row: &EncodedRow,
	previous_time: DateTime,
) -> Result<DateTime> {
	match time {
		TimeSource::Processing => Ok(previous_time),
		TimeSource::Event {
			..
		} => resolve_time(object, columns, time, shape, row, previous_time),
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::encoded::shape::RowShapeField;
	use reifydb_core::interface::catalog::{
		column::{Column, ColumnIndex},
		id::ColumnId,
	};
	use reifydb_value::value::{constraint::TypeConstraint, datetime::DateTime, value_type::ValueType};

	use super::*;

	const ARRIVAL: u64 = 1_900_000_000_000_000_000;
	const BLOCK_TIME: u64 = 1_700_000_000_000_000_000;

	fn at(nanos: u64) -> DateTime {
		DateTime::from_nanos(nanos)
	}

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
		RowShape::new(vec![
			RowShapeField::unconstrained("signature", ValueType::Utf8),
			RowShapeField::unconstrained("block_time", ValueType::DateTime),
		])
	}

	fn row(shape: &RowShape, block_time_nanos: u64) -> EncodedRow {
		let mut row = shape.allocate();
		shape.set_value(&mut row, 0, &Value::Utf8("sig".to_string()));
		shape.set_value(&mut row, 1, &Value::DateTime(DateTime::from_nanos(block_time_nanos)));
		row
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
		row: &EncodedRow,
		arrival_nanos: u64,
	) -> u64 {
		resolve_time(object, columns, time, shape, row, at(arrival_nanos))
			.expect("resolution must succeed")
			.to_nanos()
	}

	#[test]
	// Intent: an event-time object stamps #time from the column the author declared, not from the clock. This is
	// the property the whole redesign rests on - it is what makes a replay of an old corpus reproduce production's
	// retention decisions instead of re-dating every row to now.
	// Mutation: return arrival_nanos unconditionally and this fails with the wall clock.
	fn an_event_time_object_stamps_time_from_the_declared_populator() {
		let shape = shape();

		let stamped = unwrapped("trades", &columns(), &event(), &shape, &row(&shape, BLOCK_TIME), ARRIVAL);

		assert_eq!(stamped, BLOCK_TIME, "#time must come from block_time, not from the write clock");
	}

	#[test]
	// Intent: an object that declares nothing is processing-time, and its #time is arrival. Silence is a
	// legitimate declaration and must not leave #time unset - a row without a time is unrepresentable.
	fn a_processing_time_object_stamps_time_from_arrival() {
		let shape = shape();

		let stamped = unwrapped(
			"audit",
			&columns(),
			&TimeSource::Processing,
			&shape,
			&row(&shape, BLOCK_TIME),
			ARRIVAL,
		);

		assert_eq!(stamped, ARRIVAL);
	}

	#[test]
	// Intent: the replay property in miniature. When the populator value is OLDER than the write, #time and the
	// wall stamps must diverge - #time says when the event happened, created_at says when this database learned
	// about it. A backfill of week-old data must land at its own event time, or every windowed rollup over it
	// buckets into today.
	// Mutation: populate #time from the wall clock and the two collapse onto each other here.
	fn time_diverges_from_arrival_when_the_event_predates_the_write() {
		let shape = shape();

		let stamped = unwrapped("trades", &columns(), &event(), &shape, &row(&shape, BLOCK_TIME), ARRIVAL);

		assert!(stamped < ARRIVAL, "a backfilled row's #time must predate its arrival");
		assert_eq!(ARRIVAL - stamped, 200_000_000_000_000_000);
	}

	#[test]
	// Intent: the populator is resolved by NAME against the object's own columns, so it keeps working when the
	// declared column is not the last one and when other columns share its type.
	// Mutation: hardcode the last column index and this returns the wrong column's value.
	fn the_populator_is_resolved_by_name_not_by_position() {
		let shape = RowShape::new(vec![
			RowShapeField::unconstrained("block_time", ValueType::DateTime),
			RowShapeField::unconstrained("recorded_at", ValueType::DateTime),
		]);
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
	// Intent: the resolver is object-agnostic on purpose. Table, series, ringbuffer and queue all route through
	// this one function precisely so a declaration cannot be honoured for one object kind and dropped for another,
	// which is what four hand-rolled copies would eventually produce. Nothing in the signature can name an object
	// kind, so this asserts the same inputs give the same answer whatever the object is called.
	fn the_resolution_does_not_depend_on_the_object_kind() {
		let shape = shape();
		let r = row(&shape, BLOCK_TIME);

		for object in ["trades", "prices", "recent", "jobs"] {
			assert_eq!(
				unwrapped(object, &columns(), &event(), &shape, &r, ARRIVAL),
				BLOCK_TIME,
				"{object} resolved differently"
			);
		}
	}

	#[test]
	// Intent: a populator naming a column the object does not have must FAIL the write, not quietly fall back to
	// the arrival clock. The fallback is the dangerous outcome: rows would keep being written, every one of them
	// stamped with now, and a windowed rollup over them would look plausible while being wrong. Definition-time
	// validation already rejects this, so an error here is the second line of defence, not the first.
	// Mutation: return arrival_nanos instead of the error and this fails, because ARRIVAL comes back as an Ok.
	fn an_absent_populator_column_fails_the_write_instead_of_falling_back() {
		let shape = shape();
		let time = TimeSource::Event {
			ts: "no_such_column".to_string(),
		};

		let err = resolve_time("trades", &columns(), &time, &shape, &row(&shape, BLOCK_TIME), at(ARRIVAL))
			.expect_err("an absent populator must not resolve");

		assert_eq!(err.diagnostic().code, "TIME_001");
	}

	#[test]
	// Intent: same second line of defence for a populator that exists but does not hold a DateTime on this row -
	// including the none case, which is what a nullable populator column would produce. Falling back would date the
	// row to now while the object claims to be event-time.
	// Mutation: return arrival_nanos for the non-DateTime arm and this fails.
	fn a_populator_that_is_not_a_datetime_fails_the_write() {
		let shape = shape();
		let time = TimeSource::Event {
			ts: "signature".to_string(),
		};

		let err = resolve_time("trades", &columns(), &time, &shape, &row(&shape, BLOCK_TIME), at(ARRIVAL))
			.expect_err("a utf8 populator must not resolve");

		assert_eq!(err.diagnostic().code, "TIME_002");
	}

	#[test]
	// Intent: a none in the populator is the realistic version of the previous case - the column is declared and of
	// the right type, but this row left it empty. It must be refused for the same reason, and it is worth pinning
	// separately because none travels a different path through get_value than a wrong-typed value does.
	fn a_none_populator_fails_the_write() {
		let shape = shape();
		let mut r = shape.allocate();
		shape.set_value(&mut r, 0, &Value::Utf8("sig".to_string()));
		shape.set_none(&mut r, 1);

		let err = resolve_time("trades", &columns(), &event(), &shape, &r, at(ARRIVAL))
			.expect_err("a none populator must not resolve");

		assert_eq!(err.diagnostic().code, "TIME_002");
	}

	const CORRECTED_TIME: u64 = 1_650_000_000_000_000_000;

	fn unwrapped_update(
		object: &str,
		columns: &[Column],
		time: &TimeSource,
		shape: &RowShape,
		row: &EncodedRow,
		previous_time_nanos: u64,
	) -> u64 {
		resolve_time_for_update(object, columns, time, shape, row, at(previous_time_nanos))
			.expect("resolution must succeed")
			.to_nanos()
	}

	#[test]
	// Intent: THE update-lifecycle divergence between the two domains, written as one table so it cannot drift. On
	// processing time an update carries the ORIGINAL #time forward - the row is still a record of the same arrival,
	// and re-stamping it would silently re-date it, moving it into a later window every time anything about it is
	// edited. On event time the same update re-reads the populator, because the author edited what the row claims
	// happened. Both arms are handed the same previous instant and the same row, so the domain is the only thing that
	// differs, and the row deliberately carries a populator value that is not the previous instant - otherwise the two
	// columns of this table would agree by accident.
	// Mutation: route event through the processing passthrough and a corrected event timestamp never reaches #time.
	fn the_two_domains_diverge_on_what_an_update_does_to_time() {
		let shape = shape();
		let corrected = row(&shape, CORRECTED_TIME);

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
	// Intent: an update is the only path on which #time can move backwards, and it must be allowed to. Correcting a
	// mis-entered event timestamp to an EARLIER instant is the whole reason the event arm re-reads rather than keeps -
	// a row wrongly dated to next year has to be draggable back into the window it belongs to.
	// Mutation: clamp to max(previous, resolved) and this fails while the forward correction above still passes.
	fn an_event_time_update_may_move_time_backwards() {
		let shape = shape();
		let earlier = row(&shape, BLOCK_TIME);

		assert_eq!(
			unwrapped_update("trades", &columns(), &event(), &shape, &earlier, ARRIVAL),
			BLOCK_TIME,
			"a correction to an earlier instant must be honoured"
		);
		assert!(BLOCK_TIME < ARRIVAL, "the corrected instant is genuinely earlier than what it replaces");
	}

	#[test]
	// Intent: an update that leaves the populator alone leaves #time alone. Re-reading is not re-stamping: an edit to
	// some other column must not disturb the instant, or a routine field update would walk a row across window
	// boundaries. Processing time reaches this by keeping, event time by re-reading a value that did not change; both
	// have to land on the same answer, which is what makes an unrelated edit safe in either domain. This is the
	// converse of the divergence table above - the two domains must part ways only when the populator itself moved.
	fn an_update_that_leaves_the_populator_alone_leaves_time_alone() {
		let shape = shape();
		let untouched = row(&shape, BLOCK_TIME);

		for (label, time) in [("processing", TimeSource::Processing), ("event", event())] {
			assert_eq!(
				unwrapped_update("trades", &columns(), &time, &shape, &untouched, BLOCK_TIME),
				BLOCK_TIME,
				"{label}: an unrelated edit must not move #time"
			);
		}
	}

	#[test]
	// Intent: the second line of defence holds on the update path too. previous_time_nanos is handed to the event arm
	// in the slot the insert path uses for the arrival clock, so a populator that stops resolving could plausibly be
	// answered with the old #time instead of an error. It must not be: a row whose populator no longer resolves is a
	// row whose catalog and contents disagree, and keeping the stale instant would hide that while the object still
	// claims to be event-time.
	// Mutation: make the event arm fall back to previous_time_nanos and both cases here return BLOCK_TIME as an Ok.
	fn an_unusable_populator_fails_the_update_instead_of_keeping_the_previous_time() {
		let shape = shape();

		let absent = TimeSource::Event {
			ts: "no_such_column".to_string(),
		};
		let err = resolve_time_for_update(
			"trades",
			&columns(),
			&absent,
			&shape,
			&row(&shape, CORRECTED_TIME),
			at(BLOCK_TIME),
		)
		.expect_err("an absent populator must not resolve on update");
		assert_eq!(err.diagnostic().code, "TIME_001");

		let mut none_row = shape.allocate();
		shape.set_value(&mut none_row, 0, &Value::Utf8("sig".to_string()));
		shape.set_none(&mut none_row, 1);
		let err = resolve_time_for_update("trades", &columns(), &event(), &shape, &none_row, at(BLOCK_TIME))
			.expect_err("a none populator must not resolve on update");
		assert_eq!(err.diagnostic().code, "TIME_002");
	}

	#[test]
	// Intent: the update resolver is object-agnostic for the same reason the insert one is - table, series and
	// ringbuffer all route their updates through it so a domain's update semantics cannot be honoured for one object
	// kind and dropped for another.
	fn the_update_resolution_does_not_depend_on_the_object_kind() {
		let shape = shape();
		let r = row(&shape, CORRECTED_TIME);

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
