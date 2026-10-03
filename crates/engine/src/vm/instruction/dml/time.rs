// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::NullBuffer;
use reifydb_codec::row::shape::RowShape;
use reifydb_core::common::TimeSource;
use reifydb_value::{
	Result,
	value::{
		Value,
		column_view::{ColumnView, ViewData},
		container::temporal_array::datetimes,
		datetime::DateTime,
		value_type::ValueType,
	},
};

use crate::error::EngineError;

pub(crate) fn populator_index(time: &TimeSource, shape: &RowShape) -> Option<usize> {
	let TimeSource::Event {
		ts,
	} = time
	else {
		return None;
	};
	shape.find_field_index(ts).filter(|&index| shape.fields()[index].constraint.get_type() == ValueType::DateTime)
}

pub(crate) struct EventColumn<'v> {
	values: Option<(&'v [DateTime], Option<NullBuffer>)>,
}

impl<'v> EventColumn<'v> {
	pub(crate) fn new(view: Option<ColumnView<'v>>) -> Self {
		let values = view.and_then(|view| match view.data {
			ViewData::DateTime(array) => Some((datetimes(array), view.logical_nulls())),
			_ => None,
		});
		Self {
			values,
		}
	}

	pub(crate) fn at(&self, row: usize) -> Option<DateTime> {
		let (values, nulls) = self.values.as_ref()?;
		match nulls {
			Some(nulls) if nulls.is_null(row) => None,
			_ => Some(values[row]),
		}
	}
}

pub(crate) fn resolve_time(
	object: &str,
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
		} => resolve_populator(object, ts, shape, row).map(Some),
	}
}

pub(crate) fn resolve_time_for_update(
	object: &str,
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
		} => resolve_populator(object, ts, shape, row).map(Some),
	}
}

fn resolve_populator(object: &str, ts: &str, shape: &RowShape, row: &[u8]) -> Result<DateTime> {
	let index = shape.find_field_index(ts).ok_or_else(|| EngineError::TimePopulatorMissing {
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
	use arrow_array::ArrayRef;
	use arrow_schema::FieldRef;
	use reifydb_codec::row::{
		bytes::{EncodedBytes, RowBuilder},
		shape::{RowFamily, RowShapeField},
	};
	use reifydb_core::value::column::builder::ColumnBuilder;
	use reifydb_value::{
		factory::time::at_nanos,
		value::{
			constraint::TypeConstraint,
			datetime::DateTime,
			dictionary::{DictionaryEntryId, DictionaryId},
			value_type::ValueType,
		},
	};

	use super::*;

	const ARRIVAL: i64 = 1_900_000_000_000_000_000;
	const BLOCK_TIME: i64 = 1_700_000_000_000_000_000;

	fn shape() -> RowShape {
		RowShape::new(
			RowFamily::Table,
			vec![
				RowShapeField::unconstrained("signature", ValueType::Utf8),
				RowShapeField::unconstrained("block_time", ValueType::DateTime),
			],
		)
	}

	fn encoded_bytes(shape: &RowShape, block_time_nanos: i64) -> EncodedBytes {
		let mut row = shape.allocate_table();
		shape.set_value(&mut row, 0, &Value::Utf8("sig".to_string()));
		shape.set_value(&mut row, 1, &Value::DateTime(DateTime::from_nanos(block_time_nanos)));
		row.freeze_bytes()
	}

	fn event() -> TimeSource {
		TimeSource::Event {
			ts: "block_time".to_string(),
		}
	}

	fn unwrapped(object: &str, time: &TimeSource, shape: &RowShape, row: &[u8], arrival_nanos: i64) -> i64 {
		resolve_time(object, time, shape, row, at_nanos(arrival_nanos))
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

		let stamped = unwrapped("trades", &event(), &shape, &encoded_bytes(&shape, BLOCK_TIME), ARRIVAL);

		assert_eq!(stamped, BLOCK_TIME, "#time must come from block_time, not from the write clock");
	}

	#[test]
	fn a_processing_time_object_stamps_time_from_arrival() {
		// An object that declares processing time wants ingest time as its clock, so the arrival
		// instant is the stamp rather than a fallback for having found no populator.
		let shape = shape();

		let stamped = unwrapped(
			"audit",
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

		let stamped = unwrapped("trades", &event(), &shape, &encoded_bytes(&shape, BLOCK_TIME), ARRIVAL);

		assert!(stamped < ARRIVAL, "a backfilled row's #time must predate its arrival");
		assert_eq!(ARRIVAL - stamped, 200_000_000_000_000_000);
	}

	#[test]
	fn the_populator_is_resolved_by_name_not_by_position() {
		// Resolving by position would pick the wrong column once another shares its type.
		let shape = RowShape::new(
			RowFamily::Table,
			vec![
				RowShapeField::unconstrained("block_time", ValueType::DateTime),
				RowShapeField::unconstrained("recorded_at", ValueType::DateTime),
			],
		);

		let mut r = shape.allocate_table();
		shape.set_value(&mut r, 0, &Value::DateTime(DateTime::from_nanos(BLOCK_TIME)));
		shape.set_value(&mut r, 1, &Value::DateTime(DateTime::from_nanos(ARRIVAL)));

		assert_eq!(unwrapped("trades", &event(), &shape, &r, 0), BLOCK_TIME);
	}

	#[test]
	fn the_resolution_does_not_depend_on_the_object_kind() {
		// Table, series, ringbuffer and queue share this resolver so a declaration cannot be
		// honoured for one object kind and dropped for another.
		let shape = shape();
		let r = encoded_bytes(&shape, BLOCK_TIME);

		for object in ["trades", "prices", "recent", "jobs"] {
			assert_eq!(
				unwrapped(object, &event(), &shape, &r, ARRIVAL),
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

		let err = resolve_time("trades", &time, &shape, &encoded_bytes(&shape, BLOCK_TIME), at_nanos(ARRIVAL))
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

		let err = resolve_time("trades", &time, &shape, &encoded_bytes(&shape, BLOCK_TIME), at_nanos(ARRIVAL))
			.expect_err("a utf8 populator must not resolve");

		assert_eq!(err.diagnostic().code, "TIME_002");
	}

	#[test]
	fn a_none_populator_fails_the_write() {
		// Pinned separately because none travels a different path through get_value than a
		// wrong-typed value does.
		let shape = shape();
		let mut r = shape.allocate_table();
		shape.set_value(&mut r, 0, &Value::Utf8("sig".to_string()));
		shape.set_none(&mut r, 1);

		let err = resolve_time("trades", &event(), &shape, &r, at_nanos(ARRIVAL))
			.expect_err("a none populator must not resolve");

		assert_eq!(err.diagnostic().code, "TIME_002");
	}

	const CORRECTED_TIME: i64 = 1_650_000_000_000_000_000;

	fn unwrapped_update(
		object: &str,
		time: &TimeSource,
		shape: &RowShape,
		row: &[u8],
		previous_time_nanos: i64,
	) -> i64 {
		resolve_time_for_update(object, time, shape, row, Some(at_nanos(previous_time_nanos)))
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
			unwrapped_update("audit", &TimeSource::Processing, &shape, &corrected, BLOCK_TIME),
			BLOCK_TIME,
			"a processing-time update must carry the original #time forward, not re-stamp it"
		);
		assert_eq!(
			unwrapped_update("trades", &event(), &shape, &corrected, BLOCK_TIME),
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
			unwrapped_update("trades", &event(), &shape, &earlier, ARRIVAL),
			BLOCK_TIME,
			"a correction to an earlier instant must be honoured"
		);
		const {
			assert!(
				BLOCK_TIME < ARRIVAL,
				"the corrected instant is genuinely earlier than what it replaces"
			)
		};
	}

	#[test]
	fn an_update_that_leaves_the_populator_alone_leaves_time_alone() {
		// A routine edit to an unrelated column must not walk a row across window boundaries;
		// the two domains may part ways only when the populator itself moved.
		let shape = shape();
		let untouched = encoded_bytes(&shape, BLOCK_TIME);

		for (label, time) in [("processing", TimeSource::Processing), ("event", event())] {
			assert_eq!(
				unwrapped_update("trades", &time, &shape, &untouched, BLOCK_TIME),
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
			&absent,
			&shape,
			&encoded_bytes(&shape, CORRECTED_TIME),
			Some(at_nanos(BLOCK_TIME)),
		)
		.expect_err("an absent populator must not resolve on update");
		assert_eq!(err.diagnostic().code, "TIME_001");

		let mut none_row = shape.allocate_table();
		shape.set_value(&mut none_row, 0, &Value::Utf8("sig".to_string()));
		shape.set_none(&mut none_row, 1);
		let err = resolve_time_for_update("trades", &event(), &shape, &none_row, Some(at_nanos(BLOCK_TIME)))
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
				unwrapped_update(object, &event(), &shape, &r, BLOCK_TIME),
				CORRECTED_TIME,
				"{object} resolved differently"
			);
			assert_eq!(
				unwrapped_update(object, &TimeSource::Processing, &shape, &r, BLOCK_TIME),
				BLOCK_TIME,
				"{object} resolved differently"
			);
		}
	}

	type Outcome = std::result::Result<Option<i64>, (String, String)>;

	fn outcome(result: Result<Option<DateTime>>) -> Outcome {
		result.map(|time| time.map(|time| time.to_nanos())).map_err(|err| {
			let diagnostic = err.diagnostic();
			(diagnostic.code, diagnostic.message)
		})
	}

	fn column_of(name: &str, ty: ValueType, values: Vec<Value>) -> (FieldRef, ArrayRef) {
		let mut builder = ColumnBuilder::with_capacity(ty, values.len());
		for value in values {
			builder.push_value(value);
		}
		builder.finish(name)
	}

	fn datetimes_of(name: &str, nanos: &[Option<i64>]) -> (FieldRef, ArrayRef) {
		let values =
			nanos.iter().map(|n| n.map_or_else(Value::none, |n| Value::DateTime(DateTime::from_nanos(n))));
		column_of(name, ValueType::DateTime, values.collect())
	}

	fn write_rows<B: RowBuilder>(shape: &RowShape, views: &[ColumnView<'_>], mut rows: Vec<B>) -> Vec<Vec<u8>> {
		shape.write_columns(&mut rows, views).expect("the columns write");
		rows.iter().map(|row| row.as_slice().to_vec()).collect()
	}

	fn written(shape: &RowShape, views: &[ColumnView<'_>]) -> Vec<Vec<u8>> {
		let count = views[0].len();
		match shape.family() {
			RowFamily::Table => {
				write_rows(shape, views, (0..count).map(|_| shape.allocate_table()).collect())
			}
			RowFamily::RingBuffer => {
				write_rows(shape, views, (0..count).map(|_| shape.allocate_ringbuffer()).collect())
			}
			RowFamily::Queue => {
				write_rows(shape, views, (0..count).map(|_| shape.allocate_queue()).collect())
			}
			RowFamily::Series => {
				write_rows(shape, views, (0..count).map(|_| shape.allocate_series()).collect())
			}
			family => panic!("no rows for {family:?}"),
		}
	}

	fn batch_outcomes(
		object: &str,
		time: &TimeSource,
		shape: &RowShape,
		columns: &[(FieldRef, ArrayRef)],
	) -> (Vec<Outcome>, Vec<usize>) {
		let views: Vec<ColumnView<'_>> =
			columns.iter().map(|column| ColumnView::try_from(column).expect("the view reads")).collect();
		let rows = written(shape, &views);
		let event = EventColumn::new(populator_index(time, shape).map(|index| views[index].clone()));
		let arrival = at_nanos(ARRIVAL);
		let previous = Some(at_nanos(BLOCK_TIME));
		let mut outcomes = Vec::new();
		let mut hits = Vec::new();
		for (index, row) in rows.iter().enumerate() {
			if event.at(index).is_some() {
				hits.push(index);
			}
			let insert = event
				.at(index)
				.map_or_else(|| resolve_time(object, time, shape, row, arrival), |time| Ok(Some(time)));
			let update = event.at(index).map_or_else(
				|| resolve_time_for_update(object, time, shape, row, previous),
				|time| Ok(Some(time)),
			);
			let (insert, update) = (outcome(insert), outcome(update));
			assert_eq!(
				insert,
				outcome(resolve_time(object, time, shape, row, arrival)),
				"{object} row {index}"
			);
			assert_eq!(
				update,
				outcome(resolve_time_for_update(object, time, shape, row, previous)),
				"{object} update row {index}"
			);
			outcomes.push(insert);
		}
		(outcomes, hits)
	}

	#[test]
	fn the_batch_reader_agrees_with_the_row_resolver() {
		// The slice read must match the stored row exactly in value, none handling and error text.
		let none_text =
			"`trades.block_time` is the declared #time populator but holds None { inner: Any } on this row";
		for family in [RowFamily::Table, RowFamily::RingBuffer, RowFamily::Queue] {
			let shape = RowShape::new(
				family,
				vec![
					RowShapeField::unconstrained("signature", ValueType::Utf8),
					RowShapeField::unconstrained("block_time", ValueType::DateTime),
				],
			);
			let columns = [
				column_of(
					"signature",
					ValueType::Utf8,
					vec![Value::utf8("a"), Value::utf8("b"), Value::utf8("c")],
				),
				datetimes_of("block_time", &[Some(BLOCK_TIME), None, Some(CORRECTED_TIME)]),
			];
			assert_eq!(
				batch_outcomes("trades", &event(), &shape, &columns),
				(
					vec![
						Ok(Some(BLOCK_TIME)),
						Err(("TIME_002".to_string(), none_text.to_string())),
						Ok(Some(CORRECTED_TIME))
					],
					vec![0, 2]
				),
				"{family:?}"
			);
			let (absent, hits) = batch_outcomes(
				"trades",
				&TimeSource::Event {
					ts: "no_such_column".to_string(),
				},
				&shape,
				&columns,
			);
			assert!(absent.iter().all(|o| matches!(o, Err((code, _)) if code == "TIME_001")), "{absent:?}");
			assert_eq!(hits, Vec::<usize>::new());
			assert_eq!(
				batch_outcomes("audit", &TimeSource::Processing, &shape, &columns),
				(vec![Ok(Some(ARRIVAL)); 3], vec![])
			);
			assert_eq!(
				batch_outcomes("tokens", &TimeSource::None, &shape, &columns),
				(vec![Ok(None); 3], vec![])
			);
		}

		let declared_before_key = RowShape::new(
			RowFamily::Series,
			vec![
				RowShapeField::unconstrained("k", ValueType::Int8),
				RowShapeField::unconstrained("at", ValueType::DateTime),
				RowShapeField::unconstrained("v", ValueType::Int4),
			],
		);
		let columns = [
			column_of("k", ValueType::Int8, vec![Value::Int8(1), Value::Int8(2)]),
			datetimes_of("at", &[Some(BLOCK_TIME), Some(CORRECTED_TIME)]),
			column_of("v", ValueType::Int4, vec![Value::Int4(7), Value::Int4(8)]),
		];
		let at = TimeSource::Event {
			ts: "at".to_string(),
		};
		assert_eq!(
			batch_outcomes("prices", &at, &declared_before_key, &columns),
			(vec![Ok(Some(BLOCK_TIME)), Ok(Some(CORRECTED_TIME))], vec![0, 1])
		);

		let key_is_populator = RowShape::new(
			RowFamily::Series,
			vec![
				RowShapeField::unconstrained("ts", ValueType::DateTime),
				RowShapeField::unconstrained("v", ValueType::Int4),
			],
		);
		let columns = [
			datetimes_of("ts", &[Some(BLOCK_TIME), Some(CORRECTED_TIME)]),
			column_of("v", ValueType::Int4, vec![Value::Int4(7), Value::Int4(8)]),
		];
		let ts = TimeSource::Event {
			ts: "ts".to_string(),
		};
		assert_eq!(
			batch_outcomes("prices", &ts, &key_is_populator, &columns),
			(vec![Ok(Some(BLOCK_TIME)), Ok(Some(CORRECTED_TIME))], vec![0, 1])
		);

		let dictionary = RowShape::new(
			RowFamily::Table,
			vec![RowShapeField::new("at", TypeConstraint::dictionary(DictionaryId(1), ValueType::Uint4))],
		);
		let ids = [DictionaryEntryId::U4(1), DictionaryEntryId::U4(2)];
		let columns = [column_of("at", ValueType::DictionaryId, ids.iter().map(|id| id.to_value()).collect())];
		let found = ids.map(|id| {
			format!("`trades.at` is the declared #time populator but holds {:?} on this row", id.to_value())
		});
		assert_eq!(
			batch_outcomes("trades", &at, &dictionary, &columns),
			(found.iter().map(|text| Err(("TIME_002".to_string(), text.clone()))).collect(), vec![])
		);
	}
}
