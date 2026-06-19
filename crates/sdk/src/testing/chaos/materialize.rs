// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::flow::diff::DiffType;
use reifydb_core::{
	encoded::shape::SHAPE_HEADER_SIZE, interface::change::Change, row::Row, value::column::columns::Columns,
};
use reifydb_value::{
	reifydb_assertions,
	value::{Value, date::Date, datetime::DateTime, duration::Duration, time::Time, value_type::ValueType},
};

use super::{
	event::{ChaosBatch, ChaosEvent},
	oracle::{MaterializedRow, MaterializedTable, OutputKey},
};

pub fn materialize_history(history: &[Change], output_key_columns: &[String]) -> MaterializedTable {
	let mut table = MaterializedTable::empty();
	for change in history {
		for diff in change.diffs.iter() {
			match diff.kind() {
				DiffType::Insert | DiffType::Update => {
					if let Some(post) = diff.post() {
						apply_columns(&mut table, post, output_key_columns, false);
					}
				}
				DiffType::Remove => {
					if let Some(pre) = diff.pre() {
						apply_columns(&mut table, pre, output_key_columns, true);
					}
				}
			}
		}
	}
	table
}

pub fn materialize_batches(batches: &[ChaosBatch], output_key_columns: &[String]) -> MaterializedTable {
	let mut table = MaterializedTable::empty();
	for batch in batches {
		for ev in &batch.events {
			match ev {
				ChaosEvent::Insert {
					row,
					..
				}
				| ChaosEvent::Update {
					post: row,
					..
				} => {
					let r = row_to_materialized(row);
					let key = project_key(&r, output_key_columns);
					table.insert(key, r);
				}
				ChaosEvent::Remove {
					row,
					..
				} => {
					let r = row_to_materialized(row);
					let key = project_key(&r, output_key_columns);
					table.remove(&key);
				}
			}
		}
	}
	table
}

fn apply_columns(table: &mut MaterializedTable, columns: &Columns, output_key_columns: &[String], remove: bool) {
	let column_names: Vec<String> = columns.iter().map(|c| c.name().text().to_string()).collect();
	for i in 0..columns.row_count() {
		let values = columns.row(i);
		reifydb_assertions! {
			assert_eq!(values.len(), column_names.len());
		}
		let mat = MaterializedRow::from_pairs(column_names.iter().cloned().zip(values.into_iter()));
		let key = project_key(&mat, output_key_columns);
		if remove {
			table.remove(&key);
		} else {
			table.insert(key, mat);
		}
	}
}

fn row_to_materialized(row: &Row) -> MaterializedRow {
	let mut mat = MaterializedRow::new();
	let bitvec_size = row.shape.fields().len().div_ceil(8);
	let bitvec_start = SHAPE_HEADER_SIZE;
	let bitvec_end = bitvec_start + bitvec_size;
	let bitvec = if row.encoded.as_slice().len() >= bitvec_end {
		Some(&row.encoded.as_slice()[bitvec_start..bitvec_end])
	} else {
		None
	};
	for (idx, field) in row.shape.fields().iter().enumerate() {
		let defined = match bitvec {
			Some(bv) => {
				let byte_idx = idx / 8;
				let bit_idx = idx % 8;
				bv.get(byte_idx).map(|b| b & (1 << bit_idx) != 0).unwrap_or(true)
			}
			None => true,
		};
		if !defined {
			mat.set(field.name.clone(), Value::none_of(field.constraint.get_type()));
			continue;
		}

		let off = field.offset as usize;
		let size = field.size as usize;
		let buf = &row.encoded.as_slice()[off..off + size];
		let v = match field.constraint.get_type() {
			ValueType::Boolean => {
				let b = buf[0] != 0;
				Value::Boolean(b)
			}
			ValueType::Int1 => Value::int8(buf[0] as i8 as i64),
			ValueType::Int2 => Value::int8(i16::from_le_bytes([buf[0], buf[1]]) as i64),
			ValueType::Int4 => Value::int8(i32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as i64),
			ValueType::Int8 => {
				let mut b = [0u8; 8];
				b.copy_from_slice(&buf[..8]);
				Value::int8(i64::from_le_bytes(b))
			}
			ValueType::Uint1 => Value::uint8(buf[0] as u64),
			ValueType::Uint2 => Value::uint8(u16::from_le_bytes([buf[0], buf[1]]) as u64),
			ValueType::Uint4 => Value::uint8(u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as u64),
			ValueType::Uint8 => {
				let mut b = [0u8; 8];
				b.copy_from_slice(&buf[..8]);
				Value::uint8(u64::from_le_bytes(b))
			}
			ValueType::Float4 => {
				let mut b = [0u8; 4];
				b.copy_from_slice(&buf[..4]);
				Value::float4(f32::from_le_bytes(b))
			}
			ValueType::Float8 => {
				let mut b = [0u8; 8];
				b.copy_from_slice(&buf[..8]);
				Value::float8(f64::from_le_bytes(b))
			}
			ValueType::DateTime => {
				let mut b = [0u8; 8];
				b.copy_from_slice(&buf[..8]);
				Value::datetime(DateTime::from_nanos(u64::from_le_bytes(b)))
			}
			ValueType::Duration => {
				let mut months_b = [0u8; 4];
				months_b.copy_from_slice(&buf[..4]);
				let mut days_b = [0u8; 4];
				days_b.copy_from_slice(&buf[4..8]);
				let mut nanos_b = [0u8; 8];
				nanos_b.copy_from_slice(&buf[8..16]);
				let months = i32::from_le_bytes(months_b);
				let days = i32::from_le_bytes(days_b);
				let nanos = i64::from_le_bytes(nanos_b);
				match Duration::new(months, days, nanos) {
					Ok(d) => Value::duration(d),
					Err(_) => Value::none_of(ValueType::Duration),
				}
			}

			ValueType::Date => {
				let mut b = [0u8; 4];
				b.copy_from_slice(&buf[..4]);
				match Date::from_days_since_epoch(i32::from_le_bytes(b)) {
					Some(d) => Value::date(d),
					None => Value::none_of(ValueType::Date),
				}
			}
			ValueType::Time => {
				let mut b = [0u8; 8];
				b.copy_from_slice(&buf[..8]);
				match Time::from_nanos_since_midnight(u64::from_le_bytes(b)) {
					Some(t) => Value::time(t),
					None => Value::none_of(ValueType::Time),
				}
			}
			other => Value::none_of(other),
		};
		mat.set(field.name.clone(), v);
	}
	mat
}

fn project_key(row: &MaterializedRow, output_key_columns: &[String]) -> OutputKey {
	let values: Vec<Value> =
		output_key_columns.iter().map(|name| row.get(name).cloned().unwrap_or_else(Value::none)).collect();
	OutputKey::new(values)
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		common::CommitVersion,
		encoded::shape::{RowShape, RowShapeField},
		interface::{
			catalog::shape::ShapeId,
			change::{Change, ChangeOrigin, Diff, Diffs},
		},
		row::Row,
		value::column::columns::Columns,
	};
	use reifydb_value::value::{
		Value, date::Date, datetime::DateTime, duration::Duration, row_number::RowNumber, time::Time,
		value_type::ValueType,
	};

	use super::*;
	use crate::testing::builders::TestRowBuilder;

	fn shape() -> RowShape {
		RowShape::new(vec![
			RowShapeField::unconstrained("k", ValueType::Uint8),
			RowShapeField::unconstrained("v", ValueType::Float8),
		])
	}

	fn build_row(rn: u64, k: u64, v: f64) -> Row {
		TestRowBuilder::new(RowNumber(rn))
			.with_shape(shape())
			.with_values(vec![Value::uint8(k), Value::float8(v)])
			.build()
	}

	fn change(diffs: Vec<Diff>) -> Change {
		Change {
			origin: ChangeOrigin::Shape(ShapeId::table(1)),
			diffs: Diffs::from_iter(diffs),
			version: CommitVersion(1),
			changed_at: DateTime::default(),
		}
	}

	#[test]
	fn insert_then_update_yields_post_state() {
		let history = vec![
			change(vec![Diff::insert(Columns::from_row(&build_row(1, 7, 1.0)))]),
			change(vec![Diff::update(
				Columns::from_row(&build_row(1, 7, 1.0)),
				Columns::from_row(&build_row(1, 7, 2.5)),
			)]),
		];
		let table = materialize_history(&history, &["k".to_string()]);
		assert_eq!(table.len(), 1);
		let row = table.get(&OutputKey::new(vec![Value::uint8(7u64)])).unwrap();
		assert_eq!(row.get("v"), Some(&Value::float8(2.5_f64)));
	}

	#[test]
	fn remove_drops_row_by_output_key() {
		let history = vec![
			change(vec![Diff::insert(Columns::from_row(&build_row(1, 7, 1.0)))]),
			change(vec![Diff::insert(Columns::from_row(&build_row(2, 8, 9.0)))]),
			change(vec![Diff::remove(Columns::from_row(&build_row(1, 7, 1.0)))]),
		];
		let table = materialize_history(&history, &["k".to_string()]);
		assert_eq!(table.len(), 1);
		assert!(table.get(&OutputKey::new(vec![Value::uint8(7u64)])).is_none());
		assert!(table.get(&OutputKey::new(vec![Value::uint8(8u64)])).is_some());
	}

	#[test]
	fn many_inserts_with_collisions_keep_latest() {
		// Same output key appears in multiple Inserts (legal at the
		// materialization layer because the operator may have remapped
		// RowNumbers). The last one wins.
		let history = vec![change(vec![
			Diff::insert(Columns::from_row(&build_row(1, 5, 10.0))),
			Diff::insert(Columns::from_row(&build_row(2, 5, 20.0))),
			Diff::insert(Columns::from_row(&build_row(3, 5, 30.0))),
		])];
		let table = materialize_history(&history, &["k".to_string()]);
		assert_eq!(table.len(), 1);
		let row = table.get(&OutputKey::new(vec![Value::uint8(5u64)])).unwrap();
		assert_eq!(row.get("v"), Some(&Value::float8(30.0_f64)));
	}

	#[test]
	fn empty_history_yields_empty_table() {
		let table = materialize_history(&[], &["k".to_string()]);
		assert!(table.is_empty());
	}

	#[test]
	fn multi_column_output_key() {
		let s = RowShape::new(vec![
			RowShapeField::unconstrained("base", ValueType::Uint8),
			RowShapeField::unconstrained("quote", ValueType::Uint8),
			RowShapeField::unconstrained("v", ValueType::Float8),
		]);
		fn r(s: &RowShape, rn: u64, base: u64, quote: u64, v: f64) -> Row {
			TestRowBuilder::new(RowNumber(rn))
				.with_shape(s.clone())
				.with_values(vec![Value::uint8(base), Value::uint8(quote), Value::float8(v)])
				.build()
		}
		let history = vec![change(vec![
			Diff::insert(Columns::from_row(&r(&s, 1, 1, 100, 1.0))),
			Diff::insert(Columns::from_row(&r(&s, 2, 1, 200, 2.0))),
			Diff::insert(Columns::from_row(&r(&s, 3, 2, 100, 3.0))),
		])];
		let table = materialize_history(&history, &["base".to_string(), "quote".to_string()]);
		assert_eq!(table.len(), 3);
		assert!(table.get(&OutputKey::new(vec![Value::uint8(1u64), Value::uint8(100u64)])).is_some());
		assert!(table.get(&OutputKey::new(vec![Value::uint8(1u64), Value::uint8(200u64)])).is_some());
		assert!(table.get(&OutputKey::new(vec![Value::uint8(2u64), Value::uint8(100u64)])).is_some());
	}

	#[test]
	fn materialize_batches_inserts_updates_removes() {
		// Smoke test the batch-side fold against a constructed log.
		let s = shape();
		let row1 = build_row(1, 7, 1.0);
		let row2 = build_row(1, 7, 2.5);
		let events = vec![
			ChaosEvent::Insert {
				row_number: RowNumber(1),
				row: row1.clone(),
			},
			ChaosEvent::Update {
				row_number: RowNumber(1),
				pre: row1.clone(),
				post: row2.clone(),
			},
		];
		let batches = vec![ChaosBatch::new(events.clone())];
		let table = materialize_batches(&batches, &["k".to_string()]);
		assert_eq!(table.len(), 1);
		let row = table.get(&OutputKey::new(vec![Value::uint8(7u64)])).unwrap();
		assert_eq!(row.get("v"), Some(&Value::float8(2.5_f64)));

		// Now remove it.
		let mut events_with_remove = events;
		events_with_remove.push(ChaosEvent::Remove {
			row_number: RowNumber(1),
			row: row2,
		});
		let batches = vec![ChaosBatch::new(events_with_remove)];
		let table = materialize_batches(&batches, &["k".to_string()]);
		assert!(table.is_empty());

		// Suppress unused-shape warning in this test.
		let _ = s;
	}

	#[test]
	fn datetime_and_duration_columns_survive_materialization() {
		// A DateTime/Duration column must round-trip through
		// row_to_materialized intact. Before DateTime/Duration cases
		// existed they fell through to Value::none_of, nulling the
		// operator's emitted window_start and breaking output-key
		// comparison.
		let s = RowShape::new(vec![
			RowShapeField::unconstrained("window_start", ValueType::DateTime),
			RowShapeField::unconstrained("window_duration", ValueType::Duration),
			RowShapeField::unconstrained("v", ValueType::Float8),
		]);
		let window_start = DateTime::from_timestamp(1_700_000_000).unwrap();
		let window_duration = Duration::from_seconds(60).unwrap();
		let row = TestRowBuilder::new(RowNumber(1))
			.with_shape(s.clone())
			.with_values(vec![
				Value::datetime(window_start),
				Value::duration(window_duration),
				Value::float8(1.5_f64),
			])
			.build();
		let events = vec![ChaosEvent::Insert {
			row_number: RowNumber(1),
			row,
		}];
		let batches = vec![ChaosBatch::new(events)];
		// Key on the DateTime column - this is the path that regressed.
		let table = materialize_batches(&batches, &["window_start".to_string()]);
		assert_eq!(table.len(), 1);
		let stored = table
			.get(&OutputKey::new(vec![Value::datetime(window_start)]))
			.expect("DateTime output key must locate the row; a nulled window_start would miss it");
		assert_eq!(stored.get("window_start"), Some(&Value::datetime(window_start)));
		assert_eq!(stored.get("window_duration"), Some(&Value::duration(window_duration)));
		assert_eq!(stored.get("v"), Some(&Value::float8(1.5_f64)));
	}

	#[test]
	fn date_and_time_columns_survive_materialization() {
		// A Date/Time column must round-trip through row_to_materialized intact.
		// Before Date/Time cases existed they fell through to Value::none_of,
		// nulling emitted columns and breaking output-key comparison.
		let s = RowShape::new(vec![
			RowShapeField::unconstrained("window_date", ValueType::Date),
			RowShapeField::unconstrained("window_time", ValueType::Time),
			RowShapeField::unconstrained("v", ValueType::Float8),
		]);
		let window_date = Date::new(2024, 3, 15).unwrap();
		let window_time = Time::new(14, 30, 45, 0).unwrap();
		let row = TestRowBuilder::new(RowNumber(1))
			.with_shape(s.clone())
			.with_values(vec![Value::date(window_date), Value::time(window_time), Value::float8(1.5_f64)])
			.build();
		let events = vec![ChaosEvent::Insert {
			row_number: RowNumber(1),
			row,
		}];
		let batches = vec![ChaosBatch::new(events)];
		let table = materialize_batches(&batches, &["window_date".to_string()]);
		assert_eq!(table.len(), 1);
		let stored = table
			.get(&OutputKey::new(vec![Value::date(window_date)]))
			.expect("Date output key must locate the row; a nulled window_date would miss it");
		assert_eq!(stored.get("window_date"), Some(&Value::date(window_date)));
		assert_eq!(stored.get("window_time"), Some(&Value::time(window_time)));
		assert_eq!(stored.get("v"), Some(&Value::float8(1.5_f64)));
		let _ = s;
	}
}
