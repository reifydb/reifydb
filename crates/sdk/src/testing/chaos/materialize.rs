// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_abi::flow::diff::DiffType;
use reifydb_core::{
	encoded::shape::SHAPE_HEADER_SIZE, interface::change::Change, row::Row, value::column::columns::Columns,
};
use reifydb_type::value::{Value, r#type::Type};

use super::{
	event::ChaosEvent,
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

pub fn materialize_events(events: &[ChaosEvent], output_key_columns: &[String]) -> MaterializedTable {
	let mut table = MaterializedTable::empty();
	for ev in events {
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
	table
}

fn apply_columns(table: &mut MaterializedTable, columns: &Columns, output_key_columns: &[String], remove: bool) {
	let column_names: Vec<String> = columns.iter().map(|c| c.name().text().to_string()).collect();
	for i in 0..columns.row_count() {
		let values = columns.row(i);
		debug_assert_eq!(values.len(), column_names.len());
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
			Type::Boolean => {
				let b = buf[0] != 0;
				Value::Boolean(b)
			}
			Type::Int1 => Value::int8(buf[0] as i8 as i64),
			Type::Int2 => Value::int8(i16::from_le_bytes([buf[0], buf[1]]) as i64),
			Type::Int4 => Value::int8(i32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as i64),
			Type::Int8 => {
				let mut b = [0u8; 8];
				b.copy_from_slice(&buf[..8]);
				Value::int8(i64::from_le_bytes(b))
			}
			Type::Uint1 => Value::uint8(buf[0] as u64),
			Type::Uint2 => Value::uint8(u16::from_le_bytes([buf[0], buf[1]]) as u64),
			Type::Uint4 => Value::uint8(u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as u64),
			Type::Uint8 => {
				let mut b = [0u8; 8];
				b.copy_from_slice(&buf[..8]);
				Value::uint8(u64::from_le_bytes(b))
			}
			Type::Float4 => {
				let mut b = [0u8; 4];
				b.copy_from_slice(&buf[..4]);
				Value::float4(f32::from_le_bytes(b))
			}
			Type::Float8 => {
				let mut b = [0u8; 8];
				b.copy_from_slice(&buf[..8]);
				Value::float8(f64::from_le_bytes(b))
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
	use reifydb_type::value::{Value, datetime::DateTime, row_number::RowNumber, r#type::Type};

	use super::*;
	use crate::testing::builders::TestRowBuilder;

	fn shape() -> RowShape {
		RowShape::new(vec![
			RowShapeField::unconstrained("k", Type::Uint8),
			RowShapeField::unconstrained("v", Type::Float8),
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
			RowShapeField::unconstrained("base", Type::Uint8),
			RowShapeField::unconstrained("quote", Type::Uint8),
			RowShapeField::unconstrained("v", Type::Float8),
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
	fn materialize_events_inserts_updates_removes() {
		// Smoke test the events-side fold against a constructed log.
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
		let table = materialize_events(&events, &["k".to_string()]);
		assert_eq!(table.len(), 1);
		let row = table.get(&OutputKey::new(vec![Value::uint8(7u64)])).unwrap();
		assert_eq!(row.get("v"), Some(&Value::float8(2.5_f64)));

		// Now remove it.
		let events_with_remove = {
			let mut e = events;
			e.push(ChaosEvent::Remove {
				row_number: RowNumber(1),
				row: row2,
			});
			e
		};
		let table = materialize_events(&events_with_remove, &["k".to_string()]);
		assert!(table.is_empty());

		// Suppress unused-shape warning in this test.
		let _ = s;
	}
}
