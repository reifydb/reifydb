// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{Array, ArrayRef};
use arrow_schema::FieldRef;
use reifydb_core::value::column::builder::ColumnBuilder;
use reifydb_store_column::{
	compress::Compressor,
	persist::{deserialize_block, serialize_block},
	predicate::{ColRef, Predicate, evaluate},
	selection::Selection,
	session::new_session,
	snapshot::ColumnBlock,
	stats::{block_stats, has_ordering},
};
use reifydb_value::value::{
	Value,
	blob::Blob,
	column_view::ColumnView,
	constraint::{precision::Precision, scale::Scale},
	date::Date,
	datetime::DateTime,
	decimal::Decimal,
	dictionary::DictionaryEntryId,
	digest::Digest,
	duration::Duration,
	identity::IdentityId,
	time::Time,
	uuid::{Uuid4, Uuid7},
	value_type::ValueType,
};
use uuid::Uuid;

type Column = (FieldRef, ArrayRef);

const OPS: [&str; 6] = ["eq", "ne", "lt", "lt_eq", "gt", "gt_eq"];

fn column_of(ty: &ValueType, values: &[Value]) -> Column {
	let mut builder = ColumnBuilder::with_capacity(ty.clone(), values.len() + 1);
	for value in values {
		builder.push_value(value.clone());
	}
	builder.push_none();
	builder.finish("c")
}

fn uuid(first: u8, last: u8, version: u128) -> Uuid {
	Uuid::from_u128(((first as u128) << 120) | (version << 76) | (0x2 << 62) | last as u128)
}

fn ids(version: u128) -> [Uuid; 4] {
	[uuid(0x10, 0, version), uuid(0x80, 0, version), uuid(0xff, 0, version), uuid(0x10, 1, version)]
}

fn digest_type() -> ValueType {
	ValueType::Digest {
		inner: Box::new(ValueType::Float8),
		accuracy: 10_000,
	}
}

fn digest(values: &[f64]) -> Value {
	let mut digest = Digest::new(ValueType::Float8, 10_000).unwrap();
	for value in values {
		digest.add_value(&Value::float8(*value)).unwrap();
	}
	Value::Digest(Box::new(digest))
}

fn decimal(text: &str) -> Value {
	Value::Decimal(text.parse::<Decimal>().unwrap())
}

fn fixtures() -> Vec<(ValueType, Vec<Value>)> {
	vec![
		(ValueType::Boolean, vec![Value::Boolean(false), Value::Boolean(true)]),
		(ValueType::Int1, [i8::MIN, -1, 0, 1, i8::MAX].map(Value::Int1).to_vec()),
		(ValueType::Int2, [i16::MIN, -1, 0, 1, i16::MAX].map(Value::Int2).to_vec()),
		(ValueType::Int4, [i32::MIN, -1, 0, 1, i32::MAX].map(Value::Int4).to_vec()),
		(ValueType::Int8, [i64::MIN, -1, 0, 1, i64::MAX].map(Value::Int8).to_vec()),
		(ValueType::Int16, [i128::MIN, -128, -1, 0, 5, i128::MAX].map(Value::Int16).to_vec()),
		(ValueType::Uint1, [0, 1, u8::MAX].map(Value::Uint1).to_vec()),
		(ValueType::Uint2, [0, 1, u16::MAX].map(Value::Uint2).to_vec()),
		(ValueType::Uint4, [0, 1, u32::MAX].map(Value::Uint4).to_vec()),
		(ValueType::Uint8, [0, 1, u64::MAX].map(Value::Uint8).to_vec()),
		(ValueType::Uint16, [0, 1, 255, 256, u128::MAX].map(Value::Uint16).to_vec()),
		(ValueType::Float4, [f32::NEG_INFINITY, -1.5, 0.0, 1.5, f32::INFINITY].map(Value::float4).to_vec()),
		(ValueType::Float8, [f64::NEG_INFINITY, -1.5, 0.0, 1.5, f64::INFINITY].map(Value::float8).to_vec()),
		(
			ValueType::Decimal {
				precision: Precision::MAX,
				scale: Scale::new(2),
			},
			vec![decimal("-1.25"), decimal("0"), decimal("3.5"), decimal("3.50"), decimal("12345.67")],
		),
		(
			ValueType::Date,
			vec![
				Value::Date(Date::from_ymd(1, 1, 1).unwrap()),
				Value::Date(Date::from_ymd(1900, 2, 28).unwrap()),
				Value::Date(Date::from_ymd(1970, 1, 1).unwrap()),
				Value::Date(Date::from_ymd(9999, 12, 31).unwrap()),
			],
		),
		(
			ValueType::DateTime,
			[DateTime::MIN, DateTime::from_nanos(-1), DateTime::EPOCH, DateTime::MAX]
				.map(Value::DateTime)
				.to_vec(),
		),
		(
			ValueType::Time,
			vec![
				Value::Time(Time::from_hms_nano(0, 0, 0, 0).unwrap()),
				Value::Time(Time::from_hms_nano(12, 0, 0, 0).unwrap()),
				Value::Time(Time::from_hms_nano(23, 59, 59, 999_999_999).unwrap()),
			],
		),
		(
			ValueType::Duration,
			[(1, 0, 0), (0, 31, 0), (0, 0, 5), (-1, 0, 0), (0, 0, -5)]
				.map(|(months, days, nanos)| {
					Value::Duration(Duration::new(months, days, nanos).unwrap())
				})
				.to_vec(),
		),
		(ValueType::Utf8, ["", "a", "ab", "b", "\u{e9}"].map(|s| Value::Utf8(s.to_string())).to_vec()),
		(
			ValueType::Blob,
			[vec![], vec![0x00], vec![0x7f], vec![0x80], vec![0xff]]
				.map(|b| Value::Blob(Blob::new(b)))
				.to_vec(),
		),
		(ValueType::Uuid4, ids(4).map(|id| Value::Uuid4(Uuid4(id))).to_vec()),
		(ValueType::Uuid7, ids(7).map(|id| Value::Uuid7(Uuid7(id))).to_vec()),
		(ValueType::IdentityId, ids(7).map(|id| Value::IdentityId(IdentityId(Uuid7(id)))).to_vec()),
		(
			ValueType::DictionaryId,
			vec![
				DictionaryEntryId::U1(1),
				DictionaryEntryId::U2(300),
				DictionaryEntryId::U4(70_000),
				DictionaryEntryId::U8(1 << 40),
				DictionaryEntryId::U16(1 << 100),
				DictionaryEntryId::U4(1),
			]
			.into_iter()
			.map(Value::DictionaryId)
			.collect(),
		),
		(digest_type(), vec![digest(&[1.0]), digest(&[2.0, 3.0]), digest(&[-4.0]), digest(&[])]),
	]
}

fn reloaded(ty: &ValueType, column: &Column) -> Result<ColumnBlock, String> {
	let chunks =
		Compressor::new(new_session()).compress(ty.clone(), column).map_err(|e| format!("compress: {e}"))?;
	let block = ColumnBlock::new(Arc::new(vec![("c".to_string(), ty.clone(), true)]), vec![chunks]);
	let bytes = serialize_block(&block, &new_session()).map_err(|e| format!("persist: {e}"))?;
	deserialize_block(&bytes, &new_session()).map_err(|e| format!("load: {e}"))
}

fn predicate(op: &str, value: &Value) -> Predicate {
	let col = ColRef::from("c");
	match op {
		"eq" => Predicate::Eq(col, value.clone()),
		"ne" => Predicate::Ne(col, value.clone()),
		"lt" => Predicate::Lt(col, value.clone()),
		"lt_eq" => Predicate::LtEq(col, value.clone()),
		"gt" => Predicate::Gt(col, value.clone()),
		_ => Predicate::GtEq(col, value.clone()),
	}
}

fn holds(op: &str, row: &Value, value: &Value) -> bool {
	match op {
		"eq" => row == value,
		"ne" => row != value,
		"lt" => row < value,
		"lt_eq" => row <= value,
		"gt" => row > value,
		_ => row >= value,
	}
}

fn selected(selection: Selection, rows: usize) -> Vec<bool> {
	match selection {
		Selection::All => vec![true; rows],
		Selection::None_ => vec![false; rows],
		Selection::Mask(bits) => bits.iter().collect(),
	}
}

fn has_no_vortex_bounds(ty: &ValueType) -> bool {
	matches!(
		ty,
		ValueType::Int16
			| ValueType::Uint16
			| ValueType::Uuid4
			| ValueType::Uuid7
			| ValueType::IdentityId
			| ValueType::DictionaryId
			| ValueType::Duration
	)
}

fn check(ty: &ValueType, values: &[Value]) -> Vec<String> {
	let column = column_of(ty, values);
	let view = ColumnView::try_from(&column).unwrap();
	let rows: Vec<Option<Value>> =
		(0..column.1.len()).map(|i| view.is_defined(i).then(|| view.get_value(i))).collect();
	let block = match reloaded(ty, &column) {
		Ok(block) => block,
		Err(err) => return vec![format!("{ty}: {err}")],
	};
	let session = new_session();
	let mut failures = Vec::new();
	for value in rows.iter().flatten() {
		for op in OPS {
			let got = match evaluate(&block, &predicate(op, value), &session) {
				Ok(selection) => selected(selection, rows.len()),
				Err(err) => {
					failures.push(format!("{ty} {op} {value}: evaluate failed: {err}"));
					continue;
				}
			};
			for (row, cell) in rows.iter().enumerate() {
				let want = cell.as_ref().is_some_and(|cell| holds(op, cell, value));
				if got[row] != want {
					failures.push(format!(
						"{ty} {op} {value}: row {row} ({cell:?}) selected {}, expected {want}",
						got[row]
					));
				}
			}
		}
	}
	let stats = match block_stats(&block, &session) {
		Ok(mut stats) => stats.remove(0),
		Err(err) => return [failures, vec![format!("{ty}: block_stats failed: {err}")]].concat(),
	};
	let defined: Vec<&Value> = rows.iter().flatten().collect();
	let (min, max) = match has_no_vortex_bounds(ty) {
		true => (None, None),
		false => (defined.iter().min().map(|v| (*v).clone()), defined.iter().max().map(|v| (*v).clone())),
	};
	if stats.min != min || stats.max != max {
		failures.push(format!("{ty}: bounds {:?}..{:?}, expected {min:?}..{max:?}", stats.min, stats.max));
	}
	if stats.none_count != 1 {
		failures.push(format!("{ty}: none_count {}, expected 1", stats.none_count));
	}
	failures
}

#[test]
fn every_ordered_type_compares_and_bounds_like_value() {
	// A type Vortex orders differently from Value would return wrong rows or prune live blocks without an error.
	let fixtures = fixtures();
	let mut failures = Vec::new();
	for (ty, values) in &fixtures {
		assert!(has_ordering(ty), "{ty} has no ordering, so it does not belong in this test");
		failures.extend(check(ty, values));
	}
	assert!(failures.is_empty(), "{} ordering mismatches:\n{}", failures.len(), failures.join("\n"));
}

#[test]
fn the_fixtures_cover_every_ordered_type() {
	// A new ordered type without a fixture would skip the differential check silently.
	let covered: Vec<ValueType> = fixtures().into_iter().map(|(ty, _)| ty).collect();
	let ordered = [
		ValueType::Boolean,
		ValueType::Float4,
		ValueType::Float8,
		ValueType::Int1,
		ValueType::Int2,
		ValueType::Int4,
		ValueType::Int8,
		ValueType::Int16,
		ValueType::Utf8,
		ValueType::Uint1,
		ValueType::Uint2,
		ValueType::Uint4,
		ValueType::Uint8,
		ValueType::Uint16,
		ValueType::Date,
		ValueType::DateTime,
		ValueType::Time,
		ValueType::Duration,
		ValueType::IdentityId,
		ValueType::Uuid4,
		ValueType::Uuid7,
		ValueType::Blob,
		ValueType::DictionaryId,
	];
	for ty in ordered {
		assert!(covered.contains(&ty), "{ty} has an ordering but no fixture");
	}
	assert!(covered.iter().any(|ty| matches!(ty, ValueType::Decimal { .. })), "Decimal has no fixture");
	assert!(covered.iter().any(|ty| matches!(ty, ValueType::Digest { .. })), "Digest has no fixture");
}

#[test]
fn a_column_of_only_the_minimum_datetime_compresses_bounds_and_filters() {
	// A Vortex timestamp scalar at i64::MIN nanos panics, so a constant MIN column must never become one.
	let values = vec![Value::DateTime(DateTime::MIN); 200];
	let column = column_of(&ValueType::DateTime, &values);
	let block = reloaded(&ValueType::DateTime, &column).unwrap();
	let session = new_session();
	let stats = block_stats(&block, &session).unwrap().remove(0);
	assert_eq!(stats.min, Some(Value::DateTime(DateTime::MIN)));
	assert_eq!(stats.max, Some(Value::DateTime(DateTime::MIN)));
	assert_eq!(stats.none_count, 1);
	let rows = selected(evaluate(&block, &predicate("eq", &values[0]), &session).unwrap(), column.1.len());
	assert_eq!(rows, [vec![true; 200], vec![false]].concat());
}
