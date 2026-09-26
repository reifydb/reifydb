// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Write as _;

use arrow_buffer::BooleanBuffer;
use postcard::{from_bytes, to_stdvec};
use reifydb_core::value::column::{
	buffer::ColumnBuffer,
	builder::ColumnBuilder,
	cast::{cast_column_data, convert::TargetConvert},
};
use reifydb_value::{
	fragment::Fragment,
	value::{
		Value,
		blob::Blob,
		constraint::{precision::Precision, scale::Scale},
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		dictionary::DictionaryEntryId,
		digest::Digest,
		duration::Duration,
		frame::data::FrameColumnData,
		time::Time,
		uuid::Uuid4,
		value_type::ValueType,
	},
};
use serde::Serialize;
use uuid::Uuid;

const ZERO_NONE: [bool; 4] = [true, true, true, true];
const WITH_NONE: [bool; 4] = [true, false, true, true];

struct Kind {
	ty: ValueType,
	pick: fn(&[usize]) -> ColumnBuffer,
}

struct Case {
	ty: ValueType,
	actual: ColumnBuffer,
	expected: ColumnBuffer,
	bits: Vec<bool>,
}

fn hex(bytes: &[u8]) -> String {
	let mut out = String::with_capacity(bytes.len() * 2);
	for byte in bytes {
		write!(out, "{byte:02x}").unwrap();
	}
	out
}

fn postcard_hex<T: Serialize>(value: &T) -> String {
	hex(&to_stdvec(value).unwrap())
}

fn json<T: Serialize>(value: &T) -> String {
	serde_json::to_string(value).unwrap()
}

fn nullable(bare: ColumnBuffer, defined: &[bool]) -> ColumnBuffer {
	ColumnBuffer::from(FrameColumnData::Option {
		inner: Box::new(FrameColumnData::from(bare)),
		bitvec: BooleanBuffer::from(defined.to_vec()),
	})
}

fn mapped(kind: &Kind, defined: &[bool], rows: &[usize]) -> ColumnBuffer {
	let bits: Vec<bool> = rows.iter().map(|&row| defined[row]).collect();
	nullable((kind.pick)(rows), &bits)
}

fn source(kind: &Kind, defined: &[bool]) -> ColumnBuffer {
	mapped(kind, defined, &[0, 1, 2, 3])
}

fn case(kind: &Kind, defined: &[bool], actual: ColumnBuffer, rows: &[usize]) -> Case {
	Case {
		ty: kind.ty.clone(),
		actual,
		expected: mapped(kind, defined, rows),
		bits: rows.iter().map(|&row| defined[row]).collect(),
	}
}

fn built(kind: &Kind, defined: &[bool]) -> ColumnBuffer {
	let mut builder = ColumnBuilder::with_capacity(ValueType::Option(Box::new(kind.ty.clone())), defined.len());
	for (row, &is_defined) in defined.iter().enumerate() {
		if is_defined {
			builder.push_value((kind.pick)(&[row]).get_value(0));
		} else {
			builder.push_none();
		}
	}
	builder.finish()
}

fn check(case: Case) {
	let nullable_type = ValueType::Option(Box::new(case.ty.clone()));
	assert_eq!(case.actual.get_type(), nullable_type, "the column must stay nullable");
	let defined: Vec<bool> = (0..case.actual.len()).map(|row| case.actual.is_defined(row)).collect();
	assert_eq!(defined, case.bits, "rows that report as defined");
	for (row, _) in case.bits.iter().enumerate().filter(|(_, is_defined)| !**is_defined) {
		assert_eq!(case.actual.get_value(row), Value::none_of(case.ty.clone()), "row {row} must read as none");
	}
	assert_eq!(
		postcard_hex(&case.actual),
		postcard_hex(&case.expected),
		"bytes must match, placeholders included\nactual:   {}\nexpected: {}",
		json(&case.actual),
		json(&case.expected)
	);
}

fn slice(kind: &Kind, defined: &[bool]) -> Case {
	case(kind, defined, source(kind, defined).slice(1, 3), &[1, 2])
}

fn take(kind: &Kind, defined: &[bool]) -> Case {
	case(kind, defined, source(kind, defined).take(2), &[0, 1])
}

fn filter(kind: &Kind, defined: &[bool]) -> Case {
	let mut column = source(kind, defined);
	column.filter(&BooleanBuffer::from(vec![true, true, false, true])).unwrap();
	case(kind, defined, column, &[0, 1, 3])
}

fn reorder(kind: &Kind, defined: &[bool]) -> Case {
	let mut column = source(kind, defined);
	column.reorder(&[2, 1, 3, 0]);
	case(kind, defined, column, &[2, 1, 3, 0])
}

fn gather(kind: &Kind, defined: &[bool]) -> Case {
	case(kind, defined, source(kind, defined).gather(&[3, 1, 1, 0]), &[3, 1, 1, 0])
}

fn extend(kind: &Kind, defined: &[bool]) -> Case {
	let mut column = source(kind, defined);
	column.extend(source(kind, defined)).unwrap();
	case(kind, defined, column, &[0, 1, 2, 3, 0, 1, 2, 3])
}

fn scatter(kind: &Kind, defined: &[bool]) -> Case {
	let rotated = mapped(kind, defined, &[2, 3, 0, 1]);
	let then_mask = BooleanBuffer::from(vec![true, true, false, false]);
	let else_mask = BooleanBuffer::from(vec![false, false, true, true]);
	let merged = source(kind, defined).scatter_merge(&rotated, &then_mask, &else_mask, 4);
	case(kind, defined, merged, &[0, 1, 0, 1])
}

fn cast(kind: &Kind, defined: &[bool]) -> Case {
	let actual = cast_column_data(
		TargetConvert {
			target: None,
		},
		&source(kind, defined),
		ValueType::Option(Box::new(kind.ty.clone())),
		|| Fragment::internal("cast"),
	)
	.unwrap();
	let expected = if defined.iter().all(|&is_defined| is_defined) {
		source(kind, defined)
	} else {
		built(kind, defined)
	};
	Case {
		ty: kind.ty.clone(),
		actual,
		expected,
		bits: defined.to_vec(),
	}
}

fn builder_round_trip(kind: &Kind, defined: &[bool]) -> Case {
	let mut builder = source(kind, defined).into_builder();
	builder.push_value((kind.pick)(&[0]).get_value(0));
	case(kind, defined, builder.finish(), &[0, 1, 2, 3, 0])
}

fn postcard_round_trip(kind: &Kind, defined: &[bool]) -> Case {
	let bytes = to_stdvec(&source(kind, defined)).unwrap();
	case(kind, defined, from_bytes(&bytes).unwrap(), &[0, 1, 2, 3])
}

fn json_round_trip(kind: &Kind, defined: &[bool]) -> Case {
	let text = json(&source(kind, defined));
	case(kind, defined, serde_json::from_str(&text).unwrap(), &[0, 1, 2, 3])
}

fn frame_round_trip(kind: &Kind, defined: &[bool]) -> Case {
	case(kind, defined, ColumnBuffer::from(FrameColumnData::from(source(kind, defined))), &[0, 1, 2, 3])
}

fn uuid4_at(row: usize) -> Uuid4 {
	Uuid4(Uuid::from_u128(((row as u128 + 1) << 80) | (0x4 << 76) | (0x2 << 62) | 0xabcd))
}

fn digest_type() -> ValueType {
	ValueType::Digest {
		inner: Box::new(ValueType::Float8),
		accuracy: 10_000,
	}
}

fn digests(rows: &[usize]) -> ColumnBuffer {
	let samples: [&[f64]; 4] = [&[1.0], &[99.0, 98.0], &[3.0], &[4.0, 5.0]];
	let mut builder = ColumnBuilder::with_capacity(digest_type(), rows.len());
	for &row in rows {
		let mut digest = Digest::new(ValueType::Float8, 10_000).unwrap();
		for &sample in samples[row] {
			digest.add_value(&Value::float8(sample)).unwrap();
		}
		builder.push_value(Value::Digest(Box::new(digest)));
	}
	builder.finish()
}

macro_rules! cells {
	($($op:ident),* $(,)?) => {
		$(
			mod $op {
				use super::kind;

				#[test]
				fn keeps_a_zero_none_column_nullable() {
					// Zero nones must never drop nullability, otherwise type and bytes change.
					crate::check(crate::$op(&kind(), &crate::ZERO_NONE));
				}

				#[test]
				fn keeps_the_none_row_and_its_placeholder() {
					// A none row must stay none over exactly its stored value, otherwise bytes drift.
					crate::check(crate::$op(&kind(), &crate::WITH_NONE));
				}
			}
		)*
	};
}

macro_rules! matrix {
	($($kind:ident => $ty:expr, $pick:expr;)*) => {
		$(
			mod $kind {
				use super::*;

				fn kind() -> Kind {
					Kind {
						ty: $ty,
						pick: $pick,
					}
				}

				cells!(
					slice,
					take,
					filter,
					reorder,
					gather,
					extend,
					scatter,
					cast,
					builder_round_trip,
					postcard_round_trip,
					json_round_trip,
					frame_round_trip,
				);
			}
		)*
	};
}

matrix! {
	int4 => ValueType::Int4, |rows| ColumnBuffer::int4(rows.iter().map(|&row| [10, 99, 30, 40][row]));
	float8 => ValueType::Float8, |rows| ColumnBuffer::float8(rows.iter().map(|&row| [1.5, 99.25, -3.0, 4.0][row]));
	int16 => ValueType::Int16, |rows| ColumnBuffer::int16(rows.iter().map(|&row| [1, 99, -3, i128::MAX][row]));
	uint16 => ValueType::Uint16, |rows| ColumnBuffer::uint16(rows.iter().map(|&row| [1, 99, 3, u128::MAX][row]));
	boolean => ValueType::Boolean, |rows| ColumnBuffer::bool(rows.iter().map(|&row| [false, true, false, true][row]));
	utf8 => ValueType::Utf8, |rows| ColumnBuffer::utf8(rows.iter().map(|&row| ["a", "placeholder", "", "dd"][row]));
	blob => ValueType::Blob, |rows| {
		ColumnBuffer::blob(rows.iter().map(|&row| Blob::new([&[1u8][..], &[9, 9], &[], &[3, 4]][row].to_vec())))
	};
	uuid4 => ValueType::Uuid4, |rows| ColumnBuffer::uuid4(rows.iter().map(|&row| uuid4_at(row)));
	date => ValueType::Date, |rows| {
		ColumnBuffer::date(rows.iter().map(|&row| {
			[(2026, 9, 22), (1999, 12, 31), (1970, 1, 2), (2000, 2, 29)]
				.map(|(y, m, d)| Date::from_ymd(y, m, d).unwrap())[row]
		}))
	};
	datetime => ValueType::DateTime, |rows| {
		ColumnBuffer::datetime(rows.iter().map(|&row| {
			DateTime::from_nanos([1_758_500_000_123_456_789, 99, 3_000, 4_000_000][row])
		}))
	};
	time => ValueType::Time, |rows| {
		ColumnBuffer::time(rows.iter().map(|&row| {
			[(1, 2, 3, 4), (23, 59, 59, 999_999_999), (0, 0, 1, 0), (12, 0, 0, 5)]
				.map(|(h, m, s, n)| Time::from_hms_nano(h, m, s, n).unwrap())[row]
		}))
	};
	duration => ValueType::Duration, |rows| {
		ColumnBuffer::duration(rows.iter().map(|&row| {
			[(1, 2, 3), (99, 9, 9), (-1, 0, 0), (0, 4, 4_000)].map(|(m, d, n)| Duration::new(m, d, n).unwrap())
				[row]
		}))
	};
	dictionary_id => ValueType::DictionaryId, |rows| {
		ColumnBuffer::dictionary_id(rows.iter().map(|&row| DictionaryEntryId::U4([1, 99, 3, 4][row])))
	};
	decimal => ValueType::decimal(Precision::new(10), Scale::new(2)), |rows| {
		ColumnBuffer::decimal(
			Precision::new(10),
			Scale::new(2),
			rows.iter().map(|&row| ["1.5", "99", "-3.25", "4"][row].parse::<Decimal>().unwrap()),
		)
	};
	any => ValueType::Any, |rows| {
		ColumnBuffer::any(rows.iter().map(|&row| {
			[Value::Int4(1), Value::Utf8("placeholder".to_string()), Value::Boolean(true), Value::Int8(4)][row]
				.clone()
		}))
	};
	digest => digest_type(), digests;
}
