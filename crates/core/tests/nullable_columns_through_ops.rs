// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{ArrayRef, RecordBatch};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use arrow_schema::FieldRef;
use reifydb_core::value::{
	batch::{batch, filter as filter_rows, head, take_rows},
	column::{
		builder::ColumnBuilder,
		cast::{cast_column_data, convert::TargetConvert},
		factory,
		nulls::with_nulls,
		scatter::scatter_merge,
	},
};
use reifydb_value::{
	fragment::Fragment,
	value::{
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
		time::Time,
		uuid::Uuid4,
		value_type::ValueType,
	},
};
use uuid::Uuid;

const ZERO_NONE: [bool; 4] = [true, true, true, true];
const WITH_NONE: [bool; 4] = [true, false, true, true];

type Pair = (FieldRef, ArrayRef);

struct Kind {
	ty: ValueType,
	pick: fn(&[usize]) -> Pair,
}

struct Case {
	ty: ValueType,
	actual: Pair,
	expected: Pair,
	bits: Vec<bool>,
}

fn view(column: &Pair) -> ColumnView<'_> {
	ColumnView::try_from(column).unwrap()
}

fn only(batch: &RecordBatch) -> Pair {
	(batch.schema_ref().fields()[0].clone(), batch.column(0).clone())
}

fn nullable(bare: Pair, defined: &[bool]) -> Pair {
	with_nulls(bare, NullBuffer::new(BooleanBuffer::from(defined.to_vec()))).unwrap()
}

fn mapped(kind: &Kind, defined: &[bool], rows: &[usize]) -> Pair {
	let bits: Vec<bool> = rows.iter().map(|&row| defined[row]).collect();
	nullable((kind.pick)(rows), &bits)
}

fn source(kind: &Kind, defined: &[bool]) -> Pair {
	mapped(kind, defined, &[0, 1, 2, 3])
}

fn source_batch(kind: &Kind, defined: &[bool]) -> RecordBatch {
	batch(vec![source(kind, defined)]).unwrap()
}

fn case(kind: &Kind, defined: &[bool], actual: Pair, rows: &[usize]) -> Case {
	Case {
		ty: kind.ty.clone(),
		actual,
		expected: mapped(kind, defined, rows),
		bits: rows.iter().map(|&row| defined[row]).collect(),
	}
}

fn built(kind: &Kind, defined: &[bool]) -> Pair {
	let mut builder = ColumnBuilder::with_capacity(ValueType::Option(Box::new(kind.ty.clone())), defined.len());
	for (row, &is_defined) in defined.iter().enumerate() {
		if is_defined {
			builder.push_value(view(&(kind.pick)(&[row])).get_value(0));
		} else {
			builder.push_none();
		}
	}
	builder.finish("c")
}

fn check(case: Case) {
	let nullable_type = ValueType::Option(Box::new(case.ty.clone()));
	let actual = view(&case.actual);
	let expected = view(&case.expected);
	assert_eq!(actual.get_type(), nullable_type, "the column must stay nullable");
	assert_eq!(expected.get_type(), nullable_type, "the expected column must be nullable");
	let defined: Vec<bool> = (0..actual.len()).map(|row| actual.is_defined(row)).collect();
	assert_eq!(defined, case.bits, "rows that report as defined");
	for (row, _) in case.bits.iter().enumerate().filter(|(_, is_defined)| !**is_defined) {
		assert_eq!(actual.get_value(row), Value::none_of(case.ty.clone()), "row {row} must read as none");
	}
	assert_eq!(actual.len(), expected.len(), "row count");
	for row in 0..actual.len() {
		assert_eq!(actual.get_value(row), expected.get_value(row), "row {row}");
	}
}

fn slice(kind: &Kind, defined: &[bool]) -> Case {
	case(kind, defined, only(&source_batch(kind, defined).slice(1, 2)), &[1, 2])
}

fn take(kind: &Kind, defined: &[bool]) -> Case {
	case(kind, defined, only(&head(&source_batch(kind, defined), 2)), &[0, 1])
}

fn filter(kind: &Kind, defined: &[bool]) -> Case {
	let filtered =
		filter_rows(&source_batch(kind, defined), &BooleanBuffer::from(vec![true, true, false, true])).unwrap();
	case(kind, defined, only(&filtered), &[0, 1, 3])
}

fn reorder(kind: &Kind, defined: &[bool]) -> Case {
	case(kind, defined, only(&take_rows(&source_batch(kind, defined), &[2, 1, 3, 0]).unwrap()), &[2, 1, 3, 0])
}

fn gather(kind: &Kind, defined: &[bool]) -> Case {
	case(kind, defined, only(&take_rows(&source_batch(kind, defined), &[3, 1, 1, 0]).unwrap()), &[3, 1, 1, 0])
}

fn extend(kind: &Kind, defined: &[bool]) -> Case {
	let column = source(kind, defined);
	let mut builder = ColumnBuilder::from_view(&view(&column));
	builder.extend(&view(&source(kind, defined))).unwrap();
	case(kind, defined, builder.finish("c"), &[0, 1, 2, 3, 0, 1, 2, 3])
}

fn scatter(kind: &Kind, defined: &[bool]) -> Case {
	let rotated = mapped(kind, defined, &[2, 3, 0, 1]);
	let then_mask = BooleanBuffer::from(vec![true, true, false, false]);
	let else_mask = BooleanBuffer::from(vec![false, false, true, true]);
	let source = source(kind, defined);
	let merged = scatter_merge(&view(&source), &view(&rotated), &then_mask, &else_mask, 4, "c").unwrap();
	case(kind, defined, merged, &[0, 1, 0, 1])
}

fn cast(kind: &Kind, defined: &[bool]) -> Case {
	let actual = cast_column_data(
		TargetConvert {
			target: None,
		},
		&view(&source(kind, defined)),
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
	let mut builder = ColumnBuilder::from_view(&view(&source(kind, defined)));
	builder.push_value(view(&(kind.pick)(&[0])).get_value(0));
	case(kind, defined, builder.finish("c"), &[0, 1, 2, 3, 0])
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

fn digests(rows: &[usize]) -> Pair {
	let samples: [&[f64]; 4] = [&[1.0], &[99.0, 98.0], &[3.0], &[4.0, 5.0]];
	let mut builder = ColumnBuilder::with_capacity(digest_type(), rows.len());
	for &row in rows {
		let mut digest = Digest::new(ValueType::Float8, 10_000).unwrap();
		for &sample in samples[row] {
			digest.add_value(&Value::float8(sample)).unwrap();
		}
		builder.push_value(Value::Digest(Box::new(digest)));
	}
	builder.finish("c")
}

macro_rules! cells {
	($($op:ident),* $(,)?) => {
		$(
			mod $op {
				use super::kind;

				#[test]
				fn keeps_a_zero_none_column_nullable() {
					// Zero nones must never drop nullability, otherwise the column type changes.
					crate::check(crate::$op(&kind(), &crate::ZERO_NONE));
				}

				#[test]
				fn keeps_the_none_row_in_place() {
					// A none row must stay none at its own row, never move onto a defined value.
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
				);
			}
		)*
	};
}

matrix! {
	int4 => ValueType::Int4, |rows| factory::int4("c", rows.iter().map(|&row| [10, 99, 30, 40][row]));
	float8 => ValueType::Float8, |rows| factory::float8("c", rows.iter().map(|&row| [1.5, 99.25, -3.0, 4.0][row]));
	int16 => ValueType::Int16, |rows| factory::int16("c", rows.iter().map(|&row| [1, 99, -3, i128::MAX][row]));
	uint16 => ValueType::Uint16, |rows| factory::uint16("c", rows.iter().map(|&row| [1, 99, 3, u128::MAX][row]));
	boolean => ValueType::Boolean, |rows| {
		factory::bool("c", rows.iter().map(|&row| [false, true, false, true][row]))
	};
	utf8 => ValueType::Utf8, |rows| factory::utf8("c", rows.iter().map(|&row| ["a", "placeholder", "", "dd"][row]));
	blob => ValueType::Blob, |rows| {
		factory::blob("c", rows.iter().map(|&row| Blob::new([&[1u8][..], &[9, 9], &[], &[3, 4]][row].to_vec())))
	};
	uuid4 => ValueType::Uuid4, |rows| factory::uuid4("c", rows.iter().map(|&row| uuid4_at(row)));
	date => ValueType::Date, |rows| {
		factory::date("c", rows.iter().map(|&row| {
			[(2026, 9, 22), (1999, 12, 31), (1970, 1, 2), (2000, 2, 29)]
				.map(|(y, m, d)| Date::from_ymd(y, m, d).unwrap())[row]
		}))
	};
	datetime => ValueType::DateTime, |rows| {
		factory::datetime("c", rows.iter().map(|&row| {
			DateTime::from_nanos([1_758_500_000_123_456_789, 99, 3_000, 4_000_000][row])
		}))
	};
	time => ValueType::Time, |rows| {
		factory::time("c", rows.iter().map(|&row| {
			[(1, 2, 3, 4), (23, 59, 59, 999_999_999), (0, 0, 1, 0), (12, 0, 0, 5)]
				.map(|(h, m, s, n)| Time::from_hms_nano(h, m, s, n).unwrap())[row]
		}))
	};
	duration => ValueType::Duration, |rows| {
		factory::duration("c", rows.iter().map(|&row| {
			[(1, 2, 3), (99, 9, 9), (-1, 0, 0), (0, 4, 4_000)].map(|(m, d, n)| Duration::new(m, d, n).unwrap())
				[row]
		}))
	};
	dictionary_id => ValueType::DictionaryId, |rows| {
		factory::dictionary_id("c", rows.iter().map(|&row| DictionaryEntryId::U4([1, 99, 3, 4][row])))
	};
	decimal => ValueType::decimal(Precision::new(10), Scale::new(2)), |rows| {
		factory::decimal(
			"c",
			Precision::new(10),
			Scale::new(2),
			rows.iter().map(|&row| ["1.5", "99", "-3.25", "4"][row].parse::<Decimal>().unwrap()),
		)
	};
	any => ValueType::Any, |rows| {
		factory::any("c", rows.iter().map(|&row| {
			[Value::Int4(1), Value::Utf8("placeholder".to_string()), Value::Boolean(true), Value::Int8(4)][row]
				.clone()
		}))
	};
	digest => digest_type(), digests;
}
