// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cmp::Ordering, sync::Arc};

use arrow_array::UInt32Array;
use arrow_ord::sort::{SortColumn, SortOptions, lexsort_to_indices};
use reifydb_core::{
	internal_error,
	sort::{
		SortDirection,
		SortDirection::{Asc, Desc},
	},
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	container::{
		decimal_array::decimal_at,
		dictionary_array,
		temporal_array::{dates, datetimes, durations, times},
		uuid_array::{identity_ids, uuid4s, uuid7s},
		wide_int_array::wide_at,
	},
	ordered_f32::OrderedF32,
	ordered_f64::OrderedF64,
};

use crate::Result;

pub(crate) fn rank_rows(
	keys: &[(ColumnView<'_>, SortDirection)],
	row_count: usize,
	limit: Option<usize>,
) -> Result<Vec<usize>> {
	match sort_columns(keys, row_count) {
		Some(columns) => {
			let indices = lexsort_to_indices(&columns, limit)
				.map_err(|e| internal_error!("Failed to rank sort keys: {}", e))?;
			Ok(indices.values().iter().map(|&index| index as usize).collect())
		}
		None => Ok(compare_rank(keys, row_count, limit)),
	}
}

fn sort_columns(keys: &[(ColumnView<'_>, SortDirection)], row_count: usize) -> Option<Vec<SortColumn>> {
	let row_count = u32::try_from(row_count).ok()?;
	if keys.iter().any(|(data, _)| ranks_by_value(data)) {
		return None;
	}
	let mut columns: Vec<SortColumn> = keys
		.iter()
		.map(|(data, direction)| SortColumn {
			values: data.array().slice(0, data.len()),
			options: Some(options(direction)),
		})
		.collect();
	columns.push(SortColumn {
		values: Arc::new(UInt32Array::from_iter_values(0..row_count)),
		options: Some(options(&Asc)),
	});
	Some(columns)
}

fn options(direction: &SortDirection) -> SortOptions {
	let descending = matches!(direction, Desc);
	SortOptions {
		descending,
		nulls_first: descending,
	}
}

fn ranks_by_value(data: &ColumnView<'_>) -> bool {
	matches!(data.data, ViewData::DictionaryId { .. })
}

type Comparator<'a> = Box<dyn Fn(usize, usize) -> Ordering + 'a>;

fn compare_rank(keys: &[(ColumnView<'_>, SortDirection)], row_count: usize, limit: Option<usize>) -> Vec<usize> {
	let comparators: Vec<(Comparator<'_>, &SortDirection)> =
		keys.iter().map(|(data, direction)| (comparator(data), direction)).collect();
	let mut indices: Vec<usize> = (0..row_count).collect();
	indices.sort_by(|&left, &right| compare_rows(&comparators, left, right));
	if let Some(limit) = limit {
		indices.truncate(limit);
	}
	indices
}

fn compare_rows(comparators: &[(Comparator<'_>, &SortDirection)], left: usize, right: usize) -> Ordering {
	for (compare, direction) in comparators {
		let ordering = compare(left, right);
		let ordering = match direction {
			Asc => ordering,
			Desc => ordering.reverse(),
		};
		if ordering != Ordering::Equal {
			return ordering;
		}
	}
	Ordering::Equal
}

fn comparator<'a>(data: &'a ColumnView<'a>) -> Comparator<'a> {
	match &data.data {
		ViewData::Bool(a) => by(data, move |i| Some(a.value(i))),
		ViewData::Float4(a) => by(data, move |i| OrderedF32::try_from(a.value(i)).ok()),
		ViewData::Float8(a) => by(data, move |i| OrderedF64::try_from(a.value(i)).ok()),
		ViewData::Int1(a) => by(data, move |i| Some(a.value(i))),
		ViewData::Int2(a) => by(data, move |i| Some(a.value(i))),
		ViewData::Int4(a) => by(data, move |i| Some(a.value(i))),
		ViewData::Int8(a) => by(data, move |i| Some(a.value(i))),
		ViewData::Int16(a) => by(data, move |i| wide_at::<i128>(a, i)),
		ViewData::Uint1(a) => by(data, move |i| Some(a.value(i))),
		ViewData::Uint2(a) => by(data, move |i| Some(a.value(i))),
		ViewData::Uint4(a) => by(data, move |i| Some(a.value(i))),
		ViewData::Uint8(a) => by(data, move |i| Some(a.value(i))),
		ViewData::Uint16(a) => by(data, move |i| wide_at::<u128>(a, i)),
		ViewData::Utf8 {
			container,
			..
		} => by(data, move |i| Some(container.value(i))),
		ViewData::Blob {
			container,
			..
		} => by(data, move |i| Some(container.value(i))),
		ViewData::Date(a) => {
			let values = dates(a);
			by(data, move |i| values.get(i))
		}
		ViewData::DateTime(a) => {
			let values = datetimes(a);
			by(data, move |i| values.get(i))
		}
		ViewData::Time(a) => {
			let values = times(a);
			by(data, move |i| values.get(i))
		}
		ViewData::Duration(a) => {
			let values = durations(a);
			by(data, move |i| values.get(i))
		}
		ViewData::IdentityId(a) => {
			let values = identity_ids(a);
			by(data, move |i| values.get(i))
		}
		ViewData::Uuid4(a) => {
			let values = uuid4s(a);
			by(data, move |i| values.get(i))
		}
		ViewData::Uuid7(a) => {
			let values = uuid7s(a);
			by(data, move |i| values.get(i))
		}
		ViewData::Decimal(d) => by(data, move |i| decimal_at(d, i)),
		ViewData::DictionaryId {
			container,
			..
		} => by(data, move |i| dictionary_array::get(container, i).map(|id| id.to_u128())),
		ViewData::Any {
			..
		}
		| ViewData::Digest {
			..
		}
		| ViewData::None {
			..
		} => Box::new(move |left, right| data.get_value(left).cmp(&data.get_value(right))),
	}
}

fn by<'a, T: Ord>(data: &'a ColumnView<'a>, get: impl Fn(usize) -> Option<T> + 'a) -> Comparator<'a> {
	let present = move |i: usize| {
		if data.none_at(i) {
			None
		} else {
			get(i)
		}
	};
	Box::new(move |left, right| match (present(left), present(right)) {
		(None, None) => Ordering::Equal,
		(None, Some(_)) => Ordering::Greater,
		(Some(_), None) => Ordering::Less,
		(Some(left), Some(right)) => left.cmp(&right),
	})
}

#[cfg(test)]
mod tests {
	use std::{str::FromStr, sync::Arc};

	use arrow_array::{Array, ArrayRef, Float32Array, Float64Array};
	use arrow_schema::FieldRef;
	use reifydb_core::{
		sort::SortDirection,
		value::column::{builder::ColumnBuilder, factory},
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
			duration::Duration,
			time::Time,
			uuid::parse::{parse_identity_id, parse_uuid4, parse_uuid7},
			value_type::ValueType,
		},
	};

	use super::{compare_rank, rank_rows};

	fn view(column: &(FieldRef, ArrayRef)) -> ColumnView<'_> {
		ColumnView::try_from(column).expect("a factory column must form a view")
	}

	fn ranked(data: (FieldRef, ArrayRef), direction: SortDirection) -> Vec<usize> {
		let row_count = data.1.len();
		rank_rows(&[(view(&data), direction)], row_count, None).expect("ranking must succeed")
	}

	#[test]
	fn a_dictionary_id_key_ranks_by_its_number_not_by_its_stored_bytes() {
		// Entry ids store a width tag then little-endian bytes, so a byte order would sort 5 above 256.
		let data = factory::dictionary_id(
			"k",
			[DictionaryEntryId::U2(256), DictionaryEntryId::U1(5), DictionaryEntryId::U8(1)],
		);

		assert_eq!(ranked(data, SortDirection::Asc), vec![2, 1, 0]);
	}

	#[test]
	fn a_decimal_key_ranks_numerically_so_equal_values_of_different_scale_tie() {
		// The column stores every value at its own scale, so 1.0 and 1.00 must tie and keep input order.
		let decimal = |text: &str| Decimal::from_str(text).expect("a decimal literal");
		let data = factory::decimal(
			"k",
			Precision::new(10),
			Scale::new(2),
			[decimal("1.00"), decimal("0.5"), decimal("1.0")],
		);

		assert_eq!(ranked(data, SortDirection::Asc), vec![1, 0, 2]);
	}

	#[test]
	fn rows_tied_on_every_key_keep_their_input_position() {
		// Tie order is observable through take, so ranking must be a total order ending in the row index.
		let data = factory::int4("k", [7, 7, 7, 7]);

		assert_eq!(ranked(data.clone(), SortDirection::Asc), vec![0, 1, 2, 3]);
		assert_eq!(ranked(data, SortDirection::Desc), vec![0, 1, 2, 3]);
	}

	#[test]
	fn a_limit_keeps_the_earliest_rows_among_ties() {
		// A later tied row must never displace an earlier one that is already held.
		let data = factory::int4("k", [5, 5, 5, 5, 5]);
		let row_count = data.1.len();

		let indices = rank_rows(&[(view(&data), SortDirection::Asc)], row_count, Some(3)).expect("ranking");

		assert_eq!(indices, vec![0, 1, 2]);
	}

	fn column_of(ty: ValueType, values: Vec<Value>) -> (FieldRef, ArrayRef) {
		let mut builder = ColumnBuilder::with_capacity(ValueType::Option(Box::new(ty)), values.len());
		for value in values {
			builder.push_value(value);
		}
		builder.finish("k")
	}

	fn raw_floats(ty: ValueType) -> (FieldRef, ArrayRef) {
		let cells = [Some(1.5), Some(f64::NAN), Some(-0.0), None, Some(0.0), Some(-2.0), Some(f64::NAN)];
		let (field, _) = ColumnBuilder::with_capacity(ValueType::Option(Box::new(ty.clone())), 0).finish("k");
		let array: ArrayRef = match ty {
			ValueType::Float4 => {
				Arc::new(Float32Array::from_iter(cells.map(|cell| cell.map(|v| v as f32))))
			}
			_ => Arc::new(Float64Array::from_iter(cells)),
		};
		(field, array)
	}

	fn value_order(data: &(FieldRef, ArrayRef), direction: &SortDirection) -> Vec<usize> {
		let view = view(data);
		let mut indices: Vec<usize> = (0..view.len()).collect();
		indices.sort_by(|&left, &right| {
			let ordering = view.get_value(left).cmp(&view.get_value(right));
			match direction {
				SortDirection::Asc => ordering,
				SortDirection::Desc => ordering.reverse(),
			}
		});
		indices
	}

	#[test]
	fn typed_ranking_matches_value_ordering_for_every_type() {
		// the typed comparators replace Value::cmp, so every type must rank rows exactly as Value::cmp does.
		let none = Value::none_of;
		let uuid4 = |text: &str| Value::Uuid4(parse_uuid4(Fragment::testing(text)).unwrap());
		let uuid7 = |text: &str| Value::Uuid7(parse_uuid7(Fragment::testing(text)).unwrap());
		let identity = |text: &str| Value::IdentityId(parse_identity_id(Fragment::testing(text)).unwrap());
		let decimal = |text: &str| Value::Decimal(Decimal::from_str(text).unwrap());
		let columns = vec![
			column_of(
				ValueType::Boolean,
				vec![
					Value::Boolean(true),
					none(ValueType::Boolean),
					Value::Boolean(false),
					Value::Boolean(true),
				],
			),
			raw_floats(ValueType::Float4),
			raw_floats(ValueType::Float8),
			column_of(
				ValueType::Int1,
				vec![
					Value::Int1(3),
					Value::Int1(-1),
					none(ValueType::Int1),
					Value::Int1(3),
					Value::Int1(0),
				],
			),
			column_of(
				ValueType::Int2,
				vec![
					Value::Int2(3),
					Value::Int2(-1),
					none(ValueType::Int2),
					Value::Int2(3),
					Value::Int2(0),
				],
			),
			column_of(
				ValueType::Int4,
				vec![
					Value::Int4(3),
					Value::Int4(-1),
					none(ValueType::Int4),
					Value::Int4(3),
					Value::Int4(0),
				],
			),
			column_of(
				ValueType::Int8,
				vec![
					Value::Int8(3),
					Value::Int8(-1),
					none(ValueType::Int8),
					Value::Int8(3),
					Value::Int8(0),
				],
			),
			column_of(
				ValueType::Int16,
				vec![
					Value::Int16(3),
					Value::Int16(-1),
					none(ValueType::Int16),
					Value::Int16(3),
					Value::Int16(0),
				],
			),
			column_of(
				ValueType::Uint1,
				vec![
					Value::Uint1(3),
					Value::Uint1(1),
					none(ValueType::Uint1),
					Value::Uint1(3),
					Value::Uint1(0),
				],
			),
			column_of(
				ValueType::Uint2,
				vec![
					Value::Uint2(3),
					Value::Uint2(1),
					none(ValueType::Uint2),
					Value::Uint2(3),
					Value::Uint2(0),
				],
			),
			column_of(
				ValueType::Uint4,
				vec![
					Value::Uint4(3),
					Value::Uint4(1),
					none(ValueType::Uint4),
					Value::Uint4(3),
					Value::Uint4(0),
				],
			),
			column_of(
				ValueType::Uint8,
				vec![
					Value::Uint8(3),
					Value::Uint8(1),
					none(ValueType::Uint8),
					Value::Uint8(3),
					Value::Uint8(0),
				],
			),
			column_of(
				ValueType::Uint16,
				vec![
					Value::Uint16(3),
					Value::Uint16(1),
					none(ValueType::Uint16),
					Value::Uint16(3),
					Value::Uint16(0),
				],
			),
			column_of(
				ValueType::Utf8,
				vec![
					Value::Utf8("b".to_string()),
					none(ValueType::Utf8),
					Value::Utf8("a".to_string()),
					Value::Utf8(String::new()),
					Value::Utf8("b".to_string()),
					Value::Utf8("ab".to_string()),
				],
			),
			column_of(
				ValueType::Blob,
				vec![
					Value::Blob(Blob::new(vec![1])),
					Value::Blob(Blob::new(vec![])),
					none(ValueType::Blob),
					Value::Blob(Blob::new(vec![0, 255])),
					Value::Blob(Blob::new(vec![1])),
				],
			),
			column_of(
				ValueType::Date,
				vec![
					Value::Date(Date::new(2024, 1, 2).unwrap()),
					none(ValueType::Date),
					Value::Date(Date::new(1999, 12, 31).unwrap()),
					Value::Date(Date::new(2024, 1, 2).unwrap()),
				],
			),
			column_of(
				ValueType::DateTime,
				vec![
					Value::DateTime(DateTime::from_nanos(5)),
					none(ValueType::DateTime),
					Value::DateTime(DateTime::from_nanos(-5)),
					Value::DateTime(DateTime::from_nanos(5)),
				],
			),
			column_of(
				ValueType::Time,
				vec![
					Value::Time(Time::from_nanos_since_midnight(10).unwrap()),
					none(ValueType::Time),
					Value::Time(Time::from_nanos_since_midnight(5).unwrap()),
				],
			),
			column_of(
				ValueType::Duration,
				vec![
					Value::Duration(Duration::new(0, 1, 0).unwrap()),
					none(ValueType::Duration),
					Value::Duration(Duration::new(1, 0, 0).unwrap()),
					Value::Duration(Duration::new(0, 0, 5).unwrap()),
				],
			),
			column_of(
				ValueType::Uuid4,
				vec![
					uuid4("ffffffff-ffff-4fff-bfff-ffffffffffff"),
					none(ValueType::Uuid4),
					uuid4("550e8400-e29b-41d4-a716-446655440000"),
				],
			),
			column_of(
				ValueType::Uuid7,
				vec![
					uuid7("0199a5d4-0000-7000-8000-000000000000"),
					none(ValueType::Uuid7),
					uuid7("01890a5d-ac96-774b-bcce-b302099a8057"),
				],
			),
			column_of(
				ValueType::IdentityId,
				vec![
					identity("0199a5d4-0000-7000-8000-000000000000"),
					none(ValueType::IdentityId),
					identity("01890a5d-ac96-774b-bcce-b302099a8057"),
				],
			),
			column_of(
				ValueType::decimal(Precision::new(10), Scale::new(2)),
				vec![
					decimal("1.00"),
					none(ValueType::decimal(Precision::new(10), Scale::new(2))),
					decimal("0.5"),
					decimal("-3.25"),
					decimal("1.0"),
				],
			),
			column_of(
				ValueType::DictionaryId,
				vec![
					Value::DictionaryId(DictionaryEntryId::U2(256)),
					Value::DictionaryId(DictionaryEntryId::U1(5)),
					none(ValueType::DictionaryId),
					Value::DictionaryId(DictionaryEntryId::U8(1)),
					Value::DictionaryId(DictionaryEntryId::U1(5)),
					Value::DictionaryId(DictionaryEntryId::U4(70_000)),
				],
			),
		];
		for data in columns {
			for direction in [SortDirection::Asc, SortDirection::Desc] {
				let typed = compare_rank(&[(view(&data), direction.clone())], data.1.len(), None);
				assert_eq!(
					typed,
					value_order(&data, &direction),
					"{} {direction:?}",
					data.1.data_type()
				);
			}
		}
	}
}
