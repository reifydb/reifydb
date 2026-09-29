// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashSet;

use arrow_buffer::i256;
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	container::{decimal_array::DecimalView, temporal_array::times, wide_int_array::wides},
};

use crate::frame::{format::Encoding, options::CompressionLevel};

const MIN_ROWS: usize = 4;

pub fn choose_encoding(data: &ColumnView<'_>, compression: CompressionLevel) -> Encoding {
	if compression == CompressionLevel::None {
		return Encoding::Plain;
	}

	if data.len() < MIN_ROWS {
		return Encoding::Plain;
	}

	match &data.data {
		ViewData::Utf8 {
			..
		}
		| ViewData::Blob {
			..
		} => try_dict_heuristic(data),

		ViewData::Decimal(c) => match c {
			DecimalView::Decimal128(array) => try_numeric_heuristic_i128(array.values()),
			DecimalView::Decimal256(array) => try_numeric_heuristic_i256(array.values()),
		},

		ViewData::Int1(c) => {
			try_numeric_heuristic_i64(&c.values().iter().map(|&v| v as i64).collect::<Vec<_>>())
		}
		ViewData::Int2(c) => {
			try_numeric_heuristic_i64(&c.values().iter().map(|&v| v as i64).collect::<Vec<_>>())
		}
		ViewData::Int4(c) => try_numeric_heuristic_i32(c.values()),
		ViewData::Int8(c) => try_numeric_heuristic_i64(c.values()),
		ViewData::Int16(c) => try_numeric_heuristic_i128(&wides::<i128>(c)),
		ViewData::Uint1(c) => {
			try_numeric_heuristic_i64(&c.values().iter().map(|&v| v as i64).collect::<Vec<_>>())
		}
		ViewData::Uint2(c) => {
			try_numeric_heuristic_i64(&c.values().iter().map(|&v| v as i64).collect::<Vec<_>>())
		}
		ViewData::Uint4(c) => {
			try_numeric_heuristic_i64(&c.values().iter().map(|&v| v as i64).collect::<Vec<_>>())
		}
		ViewData::Uint8(c) => try_numeric_heuristic_u64(c.values()),
		ViewData::Uint16(c) => try_numeric_heuristic_u128(&wides::<u128>(c)),
		ViewData::Float4(c) => {
			try_numeric_heuristic_i64(&c.values().iter().map(|v| v.to_bits() as i64).collect::<Vec<_>>())
		}
		ViewData::Float8(c) => {
			try_numeric_heuristic_i64(&c.values().iter().map(|v| v.to_bits() as i64).collect::<Vec<_>>())
		}

		ViewData::Date(c) => try_numeric_heuristic_i32(c.values()),
		ViewData::DateTime(c) => try_numeric_heuristic_i64(c.values()),
		ViewData::Time(c) => {
			let raw: Vec<u64> = times(c).iter().map(|t| t.to_nanos_since_midnight()).collect();
			try_numeric_heuristic_u64(&raw)
		}

		_ => Encoding::Plain,
	}
}

fn try_dict_heuristic(data: &ColumnView<'_>) -> Encoding {
	let len = data.len();
	if len == 0 {
		return Encoding::Plain;
	}

	let rows: Vec<&[u8]> = match &data.data {
		ViewData::Utf8 {
			container,
			..
		} => (0..len).map(|i| container.value(i).as_bytes()).collect(),
		ViewData::Blob {
			container,
			..
		} => (0..len).map(|i| container.value(i)).collect(),
		_ => return Encoding::Plain,
	};

	let budget = (len / 2).min(10_000);
	let mut seen = HashSet::new();

	for row in rows {
		seen.insert(row);
		if seen.len() > budget {
			return Encoding::Plain;
		}
	}

	if seen.len() < len / 2 {
		Encoding::Dict
	} else {
		Encoding::Plain
	}
}

fn try_numeric_heuristic_i32(slice: &[i32]) -> Encoding {
	if slice.len() < MIN_ROWS {
		return Encoding::Plain;
	}

	let run_count = count_runs_generic(slice);
	if run_count * 2 < slice.len() {
		return Encoding::Rle;
	}

	let as_i64: Vec<i64> = slice.iter().map(|&v| v as i64).collect();

	if is_monotonic_i64(&as_i64) {
		if has_constant_stride_i64(&as_i64) {
			return Encoding::DeltaRle;
		}
		return Encoding::Delta;
	}

	Encoding::Plain
}

fn try_numeric_heuristic_i64(slice: &[i64]) -> Encoding {
	if slice.len() < MIN_ROWS {
		return Encoding::Plain;
	}

	let run_count = count_runs_generic(slice);
	if run_count * 2 < slice.len() {
		return Encoding::Rle;
	}

	if is_monotonic_i64(slice) {
		if has_constant_stride_i64(slice) {
			return Encoding::DeltaRle;
		}
		return Encoding::Delta;
	}

	Encoding::Plain
}

fn try_numeric_heuristic_u64(slice: &[u64]) -> Encoding {
	if slice.len() < MIN_ROWS {
		return Encoding::Plain;
	}

	let run_count = count_runs_generic(slice);
	if run_count * 2 < slice.len() {
		return Encoding::Rle;
	}

	let is_asc = slice.windows(2).all(|w| w[0] <= w[1]);
	let is_desc = !is_asc && slice.windows(2).all(|w| w[0] >= w[1]);

	if is_asc || is_desc {
		let as_i64: Vec<i64> = slice.iter().map(|&v| v as i64).collect();
		if has_constant_stride_i64(&as_i64) {
			return Encoding::DeltaRle;
		}
		return Encoding::Delta;
	}

	Encoding::Plain
}

fn try_numeric_heuristic_i128(slice: &[i128]) -> Encoding {
	if slice.len() < MIN_ROWS {
		return Encoding::Plain;
	}

	let run_count = count_runs_generic(slice);
	if run_count * 2 < slice.len() {
		return Encoding::Rle;
	}

	let is_asc = slice.windows(2).all(|w| w[0] <= w[1]);
	let is_desc = !is_asc && slice.windows(2).all(|w| w[0] >= w[1]);

	if is_asc || is_desc {
		if has_constant_stride_i128(slice) {
			return Encoding::DeltaRle;
		}
		return Encoding::Delta;
	}

	Encoding::Plain
}

fn try_numeric_heuristic_u128(slice: &[u128]) -> Encoding {
	if slice.len() < MIN_ROWS {
		return Encoding::Plain;
	}

	let run_count = count_runs_generic(slice);
	if run_count * 2 < slice.len() {
		return Encoding::Rle;
	}

	let is_asc = slice.windows(2).all(|w| w[0] <= w[1]);
	let is_desc = !is_asc && slice.windows(2).all(|w| w[0] >= w[1]);

	if is_asc || is_desc {
		if has_constant_stride_u128(slice) {
			return Encoding::DeltaRle;
		}
		return Encoding::Delta;
	}

	Encoding::Plain
}

fn try_numeric_heuristic_i256(slice: &[i256]) -> Encoding {
	if slice.len() < MIN_ROWS {
		return Encoding::Plain;
	}

	let run_count = count_runs_generic(slice);
	if run_count * 2 < slice.len() {
		return Encoding::Rle;
	}

	let is_asc = slice.windows(2).all(|w| w[0] <= w[1]);
	let is_desc = !is_asc && slice.windows(2).all(|w| w[0] >= w[1]);

	if is_asc || is_desc {
		if has_constant_stride_i256(slice) {
			return Encoding::DeltaRle;
		}
		return Encoding::Delta;
	}

	Encoding::Plain
}

fn is_monotonic_i64(slice: &[i64]) -> bool {
	let is_asc = slice.windows(2).all(|w| w[0] <= w[1]);
	if is_asc {
		return true;
	}
	slice.windows(2).all(|w| w[0] >= w[1])
}

fn has_constant_stride_i64(slice: &[i64]) -> bool {
	if slice.len() < 3 {
		return true;
	}
	let stride = slice[1].wrapping_sub(slice[0]);
	slice.windows(2).all(|w| w[1].wrapping_sub(w[0]) == stride)
}

fn has_constant_stride_i128(slice: &[i128]) -> bool {
	if slice.len() < 3 {
		return true;
	}
	let stride = slice[1].wrapping_sub(slice[0]);
	slice.windows(2).all(|w| w[1].wrapping_sub(w[0]) == stride)
}

fn has_constant_stride_u128(slice: &[u128]) -> bool {
	if slice.len() < 3 {
		return true;
	}
	let stride = slice[1].wrapping_sub(slice[0]);
	slice.windows(2).all(|w| w[1].wrapping_sub(w[0]) == stride)
}

fn has_constant_stride_i256(slice: &[i256]) -> bool {
	if slice.len() < 3 {
		return true;
	}
	let stride = slice[1].wrapping_sub(slice[0]);
	slice.windows(2).all(|w| w[1].wrapping_sub(w[0]) == stride)
}

fn count_runs_generic<T: PartialEq>(slice: &[T]) -> usize {
	if slice.is_empty() {
		return 0;
	}
	let mut runs = 1;
	for i in 1..slice.len() {
		if slice[i] != slice[i - 1] {
			runs += 1;
		}
	}
	runs
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use arrow_array::{ArrayRef, BooleanArray, Int32Array, LargeStringArray};
	use arrow_schema::FieldRef;
	use reifydb_value::value::{
		constraint::{precision::Precision, scale::Scale},
		container::{
			decimal_array::decimal_array,
			temporal_array::{date_array, datetime_array, time_array},
		},
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		time::Time,
		value_type::{
			ValueType,
			field::{FieldType, named},
		},
	};

	use super::*;

	fn column(value_type: ValueType, array: ArrayRef) -> (FieldRef, ArrayRef) {
		named("c", FieldType::from(value_type), array)
	}

	fn view(column: &(FieldRef, ArrayRef)) -> ColumnView<'_> {
		ColumnView::try_from(column).unwrap()
	}

	fn utf8(values: Vec<String>) -> (FieldRef, ArrayRef) {
		column(ValueType::Utf8, Arc::new(LargeStringArray::from(values)))
	}

	#[test]
	fn count_runs_generic_counts_value_transitions_not_elements() {
		assert_eq!(count_runs_generic::<i32>(&[]), 0);
		assert_eq!(count_runs_generic(&[7, 7, 7, 7]), 1);
		assert_eq!(count_runs_generic(&[1, 2, 3, 4]), 4);
		assert_eq!(count_runs_generic(&[1, 1, 2, 2, 2, 3]), 3);
	}

	#[test]
	fn is_monotonic_i64_accepts_ascending_descending_and_constant() {
		assert!(is_monotonic_i64(&[1, 2, 3]));
		assert!(is_monotonic_i64(&[3, 2, 1]));
		assert!(is_monotonic_i64(&[5, 5, 5]));
		assert!(!is_monotonic_i64(&[1, 3, 2]));
	}

	#[test]
	fn has_constant_stride_i64_is_trivially_true_below_three_elements() {
		assert!(has_constant_stride_i64(&[]));
		assert!(has_constant_stride_i64(&[1]));
		assert!(has_constant_stride_i64(&[1, 100]));
	}

	#[test]
	fn has_constant_stride_i64_detects_uniform_and_irregular_strides() {
		assert!(has_constant_stride_i64(&[10, 20, 30, 40]));
		assert!(!has_constant_stride_i64(&[10, 20, 35, 40]));
	}

	#[test]
	fn has_constant_stride_i64_wraps_instead_of_panicking_at_the_range_boundary() {
		let slice = [i64::MIN, i64::MAX, i64::MIN];
		assert!(!has_constant_stride_i64(&slice));
	}

	#[test]
	fn has_constant_stride_i128_and_u128_mirror_the_i64_behavior() {
		assert!(has_constant_stride_i128(&[10i128, 20, 30]));
		assert!(!has_constant_stride_i128(&[10i128, 20, 35]));
		assert!(has_constant_stride_u128(&[10u128, 20, 30]));
		assert!(!has_constant_stride_u128(&[10u128, 20, 35]));
	}

	#[test]
	fn try_dict_heuristic_picks_dict_only_below_the_half_cardinality_threshold() {
		let repeated = utf8((0..100).map(|i| format!("v{}", i % 5)).collect());
		assert_eq!(try_dict_heuristic(&view(&repeated)), Encoding::Dict);

		let unique = utf8((0..100).map(|i| format!("v{i}")).collect());
		assert_eq!(try_dict_heuristic(&view(&unique)), Encoding::Plain);

		let half = utf8((0..100).map(|i| format!("v{}", i % 50)).collect());
		assert_eq!(try_dict_heuristic(&view(&half)), Encoding::Plain);

		let empty = utf8(Vec::new());
		assert_eq!(try_dict_heuristic(&view(&empty)), Encoding::Plain);
	}

	#[test]
	fn try_numeric_heuristic_i32_prefers_rle_when_runs_dominate() {
		let slice: Vec<i32> = (0..20).flat_map(|i| [i; 10]).collect();
		assert_eq!(try_numeric_heuristic_i32(&slice), Encoding::Rle);
	}

	#[test]
	fn try_numeric_heuristic_i32_picks_delta_rle_for_constant_stride_and_delta_otherwise() {
		let constant_stride: Vec<i32> = (0..100).map(|i| i * 3).collect();
		assert_eq!(try_numeric_heuristic_i32(&constant_stride), Encoding::DeltaRle);

		let irregular_stride: Vec<i32> = (0..100).map(|i| i * i).collect();
		assert_eq!(try_numeric_heuristic_i32(&irregular_stride), Encoding::Delta);
	}

	#[test]
	fn try_numeric_heuristic_i32_falls_back_to_plain_below_min_rows_or_when_scattered() {
		assert_eq!(try_numeric_heuristic_i32(&[1, 2, 3]), Encoding::Plain);

		let scattered = [1, 5, 2, 8, 3, 9, 0, 7];
		assert_eq!(try_numeric_heuristic_i32(&scattered), Encoding::Plain);
	}

	#[test]
	fn try_numeric_heuristic_u64_detects_descending_stride_via_its_own_is_desc_check() {
		let descending: Vec<u64> = (0..100).rev().map(|i| i * 3).collect();
		assert_eq!(try_numeric_heuristic_u64(&descending), Encoding::DeltaRle);
	}

	#[test]
	fn try_numeric_heuristic_i128_and_u128_pick_delta_rle_for_constant_stride() {
		let asc_i128: Vec<i128> = (0..100).map(|i| i * 5).collect();
		assert_eq!(try_numeric_heuristic_i128(&asc_i128), Encoding::DeltaRle);

		let asc_u128: Vec<u128> = (0..100).map(|i| i * 5).collect();
		assert_eq!(try_numeric_heuristic_u128(&asc_u128), Encoding::DeltaRle);

		let scattered_i128: Vec<i128> = vec![5, 1, 9, 2, 8, 3, 7, 4];
		assert_eq!(try_numeric_heuristic_i128(&scattered_i128), Encoding::Plain);
	}

	#[test]
	fn choose_encoding_ignores_the_heuristic_entirely_when_compression_is_off() {
		let data = column(ValueType::Int4, Arc::new(Int32Array::from_iter_values((0..100).map(|i| i * 3))));
		assert_eq!(choose_encoding(&view(&data), CompressionLevel::None), Encoding::Plain);
	}

	#[test]
	fn choose_encoding_stays_plain_below_min_rows_even_with_compression_on() {
		let data = column(ValueType::Int4, Arc::new(Int32Array::from(vec![1, 2, 3])));
		assert_eq!(choose_encoding(&view(&data), CompressionLevel::Fast), Encoding::Plain);
	}

	#[test]
	fn choose_encoding_peels_the_option_wrapper_before_applying_the_heuristic() {
		let wrapped = column(
			ValueType::Option(Box::new(ValueType::Utf8)),
			Arc::new(LargeStringArray::from(
				(0..100).map(|i| format!("v{}", i % 5)).collect::<Vec<String>>(),
			)),
		);
		assert_eq!(choose_encoding(&view(&wrapped), CompressionLevel::Fast), Encoding::Dict);
	}

	#[test]
	fn choose_encoding_falls_back_to_plain_for_types_with_no_dedicated_heuristic() {
		let data =
			column(ValueType::Boolean, Arc::new(BooleanArray::from(vec![true, false, true, false, true])));
		assert_eq!(choose_encoding(&view(&data), CompressionLevel::Fast), Encoding::Plain);
	}

	#[test]
	fn choose_encoding_dispatches_temporal_columns_through_their_raw_representation() {
		let dates = column(
			ValueType::Date,
			Arc::new(date_array((0..100).map(|i| Date::from_days_since_epoch(i * 2).unwrap()))),
		);
		assert_eq!(choose_encoding(&view(&dates), CompressionLevel::Fast), Encoding::DeltaRle);

		let datetimes = column(
			ValueType::DateTime,
			Arc::new(datetime_array((0..100).map(|i| DateTime::from_nanos(i as i64 * 1_000)))),
		);
		assert_eq!(choose_encoding(&view(&datetimes), CompressionLevel::Fast), Encoding::DeltaRle);

		let times = column(
			ValueType::Time,
			Arc::new(time_array(
				(0..100).map(|i| Time::from_nanos_since_midnight(i as u64 * 1_000).unwrap()),
			)),
		);
		assert_eq!(choose_encoding(&view(&times), CompressionLevel::Fast), Encoding::DeltaRle);
	}

	#[test]
	fn choose_encoding_dispatches_fixed_width_family_columns_by_storage_width() {
		// Int, uint and decimal ride the fixed int16 heuristics at 16 bytes and the i256 twin at 32 bytes.
		let narrow = Precision::new(38);
		let wide = Precision::new(76);

		let runny: Vec<Decimal> = (0..100i64).map(|i| Decimal::from_i64(i / 10)).collect();
		let narrow_column = column(
			ValueType::decimal(narrow, Scale::new(2)),
			decimal_array(narrow, Scale::new(2), &runny).into_array(),
		);
		assert_eq!(choose_encoding(&view(&narrow_column), CompressionLevel::Fast), Encoding::Rle);
		let wide_column = column(
			ValueType::decimal(wide, Scale::new(2)),
			decimal_array(wide, Scale::new(2), &runny).into_array(),
		);
		assert_eq!(choose_encoding(&view(&wide_column), CompressionLevel::Fast), Encoding::Rle);
	}
}
