// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashSet;

use reifydb_value::value::frame::data::FrameColumnData;

use crate::frame::{format::Encoding, options::CompressionLevel};

const MIN_ROWS: usize = 4;

pub fn choose_encoding(data: &FrameColumnData, compression: CompressionLevel) -> Encoding {
	if compression == CompressionLevel::None {
		return Encoding::Plain;
	}

	let inner = match data {
		FrameColumnData::Option {
			inner,
			..
		} => inner.as_ref(),
		other => other,
	};

	if inner.len() < MIN_ROWS {
		return Encoding::Plain;
	}

	match inner {
		FrameColumnData::Utf8(_) | FrameColumnData::Blob(_) => try_dict_heuristic(inner),

		FrameColumnData::Int(c) => try_varlen_numeric_heuristic(c, inner),
		FrameColumnData::Uint(c) => try_varlen_numeric_heuristic(c, inner),
		FrameColumnData::Decimal(c) => try_varlen_numeric_heuristic(c, inner),

		FrameColumnData::Int1(c) => {
			try_numeric_heuristic_i64(&c.iter().map(|v| v.unwrap() as i64).collect::<Vec<_>>())
		}
		FrameColumnData::Int2(c) => {
			try_numeric_heuristic_i64(&c.iter().map(|v| v.unwrap() as i64).collect::<Vec<_>>())
		}
		FrameColumnData::Int4(c) => try_numeric_heuristic_i32(c),
		FrameColumnData::Int8(c) => try_numeric_heuristic_i64(c),
		FrameColumnData::Int16(c) => try_numeric_heuristic_i128(c),
		FrameColumnData::Uint1(c) => {
			try_numeric_heuristic_i64(&c.iter().map(|v| v.unwrap() as i64).collect::<Vec<_>>())
		}
		FrameColumnData::Uint2(c) => {
			try_numeric_heuristic_i64(&c.iter().map(|v| v.unwrap() as i64).collect::<Vec<_>>())
		}
		FrameColumnData::Uint4(c) => {
			try_numeric_heuristic_i64(&c.iter().map(|v| v.unwrap() as i64).collect::<Vec<_>>())
		}
		FrameColumnData::Uint8(c) => try_numeric_heuristic_u64(c),
		FrameColumnData::Uint16(c) => try_numeric_heuristic_u128(c),
		FrameColumnData::Float4(c) => {
			try_numeric_heuristic_i64(&c.iter().map(|v| v.unwrap().to_bits() as i64).collect::<Vec<_>>())
		}
		FrameColumnData::Float8(c) => {
			try_numeric_heuristic_i64(&c.iter().map(|v| v.unwrap().to_bits() as i64).collect::<Vec<_>>())
		}

		FrameColumnData::Date(c) => {
			let raw: Vec<i32> = (**c).iter().map(|d| d.to_days_since_epoch()).collect();
			try_numeric_heuristic_i32(&raw)
		}
		FrameColumnData::DateTime(c) => {
			let raw: Vec<u64> = (**c).iter().map(|d| d.to_nanos()).collect();
			try_numeric_heuristic_u64(&raw)
		}
		FrameColumnData::Time(c) => {
			let raw: Vec<u64> = (**c).iter().map(|t| t.to_nanos_since_midnight()).collect();
			try_numeric_heuristic_u64(&raw)
		}

		_ => Encoding::Plain,
	}
}

fn try_dict_heuristic(data: &FrameColumnData) -> Encoding {
	let len = data.len();
	if len == 0 {
		return Encoding::Plain;
	}

	let budget = (len / 2).min(10_000);
	let mut seen = HashSet::new();

	for i in 0..len {
		let s = data.as_string(i);
		seen.insert(s);
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

fn try_varlen_numeric_heuristic<T: PartialEq>(slice: &[T], data: &FrameColumnData) -> Encoding {
	if slice.len() < MIN_ROWS {
		return Encoding::Plain;
	}

	let run_count = count_runs_generic(slice);
	if run_count * 2 < slice.len() {
		return Encoding::Rle;
	}

	try_dict_heuristic(data)
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
	use num_bigint::BigInt;
	use reifydb_value::value::{
		container::{
			bool::BoolContainer, number::NumberContainer, temporal::TemporalContainer, utf8::Utf8Container,
		},
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		int::Int,
		time::Time,
		uint::Uint,
	};

	use super::*;

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
		let repeated =
			FrameColumnData::Utf8(Utf8Container::new((0..100).map(|i| format!("v{}", i % 5)).collect()));
		assert_eq!(try_dict_heuristic(&repeated), Encoding::Dict);

		let unique = FrameColumnData::Utf8(Utf8Container::new((0..100).map(|i| format!("v{i}")).collect()));
		assert_eq!(try_dict_heuristic(&unique), Encoding::Plain);

		let half =
			FrameColumnData::Utf8(Utf8Container::new((0..100).map(|i| format!("v{}", i % 50)).collect()));
		assert_eq!(try_dict_heuristic(&half), Encoding::Plain);

		let empty = FrameColumnData::Utf8(Utf8Container::new(vec![]));
		assert_eq!(try_dict_heuristic(&empty), Encoding::Plain);
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
	fn try_varlen_numeric_heuristic_checks_runs_before_falling_back_to_dict() {
		let runny: Vec<Int> = (0..20).flat_map(|i| vec![Int(BigInt::from(i)); 10]).collect();
		let container = NumberContainer::new(runny);
		let data = FrameColumnData::Int(container.clone());
		assert_eq!(try_varlen_numeric_heuristic::<Int>(&container, &data), Encoding::Rle);

		let low_cardinality: Vec<Int> = (0..100).map(|i| Int(BigInt::from(i % 5))).collect();
		let container = NumberContainer::new(low_cardinality);
		let data = FrameColumnData::Int(container.clone());
		assert_eq!(try_varlen_numeric_heuristic::<Int>(&container, &data), Encoding::Dict);
	}

	#[test]
	fn choose_encoding_ignores_the_heuristic_entirely_when_compression_is_off() {
		let data = FrameColumnData::Int4(NumberContainer::new((0..100).map(|i| i * 3).collect()));
		assert_eq!(choose_encoding(&data, CompressionLevel::None), Encoding::Plain);
	}

	#[test]
	fn choose_encoding_stays_plain_below_min_rows_even_with_compression_on() {
		let data = FrameColumnData::Int4(NumberContainer::new(vec![1, 2, 3]));
		assert_eq!(choose_encoding(&data, CompressionLevel::Fast), Encoding::Plain);
	}

	#[test]
	fn choose_encoding_peels_the_option_wrapper_before_applying_the_heuristic() {
		let inner =
			FrameColumnData::Utf8(Utf8Container::new((0..100).map(|i| format!("v{}", i % 5)).collect()));
		let wrapped = FrameColumnData::Option {
			inner: Box::new(inner),
			bitvec: reifydb_value::util::bitvec::BitVec::from_slice(&vec![true; 100]),
		};
		assert_eq!(choose_encoding(&wrapped, CompressionLevel::Fast), Encoding::Dict);
	}

	#[test]
	fn choose_encoding_falls_back_to_plain_for_types_with_no_dedicated_heuristic() {
		let data = FrameColumnData::Bool(BoolContainer::new(vec![true, false, true, false, true]));
		assert_eq!(choose_encoding(&data, CompressionLevel::Fast), Encoding::Plain);
	}

	#[test]
	fn choose_encoding_dispatches_temporal_columns_through_their_raw_representation() {
		let dates = FrameColumnData::Date(TemporalContainer::new(
			(0..100).map(|i| Date::from_days_since_epoch(i * 2).unwrap()).collect(),
		));
		assert_eq!(choose_encoding(&dates, CompressionLevel::Fast), Encoding::DeltaRle);

		let datetimes = FrameColumnData::DateTime(TemporalContainer::new(
			(0..100).map(|i| DateTime::from_nanos(i as u64 * 1_000)).collect(),
		));
		assert_eq!(choose_encoding(&datetimes, CompressionLevel::Fast), Encoding::DeltaRle);

		let times = FrameColumnData::Time(TemporalContainer::new(
			(0..100).map(|i| Time::from_nanos_since_midnight(i as u64 * 1_000).unwrap()).collect(),
		));
		assert_eq!(choose_encoding(&times, CompressionLevel::Fast), Encoding::DeltaRle);
	}

	#[test]
	fn choose_encoding_dispatches_arbitrary_precision_columns_through_the_varlen_path() {
		let low_cardinality: Vec<Uint> = (0..100).map(|i| Uint(BigInt::from(i % 5))).collect();
		let uints = FrameColumnData::Uint(NumberContainer::new(low_cardinality));
		assert_eq!(choose_encoding(&uints, CompressionLevel::Fast), Encoding::Dict);

		let low_cardinality: Vec<Decimal> =
			(0..100).map(|i| Decimal::new(format!("{}.00", i % 5).parse().unwrap())).collect();
		let decimals = FrameColumnData::Decimal(NumberContainer::new(low_cardinality));
		assert_eq!(choose_encoding(&decimals, CompressionLevel::Fast), Encoding::Dict);
	}
}
