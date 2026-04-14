// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Encoding selection heuristics.
//!
//! Chooses the optimal encoding for each column based on data characteristics.
//! Single O(n) pass per column with early termination.

use std::collections::HashSet;

use reifydb_type::value::frame::data::FrameColumnData;

use crate::{format::Encoding, options::CompressionLevel};

/// Minimum row count before considering compressed encodings.
const MIN_ROWS: usize = 4;

/// Choose the best encoding for a column based on its data and compression level.
pub fn choose_encoding(data: &FrameColumnData, compression: CompressionLevel) -> Encoding {
	if compression == CompressionLevel::None {
		return Encoding::Plain;
	}

	// Unwrap Option to get inner data for heuristics
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
		// Utf8/Blob: try dictionary encoding
		FrameColumnData::Utf8(_) | FrameColumnData::Blob(_) => try_dict_heuristic(inner),

		// Variable-length numeric: try RLE first, then dictionary
		FrameColumnData::Int(c) => try_varlen_numeric_heuristic(c, inner),
		FrameColumnData::Uint(c) => try_varlen_numeric_heuristic(c, inner),
		FrameColumnData::Decimal(c) => try_varlen_numeric_heuristic(c, inner),

		// Fixed-width types with delta/delta_rle/rle support
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

		// Temporal types backed by i32/u64
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

		// Everything else: plain
		_ => Encoding::Plain,
	}
}

fn try_dict_heuristic(data: &FrameColumnData) -> Encoding {
	let len = data.len();
	if len == 0 {
		return Encoding::Plain;
	}

	// Quick cardinality estimate: count distinct values up to a budget
	let budget = (len / 2).min(10_000);
	let mut seen = HashSet::new();

	for i in 0..len {
		let s = data.as_string(i);
		seen.insert(s);
		if seen.len() > budget {
			return Encoding::Plain;
		}
	}

	// Dict is beneficial if distinct < row_count / 2
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

	// Check RLE first — high repetition benefits more from RLE than delta
	let run_count = count_runs_generic(slice);
	if run_count * 2 < slice.len() {
		return Encoding::Rle;
	}

	let as_i64: Vec<i64> = slice.iter().map(|&v| v as i64).collect();

	// Check if monotonic (ascending or descending) for delta encoding
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

	// Check RLE first — high repetition benefits more from RLE than delta
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

	// Check RLE first — high repetition benefits more from RLE than delta
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

/// Heuristic for variable-length numeric types: RLE first, then Dict.
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

/// Check if a slice is monotonically sorted (ascending or descending).
fn is_monotonic_i64(slice: &[i64]) -> bool {
	let is_asc = slice.windows(2).all(|w| w[0] <= w[1]);
	if is_asc {
		return true;
	}
	slice.windows(2).all(|w| w[0] >= w[1])
}

fn has_constant_stride_i64(slice: &[i64]) -> bool {
	if slice.len() < 3 {
		return true; // trivially constant stride
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
