// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::{Result, error::Error, value::Value};
use serde::de::Error as _;

use crate::{
	array::{
		canonical::{CanonicalArray, CanonicalStorage},
		fixed::FixedStorage,
	},
	compute::SearchResult,
};

// Binary search over a sorted canonical array. Returns `Found(index)` if the
// needle matches a row exactly, or `NotFound(insertion_point)` otherwise. The
// caller is responsible for ensuring the array is sorted; no validation is
// performed. None values in the array are treated as less than any value
// (matching the RQL convention of sorting None first).
pub fn search_sorted(array: &CanonicalArray, needle: &Value) -> Result<SearchResult> {
	let CanonicalStorage::Fixed(f) = &array.storage else {
		return Err(Error::custom("search_sorted: only FixedArray supported in v1"));
	};

	let result = match (&f.storage, needle) {
		(FixedStorage::I32(v), Value::Int4(n)) => v.binary_search(n),
		(FixedStorage::I64(v), Value::Int8(n)) => v.binary_search(n),
		(FixedStorage::U32(v), Value::Uint4(n)) => v.binary_search(n),
		(FixedStorage::U64(v), Value::Uint8(n)) => v.binary_search(n),
		(FixedStorage::I8(v), Value::Int1(n)) => v.binary_search(n),
		(FixedStorage::I16(v), Value::Int2(n)) => v.binary_search(n),
		(FixedStorage::U8(v), Value::Uint1(n)) => v.binary_search(n),
		(FixedStorage::U16(v), Value::Uint2(n)) => v.binary_search(n),
		_ => return Err(Error::custom("search_sorted: storage/needle type mismatch or unsupported")),
	};

	Ok(match result {
		Ok(i) => SearchResult::Found(i),
		Err(i) => SearchResult::NotFound(i),
	})
}

#[cfg(test)]
mod tests {
	use reifydb_core::value::column::data::ColumnData;

	use super::*;

	#[test]
	fn search_sorted_finds_match() {
		let cd = ColumnData::int4([10i32, 20, 30, 40, 50]);
		let ca = CanonicalArray::from_column_data(&cd).unwrap();
		assert_eq!(search_sorted(&ca, &Value::Int4(30)).unwrap(), SearchResult::Found(2));
	}

	#[test]
	fn search_sorted_reports_insertion_point() {
		let cd = ColumnData::int4([10i32, 20, 30, 40, 50]);
		let ca = CanonicalArray::from_column_data(&cd).unwrap();
		assert_eq!(search_sorted(&ca, &Value::Int4(25)).unwrap(), SearchResult::NotFound(2));
	}
}
