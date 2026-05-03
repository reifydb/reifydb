// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{array::canonical::Canonical, buffer::ColumnBuffer};
use reifydb_type::{Result, value::Value};

use crate::{compute::SearchResult, error::ColumnError};

pub fn search_sorted(array: &Canonical, needle: &Value) -> Result<SearchResult> {
	let result = match (&array.buffer, needle) {
		(ColumnBuffer::Int1(_), Value::Int1(n)) => array.buffer.as_slice::<i8>().binary_search(n),
		(ColumnBuffer::Int2(_), Value::Int2(n)) => array.buffer.as_slice::<i16>().binary_search(n),
		(ColumnBuffer::Int4(_), Value::Int4(n)) => array.buffer.as_slice::<i32>().binary_search(n),
		(ColumnBuffer::Int8(_), Value::Int8(n)) => array.buffer.as_slice::<i64>().binary_search(n),
		(ColumnBuffer::Uint1(_), Value::Uint1(n)) => array.buffer.as_slice::<u8>().binary_search(n),
		(ColumnBuffer::Uint2(_), Value::Uint2(n)) => array.buffer.as_slice::<u16>().binary_search(n),
		(ColumnBuffer::Uint4(_), Value::Uint4(n)) => array.buffer.as_slice::<u32>().binary_search(n),
		(ColumnBuffer::Uint8(_), Value::Uint8(n)) => array.buffer.as_slice::<u64>().binary_search(n),
		_ => return Err(ColumnError::SearchSortedTypeMismatch.into()),
	};

	Ok(match result {
		Ok(i) => SearchResult::Found(i),
		Err(i) => SearchResult::NotFound(i),
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn search_sorted_finds_match() {
		let cd = ColumnBuffer::int4([10i32, 20, 30, 40, 50]);
		let ca = Canonical::from_column_buffer(&cd).unwrap();
		assert_eq!(search_sorted(&ca, &Value::Int4(30)).unwrap(), SearchResult::Found(2));
	}

	#[test]
	fn search_sorted_reports_insertion_point() {
		let cd = ColumnBuffer::int4([10i32, 20, 30, 40, 50]);
		let ca = Canonical::from_column_buffer(&cd).unwrap();
		assert_eq!(search_sorted(&ca, &Value::Int4(25)).unwrap(), SearchResult::NotFound(2));
	}
}
