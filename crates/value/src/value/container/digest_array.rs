// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::borrow::Borrow;

use arrow_array::{Array, LargeBinaryArray, builder::LargeBinaryBuilder};

use crate::value::{Value, container::varlen_array, digest::Digest};

fn decode(row: &[u8]) -> Digest {
	Digest::decode(row).unwrap_or_else(|error| panic!("corrupt Digest row {row:02x?}: {error}"))
}

pub fn digest_array<B: Borrow<Digest>>(values: impl IntoIterator<Item = Option<B>>) -> LargeBinaryArray {
	let mut builder = LargeBinaryBuilder::new();
	for value in values {
		match value {
			Some(digest) => push_digest(&mut builder, digest.borrow()),
			None => builder.append_null(),
		}
	}
	builder.finish()
}

pub fn push_digest(builder: &mut LargeBinaryBuilder, digest: &Digest) {
	builder.append_value(digest.encode());
}

pub fn get(array: &LargeBinaryArray, index: usize) -> Option<Digest> {
	varlen_array::get(array, index).filter(|_| array.is_valid(index)).map(decode)
}

pub fn iter(array: &LargeBinaryArray) -> impl Iterator<Item = Option<Digest>> + '_ {
	(0..array.len()).map(move |index| get(array, index))
}

pub fn is_defined(array: &LargeBinaryArray, index: usize) -> bool {
	index < array.len() && array.is_valid(index)
}

pub fn get_value(array: &LargeBinaryArray, index: usize) -> Value {
	match get(array, index) {
		Some(digest) => Value::Digest(Box::new(digest)),
		None => Value::none(),
	}
}

pub fn as_string(array: &LargeBinaryArray, index: usize) -> String {
	get_value(array, index).to_string()
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::value::digest::tests::built;

	#[test]
	fn an_empty_row_is_the_none_slot() {
		// Aggregates skip undefined digests; an empty row read as a digest would crash or count twice.
		let digest = built(10_000, &[1.0, 2.5, -4.0]);
		let array = digest_array([Some(&digest), None]);
		assert!(array.is_null(1));
		assert_eq!(array.null_count(), 1);
		assert!(is_defined(&array, 0));
		assert!(!is_defined(&array, 1));
		assert!(!is_defined(&array, 2));
		assert_eq!(get(&array, 0), Some(digest.clone()));
		assert_eq!(get(&array, 1), None);
		assert_eq!(get_value(&array, 0), Value::Digest(Box::new(digest)));
		assert_eq!(get_value(&array, 1), Value::none());
		assert_eq!(as_string(&array, 1), Value::none().to_string());
		assert_eq!(iter(&array).filter(Option::is_some).count(), 1);
	}

	#[test]
	fn reorder_past_the_end_fails() {
		// An out of range row is a bug, so it must fail naming the row and length, never read as a none digest.
		let error = varlen_array::reorder(&digest_array([Some(built(10_000, &[1.0]))]), &[4, 0]).unwrap_err();
		assert_eq!(error.diagnostic().message, "row index 4 out of range for a column of 1 rows");
	}
}
