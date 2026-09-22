// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{Array, LargeBinaryArray, LargeStringArray};
use postcard::{from_bytes, to_allocvec};
use reifydb_value::value::{
	blob::Blob,
	container::varlen_array::{self, blob_array},
};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct BlobColumn(#[serde(serialize_with = "varlen_array::serialize")] LargeBinaryArray);

#[derive(Deserialize)]
struct Utf8Column(#[serde(deserialize_with = "varlen_array::deserialize_utf8")] LargeStringArray);

#[test]
fn utf8_column_load_rejects_bytes_that_are_not_utf8() {
	// Loading bytes that are not UTF-8 into a string column must fail, never build strings that assume they are.
	let bytes = to_allocvec(&BlobColumn(blob_array(&[Blob::new(b"ok".to_vec()), Blob::new(vec![0xff, 0xfe])])))
		.expect("a blob column serializes");
	let rows = from_bytes::<Utf8Column>(&bytes).map(|column| column.0.len());
	assert!(rows.is_err(), "bytes that are not UTF-8 loaded as a string column of {rows:?} rows");
}
