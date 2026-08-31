// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::{EncodedKey, IntoEncodedKey},
	row::{bytes::EncodedBytes, shape::RowShape},
};
use reifydb_core::key::operator::state::{GroupStateKey, custom_not_cached_key};
use reifydb_value::value::Value;

pub fn get_values(shape: &RowShape, bytes: &EncodedBytes) -> Vec<Value> {
	(0..shape.field_count()).map(|i| shape.get_value(bytes, i)).collect()
}

pub fn encode_key<K: IntoEncodedKey>(key: K) -> EncodedKey {
	key.into_encoded_key()
}

pub fn probe_row_key(row_number: u64) -> GroupStateKey {
	custom_not_cached_key(encode_key(format!("row_{}", row_number)).as_ref())
		.expect("a probe row key must fit the keyspace's id width")
}
