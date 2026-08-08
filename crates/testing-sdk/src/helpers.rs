// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	encoded::{bytes::EncodedBytes, shape::RowShape},
	key::encoded::{EncodedKey, IntoEncodedKey},
};
use reifydb_core::key::operator_group_state::{GroupStateKey, Keyspace};
use reifydb_value::value::Value;

pub fn get_values(shape: &RowShape, row: &EncodedBytes) -> Vec<Value> {
	(0..shape.field_count()).map(|i| shape.get_value(row, i)).collect()
}

pub fn encode_key<K: IntoEncodedKey>(key: K) -> EncodedKey {
	key.into_encoded_key()
}

pub fn probe_row_key(row_number: u64) -> GroupStateKey {
	GroupStateKey::node_scoped(Keyspace::FIRST_CUSTOM, encode_key(format!("row_{}", row_number)).as_ref())
}
