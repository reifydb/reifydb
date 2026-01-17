// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Helper functions for common test patterns

use reifydb_core::encoded::{
	encoded::EncodedValues,
	key::{EncodedKey, IntoEncodedKey},
	schema::Schema,
};
use reifydb_type::value::Value;

/// Get all values from an encoded row using a schema
pub fn get_values(schema: &Schema, row: &EncodedValues) -> Vec<Value> {
	(0..schema.field_count()).map(|i| schema.get_value(row, i)).collect()
}

/// Helper to encode a key using IntoEncodedKey
pub fn encode_key<K: IntoEncodedKey>(key: K) -> EncodedKey {
	key.into_encoded_key()
}
