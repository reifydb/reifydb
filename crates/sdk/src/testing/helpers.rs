// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Helper functions for common test patterns

use reifydb_core::encoded::{
	key::{EncodedKey, IntoEncodedKey},
	row::EncodedRow,
	schema::RowSchema,
};
use reifydb_type::value::Value;

/// Get all values from an encoded row using a schema
pub fn get_values(schema: &RowSchema, row: &EncodedRow) -> Vec<Value> {
	(0..schema.field_count()).map(|i| schema.get_value(row, i)).collect()
}

/// Helper to encode a key using IntoEncodedKey
pub fn encode_key<K: IntoEncodedKey>(key: K) -> EncodedKey {
	key.into_encoded_key()
}
