// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Helper functions for common test patterns

use reifydb_core::encoded::{
	key::{EncodedKey, IntoEncodedKey},
	row::EncodedRow,
	shape::RowShape,
};
use reifydb_type::value::Value;

/// Get all values from an encoded row using a shape
pub fn get_values(shape: &RowShape, row: &EncodedRow) -> Vec<Value> {
	(0..shape.field_count()).map(|i| shape.get_value(row, i)).collect()
}

/// Helper to encode a key using IntoEncodedKey
pub fn encode_key<K: IntoEncodedKey>(key: K) -> EncodedKey {
	key.into_encoded_key()
}
