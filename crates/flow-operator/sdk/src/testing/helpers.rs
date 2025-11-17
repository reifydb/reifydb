// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Helper functions for common test patterns

use reifydb_core::value::encoded::{
	EncodedKey, EncodedValues, EncodedValuesLayout, EncodedValuesNamedLayout, IntoEncodedKey,
};
use reifydb_type::Value;

/// Get all values from an encoded row using a layout
pub fn get_values(layout: &EncodedValuesLayout, row: &EncodedValues) -> Vec<Value> {
	(0..layout.fields.len()).map(|i| layout.get_value(row, i)).collect()
}

/// Get all values from an encoded row using a named layout
pub fn get_values_named(layout: &EncodedValuesNamedLayout, row: &EncodedValues) -> Vec<Value> {
	(0..layout.names().len()).map(|i| layout.get_value(row, i)).collect()
}

/// Helper to encode a key using IntoEncodedKey
pub fn encode_key<K: IntoEncodedKey>(key: K) -> EncodedKey {
	key.into_encoded_key()
}
