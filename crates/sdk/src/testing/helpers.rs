// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Helper functions for common test patterns

use reifydb_core::value::encoded::{
	encoded::EncodedValues,
	key::{EncodedKey, IntoEncodedKey},
	layout::EncodedValuesLayout,
	named::EncodedValuesNamedLayout,
};
use reifydb_type::value::Value;

/// Get all values from an encoded row using a layout
pub fn get_values(layout: &EncodedValuesLayout, row: &EncodedValues) -> Vec<Value> {
	(0..layout.fields.len()).map(|i| layout.get_value(row, i)).collect()
}

/// Get all values from an encoded row using a named layout
pub fn get_values_named(layout: &EncodedValuesNamedLayout, row: &EncodedValues) -> Vec<Value> {
	(0..layout.names().len()).map(|i| layout.get_value_by_idx(row, i)).collect()
}

/// Helper to encode a key using IntoEncodedKey
pub fn encode_key<K: IntoEncodedKey>(key: K) -> EncodedKey {
	key.into_encoded_key()
}
