// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::encoded::{
	key::{EncodedKey, IntoEncodedKey},
	row::EncodedRow,
	shape::RowShape,
};
use reifydb_value::value::Value;

pub fn get_values(shape: &RowShape, row: &EncodedRow) -> Vec<Value> {
	(0..shape.field_count()).map(|i| shape.get_value(row, i)).collect()
}

pub fn encode_key<K: IntoEncodedKey>(key: K) -> EncodedKey {
	key.into_encoded_key()
}
