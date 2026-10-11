// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use postcard::{from_bytes, to_allocvec};
use reifydb_macro::catalog_shape;
use reifydb_value::value::Value;

use crate::{Result, error::CatalogError};

catalog_shape! {
	pub(crate) series {
		id: u64,
		namespace: u64,
		name: utf8,
		tag: u64,
		key_column: utf8,
		key_kind: u8,
		precision: u8,
		primary_key: u64,
		partition_by: utf8,
		ts: utf8,
		time_domain: u8,
		encoding_name: utf8?,
		encoding_config: blob,
	}

	pub(crate) series_namespace {
		id: u64,
		name: utf8,
	}
}

pub fn serialize_encoding_config(config: &[(String, Value)]) -> Vec<u8> {
	to_allocvec(config).expect("postcard serialization of an encoding config is total")
}

pub fn deserialize_encoding_config(name: &str, bytes: &[u8]) -> Result<Vec<(String, Value)>> {
	from_bytes(bytes).map_err(|e| {
		CatalogError::SeriesEncodingConfigCorrupt {
			name: name.to_string(),
			reason: e.to_string(),
		}
		.into()
	})
}

#[cfg(test)]
mod tests {
	use postcard::to_allocvec;
	use reifydb_value::value::Value;

	use super::{deserialize_encoding_config, serialize_encoding_config};

	#[test]
	fn test_encoding_config_round_trip_preserves_order_and_types() {
		// A sort or a map in the serializer changes the config an encoding reads back at bind.
		let config = vec![("b".to_string(), Value::Utf8("x".to_string())), ("a".to_string(), Value::Int4(-1))];
		let bytes = serialize_encoding_config(&config);
		assert_eq!(deserialize_encoding_config("delta", &bytes).unwrap(), config);
	}

	#[test]
	fn test_encoding_config_bytes_are_plain_postcard() {
		// A prefix or version byte breaks parity with the config bytes in the block header.
		let config = vec![("column".to_string(), Value::Utf8("v".to_string()))];
		assert_eq!(serialize_encoding_config(&config), to_allocvec(&config).unwrap());
	}
}
