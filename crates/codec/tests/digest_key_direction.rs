// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{serializer::KeySerializer, sort::SortOrder};
use reifydb_value::value::{Value, digest::Digest, value_type::ValueType};

fn digest_value() -> Value {
	let mut digest = Digest::new(ValueType::Float8, 10_000).unwrap();
	for value in [1.0, 2.0, 3.0] {
		digest.add_value(&Value::float8(value)).unwrap();
	}
	Value::Digest(Box::new(digest))
}

fn direction_error_code(value: &Value, direction: SortOrder) -> String {
	let mut serializer = KeySerializer::new();
	let Err(err) = serializer.extend_value_with_direction(value, direction) else {
		panic!("a digest must never become part of a directed key");
	};
	err.diagnostic().code
}

#[test]
fn a_digest_in_an_ascending_key_is_an_error_not_a_panic() {
	// Ascending writes straight into the serializer, so the error must come from that path.
	assert_eq!(direction_error_code(&digest_value(), SortOrder::Asc), "SERDE_003");
}

#[test]
fn a_digest_in_a_descending_key_is_an_error_not_a_panic() {
	// Descending encodes into a scratch serializer before inverting, and that inner failure must not be unwrapped.
	assert_eq!(direction_error_code(&digest_value(), SortOrder::Desc), "SERDE_003");
}

#[test]
fn a_none_of_digest_type_in_a_directed_key_is_an_error() {
	// A none still carries the digest type, and keying it would let a digest column into a sort key while empty.
	let none = Value::none_of(digest_value().get_type());

	assert_eq!(direction_error_code(&none, SortOrder::Asc), "SERDE_003");
	assert_eq!(direction_error_code(&none, SortOrder::Desc), "SERDE_003");
}

#[test]
fn a_digest_nested_in_a_list_in_a_descending_key_is_an_error() {
	// The scalar element is encoded first, so a partial key must not be inverted and returned.
	let nested = Value::List(vec![Value::Int4(1), digest_value()]);

	let mut serializer = KeySerializer::new();
	assert!(serializer.extend_value_with_direction(&nested, SortOrder::Desc).is_err());
}

#[test]
fn a_scalar_in_a_descending_key_still_orders_in_reverse() {
	// Making the call fallible must not change the encoding of keyable values.
	let key = |value: i32, direction: SortOrder| {
		let mut serializer = KeySerializer::new();
		serializer.extend_value_with_direction(&Value::Int4(value), direction).unwrap();
		serializer.to_encoded_key().to_vec()
	};

	assert!(key(1, SortOrder::Asc) < key(2, SortOrder::Asc));
	assert!(key(1, SortOrder::Desc) > key(2, SortOrder::Desc));
}
