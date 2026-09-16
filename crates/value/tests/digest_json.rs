// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	util::hex::encode,
	value::{Value, digest::Digest, duration::Duration, value_type::ValueType},
};
use serde_json::Value as JsonValue;

fn digest(inner: ValueType, accuracy: u32, values: Vec<Value>) -> Digest {
	let mut digest = Digest::new(inner, accuracy).unwrap();
	for value in values {
		digest.add_value(&value).unwrap();
	}
	digest
}

fn hex_of(digest: &Digest) -> JsonValue {
	JsonValue::String(format!("0x{}", encode(&digest.encode())))
}

#[test]
fn a_float_digest_json_value_is_the_hex_of_its_canonical_bytes() {
	// A rendered count such as digest(n: 3) cannot be decoded back, so the JSON cell must carry the bytes.
	let digest =
		digest(ValueType::Float8, 10_000, vec![Value::float8(1.0), Value::float8(2.0), Value::float8(100.0)]);
	let expected = hex_of(&digest);

	assert_eq!(Value::Digest(Box::new(digest)).to_json_value(), expected);
}

#[test]
fn a_duration_digest_json_value_keeps_its_inner_type_and_accuracy_in_the_bytes() {
	// The hex must be the full encoding, otherwise a duration digest decodes with the wrong type or accuracy.
	let values = [5, 50, 5_000]
		.into_iter()
		.map(|ms| Value::Duration(Duration::from_milliseconds(ms).unwrap()))
		.collect();
	let digest = digest(ValueType::Duration, 1_000, values);
	let expected = hex_of(&digest);

	assert_eq!(Value::Digest(Box::new(digest)).to_json_value(), expected);
}

#[test]
fn a_digest_inside_a_list_is_written_as_hex_too() {
	// Nested values go through the same conversion, so a list must not fall back to the rendered count.
	let digest = digest(ValueType::Int4, 50_000, vec![Value::Int4(10), Value::Int4(1_000)]);
	let expected = JsonValue::Array(vec![hex_of(&digest)]);

	assert_eq!(Value::List(vec![Value::Digest(Box::new(digest))]).to_json_value(), expected);
}
