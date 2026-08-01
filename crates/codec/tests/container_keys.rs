// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Key encoding for the container values: List, Record and Tuple. Elements are written back to back
//! and closed with `CONTAINER_END`, which cannot collide with an element's leading `ValueKind` tag,
//! so a sequence that is a prefix of another sorts after it, exactly as a shorter string does.

use reifydb_codec::key::{deserializer::KeyDeserializer, serializer::KeySerializer};
use reifydb_value::value::{Value, blob::Blob};

fn encode(value: &Value) -> Vec<u8> {
	let mut serializer = KeySerializer::new();
	serializer.extend_value(value);
	serializer.to_encoded_key().to_vec()
}

fn decode(bytes: &[u8]) -> Value {
	let mut de = KeyDeserializer::from_bytes(bytes);
	let value = de.read_value().expect("value must decode");
	assert_eq!(de.remaining(), 0, "decoding must consume the whole key, leftover bytes mean a framing bug");
	value
}

fn roundtrip(value: Value) {
	let decoded = decode(&encode(&value));
	assert_eq!(decoded, value, "container must survive an encode/decode round trip");
}

fn utf8(s: &str) -> Value {
	Value::Utf8(s.to_string())
}

#[test]
fn empty_containers_roundtrip() {
	roundtrip(Value::List(vec![]));
	roundtrip(Value::Tuple(vec![]));
	roundtrip(Value::Record(vec![]));
}

#[test]
fn single_element_containers_roundtrip() {
	roundtrip(Value::List(vec![Value::Int4(7)]));
	roundtrip(Value::Tuple(vec![utf8("a")]));
	roundtrip(Value::Record(vec![("f".to_string(), Value::Boolean(true))]));
}

#[test]
fn mixed_element_types_roundtrip() {
	roundtrip(Value::List(vec![Value::Int4(-1), utf8("two"), Value::Boolean(false), Value::Uint8(9)]));
	roundtrip(Value::Record(vec![
		("id".to_string(), Value::Int8(42)),
		("name".to_string(), utf8("reify")),
		("ok".to_string(), Value::Boolean(true)),
	]));
}

#[test]
fn nested_containers_roundtrip() {
	roundtrip(Value::List(vec![Value::List(vec![Value::Int4(1), Value::Int4(2)]), Value::List(vec![])]));
	roundtrip(Value::Tuple(vec![Value::List(vec![utf8("x")]), Value::Tuple(vec![Value::Int4(3)])]));
	roundtrip(Value::Record(vec![(
		"items".to_string(),
		Value::List(vec![Value::Record(vec![("k".to_string(), Value::Int4(1))])]),
	)]));
}

#[test]
fn element_payload_containing_the_terminator_byte_roundtrips() {
	// encode_u8 inverts, so Uint1(0) writes a literal 0xff, the same byte as CONTAINER_END. A
	// decoder that scanned for that byte instead of stepping element by element would truncate.
	assert!(encode(&Value::Uint1(0)).contains(&0xff), "Uint1(0) must encode a literal 0xff or this proves nothing");

	roundtrip(Value::List(vec![Value::Uint1(0), Value::Uint1(1), Value::Uint1(0)]));
	roundtrip(Value::List(vec![Value::Uint2(0), Value::Uint4(0), Value::Uint8(0)]));
	roundtrip(Value::Tuple(vec![Value::Int1(-1), Value::Int4(-1)]));
}

#[test]
fn byte_payloads_with_escapes_roundtrip_inside_containers() {
	// Strings and blobs escape 0xff as [0xff, 0x00] and close with [0xff, 0xff]; nested in a
	// container, neither sequence may be read as the container terminator.
	roundtrip(Value::List(vec![Value::Blob(Blob::from(vec![0xff, 0x00, 0xff, 0xff])), Value::Int4(1)]));
	roundtrip(Value::List(vec![utf8("a\u{00ff}b"), utf8("")]));
	roundtrip(Value::Record(vec![("\u{00ff}".to_string(), Value::Blob(Blob::from(vec![0xff])))]));
}

#[test]
fn a_container_is_distinguishable_from_its_single_element() {
	assert_ne!(
		encode(&Value::List(vec![Value::Int4(1)])),
		encode(&Value::Int4(1)),
		"a one-element list must not encode identically to the bare element"
	);
}

#[test]
fn list_tuple_and_record_with_equal_contents_encode_differently() {
	let list = encode(&Value::List(vec![Value::Int4(1)]));
	let tuple = encode(&Value::Tuple(vec![Value::Int4(1)]));
	assert_ne!(list, tuple, "List and Tuple must be distinguishable by their kind tag");
	assert_ne!(decode(&list), decode(&tuple));
}

#[test]
fn a_container_key_composes_with_following_components() {
	let mut serializer = KeySerializer::new();
	serializer.extend_value(&Value::List(vec![Value::Int4(1), Value::Int4(2)]));
	serializer.extend_u64(99u64);
	let bytes = serializer.to_encoded_key().to_vec();

	let mut de = KeyDeserializer::from_bytes(&bytes);
	assert_eq!(de.read_value().unwrap(), Value::List(vec![Value::Int4(1), Value::Int4(2)]));
	assert_eq!(de.read_u64().unwrap(), 99, "the container must consume exactly its own bytes and no more");
	assert_eq!(de.remaining(), 0);
}

#[test]
fn a_prefix_list_sorts_after_the_list_that_extends_it() {
	// Containers follow the prefix convention of encode_bytes: the terminator sorts above any
	// element tag, so a sequence that is a prefix of another sorts after it.
	let short = encode(&Value::List(vec![Value::Int4(1), Value::Int4(2)]));
	let long = encode(&Value::List(vec![Value::Int4(1), Value::Int4(2), Value::Int4(3)]));
	assert!(short > long, "[1,2] must sort after [1,2,3], matching how \"ab\" sorts after \"abc\"");
}

#[test]
fn the_empty_list_sorts_after_every_non_empty_list() {
	let empty = encode(&Value::List(vec![]));
	for other in [
		Value::List(vec![Value::Int4(0)]),
		Value::List(vec![utf8("")]),
		Value::List(vec![Value::Boolean(false)]),
	] {
		assert!(empty > encode(&other), "the empty list must sort after {other:?} under the prefix convention");
	}
}

#[test]
fn lists_of_equal_length_order_by_their_first_differing_element() {
	let a = encode(&Value::List(vec![Value::Int4(1), Value::Int4(1)]));
	let b = encode(&Value::List(vec![Value::Int4(1), Value::Int4(2)]));
	let c = encode(&Value::List(vec![Value::Int4(2), Value::Int4(0)]));

	assert_eq!(
		encode(&Value::Int4(1)) > encode(&Value::Int4(2)),
		a > b,
		"element ordering inside a list must match the element type's own encoded ordering"
	);
	assert_eq!(
		encode(&Value::Int4(1)) > encode(&Value::Int4(2)),
		a > c,
		"the first differing element decides, regardless of what follows it"
	);
}

#[test]
fn nested_list_ordering_follows_the_inner_elements() {
	let a = encode(&Value::List(vec![Value::List(vec![Value::Int4(1)])]));
	let b = encode(&Value::List(vec![Value::List(vec![Value::Int4(2)])]));
	assert_eq!(
		encode(&Value::Int4(1)) > encode(&Value::Int4(2)),
		a > b,
		"nesting must not invert the ordering of the innermost elements"
	);
}

#[test]
fn equal_containers_encode_identically() {
	let a = Value::Record(vec![("k".to_string(), Value::List(vec![Value::Int4(1), utf8("x")]))]);
	let b = Value::Record(vec![("k".to_string(), Value::List(vec![Value::Int4(1), utf8("x")]))]);
	assert_eq!(encode(&a), encode(&b), "equal values must encode to equal bytes or group-by keys break");
}

#[test]
fn record_field_order_is_significant() {
	// The encoding preserves the field order given rather than canonicalising it, so two records
	// differing only in order are different values and must encode differently.
	let a = Value::Record(vec![("a".to_string(), Value::Int4(1)), ("b".to_string(), Value::Int4(2))]);
	let b = Value::Record(vec![("b".to_string(), Value::Int4(2)), ("a".to_string(), Value::Int4(1))]);
	assert_ne!(a, b, "the values themselves differ by field order");
	assert_ne!(encode(&a), encode(&b), "so their encodings must differ too");
}

#[test]
fn a_record_field_name_cannot_be_confused_with_its_value() {
	let a = Value::Record(vec![("ab".to_string(), utf8("c"))]);
	let b = Value::Record(vec![("a".to_string(), utf8("bc"))]);
	assert_ne!(encode(&a), encode(&b), "name and value must stay separately framed");
	roundtrip(a);
	roundtrip(b);
}
