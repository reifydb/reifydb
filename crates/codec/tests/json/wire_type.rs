// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::json::wire_type::{WireValueType, from_json, to_json};
use reifydb_value::value::value_type::ValueType;
use serde_json::{Value as JsonValue, from_str, from_value, json, to_value};

// The wire names a type by its `id` and carries the types it wraps under `underlying`. Requests and
// responses share this rendering, so a type that survives one direction must survive the other; the
// round trip tests below are what keep the two directions from drifting apart.

fn option(inner: ValueType) -> ValueType {
	ValueType::Option(Box::new(inner))
}

fn round_trip(ty: ValueType) {
	let rendered = to_json(&ty);
	assert_eq!(from_json(&rendered), Ok(ty.clone()), "{ty} did not survive the round trip: {rendered}");
}

#[test]
fn a_scalar_type_is_an_object_naming_it_and_never_a_bare_string() {
	// A bare "Utf8" was the old rendering. Every type is an object now, so a consumer reads one shape
	// instead of branching on whether the type happens to wrap something.
	assert_eq!(to_json(&ValueType::Utf8), json!({"id": "Utf8"}));
	assert_eq!(to_json(&ValueType::Int4), json!({"id": "Int4"}));
	assert_eq!(to_json(&ValueType::Boolean), json!({"id": "Boolean"}));
}

#[test]
fn a_scalar_type_carries_no_underlying_key_at_all() {
	let rendered = to_json(&ValueType::Int4);
	assert!(
		rendered.get("underlying").is_none(),
		"a type that wraps nothing must omit the key rather than send an empty one: {rendered}"
	);
}

#[test]
fn an_option_names_option_as_the_id_and_the_wrapped_type_as_the_underlying() {
	assert_eq!(to_json(&option(ValueType::Int4)), json!({"id": "Option", "underlying": {"id": "Int4"}}));
}

#[test]
fn each_option_layer_nests_one_level_deeper_so_the_depth_is_readable() {
	// The none markers say how many layers are present, so a reader that cannot count the declared
	// layers cannot tell an outer none from an inner one.
	assert_eq!(
		to_json(&option(option(ValueType::Utf8))),
		json!({"id": "Option", "underlying": {"id": "Option", "underlying": {"id": "Utf8"}}})
	);
	assert_eq!(
		to_json(&option(option(option(ValueType::Int4)))),
		json!({
			"id": "Option",
			"underlying": {"id": "Option", "underlying": {"id": "Option", "underlying": {"id": "Int4"}}}
		})
	);
}

#[test]
fn a_list_carries_its_element_type_as_a_single_underlying_object() {
	assert_eq!(to_json(&ValueType::list_of(ValueType::Int4)), json!({"id": "List", "underlying": {"id": "Int4"}}));
}

#[test]
fn a_tuple_carries_its_members_as_an_ordered_underlying_array() {
	let ty = ValueType::Tuple(vec![ValueType::Int4, ValueType::Utf8, option(ValueType::Boolean)]);
	assert_eq!(
		to_json(&ty),
		json!({
			"id": "Tuple",
			"underlying": [
				{"id": "Int4"},
				{"id": "Utf8"},
				{"id": "Option", "underlying": {"id": "Boolean"}}
			]
		})
	);
}

#[test]
fn a_record_carries_named_fields_so_the_order_and_the_names_both_survive() {
	let ty = ValueType::Record(vec![
		("id".to_string(), ValueType::Int4),
		("label".to_string(), option(ValueType::Utf8)),
	]);
	assert_eq!(
		to_json(&ty),
		json!({
			"id": "Record",
			"underlying": [
				{"name": "id", "type": {"id": "Int4"}},
				{"name": "label", "type": {"id": "Option", "underlying": {"id": "Utf8"}}}
			]
		})
	);
}

#[test]
fn every_type_the_wire_can_carry_survives_a_round_trip() {
	for ty in [
		ValueType::Boolean,
		ValueType::Float4,
		ValueType::Float8,
		ValueType::Int1,
		ValueType::Int2,
		ValueType::Int4,
		ValueType::Int8,
		ValueType::Int16,
		ValueType::Utf8,
		ValueType::Uint1,
		ValueType::Uint2,
		ValueType::Uint4,
		ValueType::Uint8,
		ValueType::Uint16,
		ValueType::Date,
		ValueType::DateTime,
		ValueType::Time,
		ValueType::Duration,
		ValueType::IdentityId,
		ValueType::Uuid4,
		ValueType::Uuid7,
		ValueType::Blob,
		ValueType::Int,
		ValueType::Uint,
		ValueType::Decimal,
		ValueType::Any,
		ValueType::DictionaryId,
	] {
		round_trip(ty);
	}
}

#[test]
fn the_wrapping_types_survive_a_round_trip_including_nested_ones() {
	round_trip(option(ValueType::Int4));
	round_trip(option(option(ValueType::Utf8)));
	round_trip(ValueType::list_of(option(ValueType::Int4)));
	round_trip(ValueType::Tuple(vec![ValueType::Int4, option(ValueType::Utf8)]));
	round_trip(ValueType::Record(vec![
		("a".to_string(), ValueType::Int4),
		("b".to_string(), ValueType::list_of(ValueType::Utf8)),
	]));
}

#[test]
fn the_serde_impls_agree_with_the_json_helpers() {
	// ResponseColumn and the request parameters both go through serde rather than the helpers, so a
	// helper that drifts from the impl would corrupt the wire while these helpers still looked right.
	let ty = option(option(ValueType::Int4));
	let wire = WireValueType(ty.clone());
	assert_eq!(to_value(&wire).unwrap(), to_json(&ty));
	assert_eq!(from_value::<WireValueType>(to_json(&ty)).unwrap(), wire);
}

#[test]
fn a_type_that_is_not_an_object_is_rejected_rather_than_guessed_at() {
	// The old rendering sent a bare string for a scalar. Accepting it would leave two spellings alive.
	let err = from_json(&JsonValue::String("Int4".to_string())).unwrap_err();
	assert!(err.contains("expected a type descriptor object"), "{err}");
}

#[test]
fn a_descriptor_without_an_id_is_rejected() {
	let err = from_json(&json!({"underlying": {"id": "Int4"}})).unwrap_err();
	assert!(err.contains("missing a string `id`"), "{err}");
}

#[test]
fn an_unknown_id_is_reported_by_name_so_the_caller_can_see_the_typo() {
	let err = from_json(&json!({"id": "Utf9"})).unwrap_err();
	assert!(err.contains("unknown type id `Utf9`"), "{err}");
}

#[test]
fn a_wrapping_type_without_its_underlying_is_rejected_rather_than_defaulted() {
	// Silently reading a bare Option as Option(Any) would hand the decoder a type the server never sent.
	let err = from_json(&json!({"id": "Option"})).unwrap_err();
	assert!(err.contains("needs an `underlying` type"), "{err}");

	let err = from_json(&json!({"id": "Tuple"})).unwrap_err();
	assert!(err.contains("needs an `underlying` array"), "{err}");
}

#[test]
fn a_record_field_missing_its_name_or_type_is_rejected() {
	let err = from_json(&json!({"id": "Record", "underlying": [{"type": {"id": "Int4"}}]})).unwrap_err();
	assert!(err.contains("missing `name`"), "{err}");

	let err = from_json(&json!({"id": "Record", "underlying": [{"name": "a"}]})).unwrap_err();
	assert!(err.contains("missing `type`"), "{err}");
}

#[test]
fn a_descriptor_parses_from_the_text_a_client_would_actually_send() {
	let wire: WireValueType =
		from_str(r#"{"id":"Option","underlying":{"id":"Option","underlying":{"id":"Utf8"}}}"#).unwrap();
	assert_eq!(wire.0, option(option(ValueType::Utf8)));
}
