// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::json::{from::parse_value, none_marker};
use reifydb_value::{
	fragment::Fragment,
	value::{
		Value,
		blob::Blob,
		date::Date,
		datetime::DateTime,
		decimal::parse::parse_decimal,
		duration::Duration,
		identity::IdentityId,
		int::parse::parse_int,
		ordered_f32::OrderedF32,
		ordered_f64::OrderedF64,
		time::Time,
		uint::parse::parse_uint,
		uuid::parse::{parse_uuid4, parse_uuid7},
		value_type::ValueType,
	},
};

fn option(inner: ValueType) -> ValueType {
	ValueType::Option(Box::new(inner))
}

fn uuid7() -> reifydb_value::value::uuid::Uuid7 {
	parse_uuid7(Fragment::internal("01890000-0000-7000-8000-000000000001")).unwrap()
}

fn base_values() -> Vec<Value> {
	vec![
		Value::Boolean(true),
		Value::Float4(OrderedF32::try_from(1.5f32).unwrap()),
		Value::Float8(OrderedF64::try_from(-2.25f64).unwrap()),
		Value::Int1(-8),
		Value::Int2(-16),
		Value::Int4(-32),
		Value::Int8(-64),
		Value::Int16(-128),
		Value::Utf8("hello".to_string()),
		Value::Uint1(8),
		Value::Uint2(16),
		Value::Uint4(32),
		Value::Uint8(64),
		Value::Uint16(128),
		Value::Date(Date::from_ymd(2024, 3, 5).unwrap()),
		Value::DateTime(DateTime::from_epoch_secs(1_700_000_000).unwrap()),
		Value::Time(Time::from_hms(1, 2, 3).unwrap()),
		Value::Duration(Duration::from_seconds(90).unwrap()),
		Value::IdentityId(IdentityId::from(uuid7())),
		Value::Uuid4(parse_uuid4(Fragment::internal("550e8400-e29b-41d4-a716-446655440000")).unwrap()),
		Value::Uuid7(uuid7()),
		Value::Blob(Blob::new(vec![0xde, 0xad])),
		Value::Int(parse_int(Fragment::internal("123456789012345678901234567890")).unwrap()),
		Value::Uint(parse_uint(Fragment::internal("123456789012345678901234567890")).unwrap()),
		Value::Decimal(parse_decimal(Fragment::internal("12.345")).unwrap()),
	]
}

// the frame encoder writes a cell as its Display form, except blobs which are hex
fn frame_text(value: &Value) -> String {
	match value {
		Value::Blob(b) => b.to_hex(),
		other => other.to_string(),
	}
}

#[test]
fn every_base_type_round_trips_through_its_frame_text() {
	for value in base_values() {
		let ty = value.get_type();
		assert_eq!(parse_value(&ty, &frame_text(&value)), Ok(value.clone()), "{ty}");
	}
}

#[test]
fn a_present_value_on_an_option_type_is_the_base_value() {
	assert_eq!(parse_value(&option(ValueType::Int4), "7"), Ok(Value::Int4(7)));
	assert_eq!(parse_value(&option(option(ValueType::Utf8)), "seven"), Ok(Value::Utf8("seven".to_string())));
}

#[test]
fn a_none_at_each_layer_of_option_option_int4_names_what_it_is_a_none_of() {
	let ty = option(option(ValueType::Int4));
	assert_eq!(parse_value(&ty, &none_marker(0)), Ok(Value::none_of(option(ValueType::Int4))));
	assert_eq!(parse_value(&ty, &none_marker(1)), Ok(Value::none_of(ValueType::Int4)));
}

#[test]
fn a_none_marker_on_a_non_option_type_is_rejected() {
	let err = parse_value(&ValueType::Int4, &none_marker(0)).unwrap_err().to_string();
	assert!(err.contains("none") && err.contains("Int4"), "{err}");
	let err = parse_value(&ValueType::Utf8, &none_marker(1)).unwrap_err().to_string();
	assert!(err.contains("none") && err.contains("Utf8"), "{err}");
}

#[test]
fn a_none_marker_at_or_beyond_the_option_depth_is_rejected() {
	let err = parse_value(&option(ValueType::Int4), &none_marker(1)).unwrap_err().to_string();
	assert!(err.contains("none") && err.contains("Option(Int4)"), "{err}");
	let err = parse_value(&option(option(ValueType::Int4)), &none_marker(2)).unwrap_err().to_string();
	assert!(err.contains("none") && err.contains("Option(Option(Int4))"), "{err}");
}

#[test]
fn garbage_text_is_rejected_with_the_type_in_the_message() {
	let cases = [
		(ValueType::Boolean, "yes"),
		(ValueType::Int4, "abc"),
		(ValueType::Int1, "200"),
		(ValueType::Uint2, "-1"),
		(ValueType::Float4, "NaN"),
		(ValueType::Date, "2024-13-40"),
		(ValueType::DateTime, "yesterday"),
		(ValueType::Time, "25:61:00"),
		(ValueType::Duration, "soon"),
		(ValueType::Uuid4, "01890000-0000-7000-8000-000000000001"),
		(ValueType::Uuid7, "550e8400-e29b-41d4-a716-446655440000"),
		(ValueType::IdentityId, "not-a-uuid"),
		(ValueType::Blob, "0xzz"),
		(ValueType::Int, "abc"),
		(ValueType::Uint, "-1"),
		(ValueType::Decimal, "twelve"),
		(option(ValueType::Int4), "abc"),
	];
	for (ty, text) in cases {
		let err = parse_value(&ty, text).unwrap_err().to_string();
		assert!(err.contains(&ty.to_string()) && err.contains(text), "{ty}: {err}");
	}
}
