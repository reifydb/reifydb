// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::shape::{RowFamily, RowShapeField, fingerprint::compute_fingerprint};
use reifydb_value::value::{
	constraint::{Constraint, TypeConstraint, bytes::MaxBytes, precision::Precision, scale::Scale},
	value_type::ValueType,
};

fn make_field(name: &str, field_type: ValueType) -> RowShapeField {
	RowShapeField {
		name: name.to_string(),
		constraint: TypeConstraint::unconstrained(field_type),
		offset: 0,
		size: 0,
	}
}

fn make_constrained_field(name: &str, constraint: TypeConstraint) -> RowShapeField {
	RowShapeField {
		name: name.to_string(),
		constraint,
		offset: 0,
		size: 0,
	}
}

#[test]
fn test_fingerprint_deterministic() {
	let fields1 = vec![make_field("a", ValueType::Int4), make_field("b", ValueType::Utf8)];

	let fields2 = vec![make_field("a", ValueType::Int4), make_field("b", ValueType::Utf8)];

	assert_eq!(
		compute_fingerprint(RowFamily::Deprecated, &fields1),
		compute_fingerprint(RowFamily::Deprecated, &fields2)
	);
}

#[test]
fn test_fingerprint_different_names() {
	let fields1 = vec![make_field("a", ValueType::Int4)];
	let fields2 = vec![make_field("b", ValueType::Int4)];

	assert_ne!(
		compute_fingerprint(RowFamily::Deprecated, &fields1),
		compute_fingerprint(RowFamily::Deprecated, &fields2)
	);
}

#[test]
fn test_fingerprint_different_types() {
	let fields1 = vec![make_field("a", ValueType::Int4)];
	let fields2 = vec![make_field("a", ValueType::Int8)];

	assert_ne!(
		compute_fingerprint(RowFamily::Deprecated, &fields1),
		compute_fingerprint(RowFamily::Deprecated, &fields2)
	);
}

#[test]
fn test_fingerprint_different_order() {
	let fields1 = vec![make_field("a", ValueType::Int4), make_field("b", ValueType::Utf8)];

	let fields2 = vec![make_field("b", ValueType::Utf8), make_field("a", ValueType::Int4)];

	assert_ne!(
		compute_fingerprint(RowFamily::Deprecated, &fields1),
		compute_fingerprint(RowFamily::Deprecated, &fields2)
	);
}

#[test]
fn test_fingerprint_empty_shape() {
	let fields: Vec<RowShapeField> = vec![];
	// Should not panic and should produce a valid hash
	let fp = compute_fingerprint(RowFamily::Deprecated, &fields);
	assert_ne!(*fp, 0);
}

#[test]
fn test_fingerprint_utf8_constrained_vs_unconstrained() {
	let unconstrained = vec![make_field("text", ValueType::Utf8)];
	let constrained = vec![make_constrained_field(
		"text",
		TypeConstraint::with_constraint(ValueType::Utf8, Constraint::MaxBytes(MaxBytes::new(255))),
	)];

	assert_ne!(
		compute_fingerprint(RowFamily::Deprecated, &unconstrained),
		compute_fingerprint(RowFamily::Deprecated, &constrained),
		"Utf8 unconstrained should differ from Utf8(255)"
	);
}

#[test]
fn test_fingerprint_utf8_same_constraint_deterministic() {
	let fields1 = vec![make_constrained_field(
		"text",
		TypeConstraint::with_constraint(ValueType::Utf8, Constraint::MaxBytes(MaxBytes::new(100))),
	)];
	let fields2 = vec![make_constrained_field(
		"text",
		TypeConstraint::with_constraint(ValueType::Utf8, Constraint::MaxBytes(MaxBytes::new(100))),
	)];

	assert_eq!(
		compute_fingerprint(RowFamily::Deprecated, &fields1),
		compute_fingerprint(RowFamily::Deprecated, &fields2),
		"Utf8(100) should produce same fingerprint"
	);
}

#[test]
fn test_fingerprint_utf8_different_max_bytes() {
	let small = vec![make_constrained_field(
		"text",
		TypeConstraint::with_constraint(ValueType::Utf8, Constraint::MaxBytes(MaxBytes::new(50))),
	)];
	let large = vec![make_constrained_field(
		"text",
		TypeConstraint::with_constraint(ValueType::Utf8, Constraint::MaxBytes(MaxBytes::new(500))),
	)];

	assert_ne!(
		compute_fingerprint(RowFamily::Deprecated, &small),
		compute_fingerprint(RowFamily::Deprecated, &large),
		"Utf8(50) should differ from Utf8(500)"
	);
}

#[test]
fn test_fingerprint_int_constrained_vs_unconstrained() {
	let unconstrained = vec![make_field("num", ValueType::Int)];
	let constrained = vec![make_constrained_field(
		"num",
		TypeConstraint::with_constraint(ValueType::Int, Constraint::MaxBytes(MaxBytes::new(8))),
	)];

	assert_ne!(
		compute_fingerprint(RowFamily::Deprecated, &unconstrained),
		compute_fingerprint(RowFamily::Deprecated, &constrained),
		"Int unconstrained should differ from Int(8)"
	);
}

#[test]
fn test_fingerprint_int_same_constraint_deterministic() {
	let fields1 = vec![make_constrained_field(
		"num",
		TypeConstraint::with_constraint(ValueType::Int, Constraint::MaxBytes(MaxBytes::new(16))),
	)];
	let fields2 = vec![make_constrained_field(
		"num",
		TypeConstraint::with_constraint(ValueType::Int, Constraint::MaxBytes(MaxBytes::new(16))),
	)];

	assert_eq!(
		compute_fingerprint(RowFamily::Deprecated, &fields1),
		compute_fingerprint(RowFamily::Deprecated, &fields2),
		"Int(16) should produce same fingerprint"
	);
}

#[test]
fn test_fingerprint_int_different_max_bytes() {
	let small = vec![make_constrained_field(
		"num",
		TypeConstraint::with_constraint(ValueType::Int, Constraint::MaxBytes(MaxBytes::new(4))),
	)];
	let large = vec![make_constrained_field(
		"num",
		TypeConstraint::with_constraint(ValueType::Int, Constraint::MaxBytes(MaxBytes::new(32))),
	)];

	assert_ne!(
		compute_fingerprint(RowFamily::Deprecated, &small),
		compute_fingerprint(RowFamily::Deprecated, &large),
		"Int(4) should differ from Int(32)"
	);
}

#[test]
fn test_fingerprint_uint_constrained_vs_unconstrained() {
	let unconstrained = vec![make_field("num", ValueType::Uint)];
	let constrained = vec![make_constrained_field(
		"num",
		TypeConstraint::with_constraint(ValueType::Uint, Constraint::MaxBytes(MaxBytes::new(8))),
	)];

	assert_ne!(
		compute_fingerprint(RowFamily::Deprecated, &unconstrained),
		compute_fingerprint(RowFamily::Deprecated, &constrained),
		"Uint unconstrained should differ from Uint(8)"
	);
}

#[test]
fn test_fingerprint_uint_same_constraint_deterministic() {
	let fields1 = vec![make_constrained_field(
		"num",
		TypeConstraint::with_constraint(ValueType::Uint, Constraint::MaxBytes(MaxBytes::new(64))),
	)];
	let fields2 = vec![make_constrained_field(
		"num",
		TypeConstraint::with_constraint(ValueType::Uint, Constraint::MaxBytes(MaxBytes::new(64))),
	)];

	assert_eq!(
		compute_fingerprint(RowFamily::Deprecated, &fields1),
		compute_fingerprint(RowFamily::Deprecated, &fields2),
		"Uint(64) should produce same fingerprint"
	);
}

#[test]
fn test_fingerprint_uint_different_max_bytes() {
	let small = vec![make_constrained_field(
		"num",
		TypeConstraint::with_constraint(ValueType::Uint, Constraint::MaxBytes(MaxBytes::new(2))),
	)];
	let large = vec![make_constrained_field(
		"num",
		TypeConstraint::with_constraint(ValueType::Uint, Constraint::MaxBytes(MaxBytes::new(128))),
	)];

	assert_ne!(
		compute_fingerprint(RowFamily::Deprecated, &small),
		compute_fingerprint(RowFamily::Deprecated, &large),
		"Uint(2) should differ from Uint(128)"
	);
}

#[test]
fn test_fingerprint_blob_constrained_vs_unconstrained() {
	let unconstrained = vec![make_field("data", ValueType::Blob)];
	let constrained = vec![make_constrained_field(
		"data",
		TypeConstraint::with_constraint(ValueType::Blob, Constraint::MaxBytes(MaxBytes::new(1024))),
	)];

	assert_ne!(
		compute_fingerprint(RowFamily::Deprecated, &unconstrained),
		compute_fingerprint(RowFamily::Deprecated, &constrained),
		"Blob unconstrained should differ from Blob(1024)"
	);
}

#[test]
fn test_fingerprint_blob_same_constraint_deterministic() {
	let fields1 = vec![make_constrained_field(
		"data",
		TypeConstraint::with_constraint(ValueType::Blob, Constraint::MaxBytes(MaxBytes::new(4096))),
	)];
	let fields2 = vec![make_constrained_field(
		"data",
		TypeConstraint::with_constraint(ValueType::Blob, Constraint::MaxBytes(MaxBytes::new(4096))),
	)];

	assert_eq!(
		compute_fingerprint(RowFamily::Deprecated, &fields1),
		compute_fingerprint(RowFamily::Deprecated, &fields2),
		"Blob(4096) should produce same fingerprint"
	);
}

#[test]
fn test_fingerprint_blob_different_max_bytes() {
	let small = vec![make_constrained_field(
		"data",
		TypeConstraint::with_constraint(ValueType::Blob, Constraint::MaxBytes(MaxBytes::new(256))),
	)];
	let large = vec![make_constrained_field(
		"data",
		TypeConstraint::with_constraint(ValueType::Blob, Constraint::MaxBytes(MaxBytes::new(65536))),
	)];

	assert_ne!(
		compute_fingerprint(RowFamily::Deprecated, &small),
		compute_fingerprint(RowFamily::Deprecated, &large),
		"Blob(256) should differ from Blob(65536)"
	);
}

#[test]
fn test_fingerprint_decimal_constrained_vs_unconstrained() {
	let unconstrained = vec![make_field("amount", ValueType::Decimal)];
	let constrained = vec![make_constrained_field(
		"amount",
		TypeConstraint::with_constraint(
			ValueType::Decimal,
			Constraint::PrecisionScale(Precision::new(10), Scale::new(2)),
		),
	)];

	assert_ne!(
		compute_fingerprint(RowFamily::Deprecated, &unconstrained),
		compute_fingerprint(RowFamily::Deprecated, &constrained),
		"Decimal unconstrained should differ from Decimal(10,2)"
	);
}

#[test]
fn test_fingerprint_decimal_same_constraint_deterministic() {
	let fields1 = vec![make_constrained_field(
		"amount",
		TypeConstraint::with_constraint(
			ValueType::Decimal,
			Constraint::PrecisionScale(Precision::new(18), Scale::new(6)),
		),
	)];
	let fields2 = vec![make_constrained_field(
		"amount",
		TypeConstraint::with_constraint(
			ValueType::Decimal,
			Constraint::PrecisionScale(Precision::new(18), Scale::new(6)),
		),
	)];

	assert_eq!(
		compute_fingerprint(RowFamily::Deprecated, &fields1),
		compute_fingerprint(RowFamily::Deprecated, &fields2),
		"Decimal(18,6) should produce same fingerprint"
	);
}

#[test]
fn test_fingerprint_decimal_different_precision() {
	let low_precision = vec![make_constrained_field(
		"amount",
		TypeConstraint::with_constraint(
			ValueType::Decimal,
			Constraint::PrecisionScale(Precision::new(5), Scale::new(2)),
		),
	)];
	let high_precision = vec![make_constrained_field(
		"amount",
		TypeConstraint::with_constraint(
			ValueType::Decimal,
			Constraint::PrecisionScale(Precision::new(38), Scale::new(2)),
		),
	)];

	assert_ne!(
		compute_fingerprint(RowFamily::Deprecated, &low_precision),
		compute_fingerprint(RowFamily::Deprecated, &high_precision),
		"Decimal(5,2) should differ from Decimal(38,2)"
	);
}

#[test]
fn test_fingerprint_decimal_different_scale() {
	let low_scale = vec![make_constrained_field(
		"amount",
		TypeConstraint::with_constraint(
			ValueType::Decimal,
			Constraint::PrecisionScale(Precision::new(10), Scale::new(0)),
		),
	)];
	let high_scale = vec![make_constrained_field(
		"amount",
		TypeConstraint::with_constraint(
			ValueType::Decimal,
			Constraint::PrecisionScale(Precision::new(10), Scale::new(8)),
		),
	)];

	assert_ne!(
		compute_fingerprint(RowFamily::Deprecated, &low_scale),
		compute_fingerprint(RowFamily::Deprecated, &high_scale),
		"Decimal(10,0) should differ from Decimal(10,8)"
	);
}

#[test]
fn test_fingerprint_decimal_different_precision_and_scale() {
	let fields1 = vec![make_constrained_field(
		"amount",
		TypeConstraint::with_constraint(
			ValueType::Decimal,
			Constraint::PrecisionScale(Precision::new(10), Scale::new(2)),
		),
	)];
	let fields2 = vec![make_constrained_field(
		"amount",
		TypeConstraint::with_constraint(
			ValueType::Decimal,
			Constraint::PrecisionScale(Precision::new(15), Scale::new(4)),
		),
	)];

	assert_ne!(
		compute_fingerprint(RowFamily::Deprecated, &fields1),
		compute_fingerprint(RowFamily::Deprecated, &fields2),
		"Decimal(10,2) should differ from Decimal(15,4)"
	);
}

#[test]
fn test_fingerprint_different_types_same_max_bytes() {
	// Same MaxBytes value but different base types should produce different fingerprints
	let utf8 = vec![make_constrained_field(
		"field",
		TypeConstraint::with_constraint(ValueType::Utf8, Constraint::MaxBytes(MaxBytes::new(100))),
	)];
	let blob = vec![make_constrained_field(
		"field",
		TypeConstraint::with_constraint(ValueType::Blob, Constraint::MaxBytes(MaxBytes::new(100))),
	)];
	let int = vec![make_constrained_field(
		"field",
		TypeConstraint::with_constraint(ValueType::Int, Constraint::MaxBytes(MaxBytes::new(100))),
	)];
	let uint = vec![make_constrained_field(
		"field",
		TypeConstraint::with_constraint(ValueType::Uint, Constraint::MaxBytes(MaxBytes::new(100))),
	)];

	let fp_utf8 = compute_fingerprint(RowFamily::Deprecated, &utf8);
	let fp_blob = compute_fingerprint(RowFamily::Deprecated, &blob);
	let fp_int = compute_fingerprint(RowFamily::Deprecated, &int);
	let fp_uint = compute_fingerprint(RowFamily::Deprecated, &uint);

	assert_ne!(fp_utf8, fp_blob, "Utf8(100) should differ from Blob(100)");
	assert_ne!(fp_utf8, fp_int, "Utf8(100) should differ from Int(100)");
	assert_ne!(fp_utf8, fp_uint, "Utf8(100) should differ from Uint(100)");
	assert_ne!(fp_blob, fp_int, "Blob(100) should differ from Int(100)");
	assert_ne!(fp_blob, fp_uint, "Blob(100) should differ from Uint(100)");
	assert_ne!(fp_int, fp_uint, "Int(100) should differ from Uint(100)");
}

#[test]
fn test_fingerprint_multiple_constrained_fields() {
	let fields1 = vec![
		make_constrained_field(
			"name",
			TypeConstraint::with_constraint(ValueType::Utf8, Constraint::MaxBytes(MaxBytes::new(255))),
		),
		make_constrained_field(
			"price",
			TypeConstraint::with_constraint(
				ValueType::Decimal,
				Constraint::PrecisionScale(Precision::new(10), Scale::new(2)),
			),
		),
		make_constrained_field(
			"data",
			TypeConstraint::with_constraint(ValueType::Blob, Constraint::MaxBytes(MaxBytes::new(1024))),
		),
	];

	let fields2 = vec![
		make_constrained_field(
			"name",
			TypeConstraint::with_constraint(ValueType::Utf8, Constraint::MaxBytes(MaxBytes::new(255))),
		),
		make_constrained_field(
			"price",
			TypeConstraint::with_constraint(
				ValueType::Decimal,
				Constraint::PrecisionScale(Precision::new(10), Scale::new(2)),
			),
		),
		make_constrained_field(
			"data",
			TypeConstraint::with_constraint(ValueType::Blob, Constraint::MaxBytes(MaxBytes::new(1024))),
		),
	];

	assert_eq!(
		compute_fingerprint(RowFamily::Deprecated, &fields1),
		compute_fingerprint(RowFamily::Deprecated, &fields2),
		"Identical multi-field constrained shapes should produce same fingerprint"
	);
}

#[test]
fn test_fingerprint_multiple_fields_one_constraint_differs() {
	let fields1 = vec![
		make_constrained_field(
			"name",
			TypeConstraint::with_constraint(ValueType::Utf8, Constraint::MaxBytes(MaxBytes::new(255))),
		),
		make_constrained_field(
			"price",
			TypeConstraint::with_constraint(
				ValueType::Decimal,
				Constraint::PrecisionScale(Precision::new(10), Scale::new(2)),
			),
		),
	];

	let fields2 = vec![
		make_constrained_field(
			"name",
			TypeConstraint::with_constraint(ValueType::Utf8, Constraint::MaxBytes(MaxBytes::new(255))),
		),
		make_constrained_field(
			"price",
			TypeConstraint::with_constraint(
				ValueType::Decimal,
				Constraint::PrecisionScale(Precision::new(10), Scale::new(4)), // Different scale
			),
		),
	];

	assert_ne!(
		compute_fingerprint(RowFamily::Deprecated, &fields1),
		compute_fingerprint(RowFamily::Deprecated, &fields2),
		"Shapes differing only in one constraint should have different fingerprints"
	);
}

#[test]
fn test_fingerprint_mixed_constrained_and_unconstrained() {
	let fields1 = vec![
		make_field("id", ValueType::Int8),
		make_constrained_field(
			"name",
			TypeConstraint::with_constraint(ValueType::Utf8, Constraint::MaxBytes(MaxBytes::new(100))),
		),
		make_field("active", ValueType::Boolean),
	];

	let fields2 = vec![
		make_field("id", ValueType::Int8),
		make_field("name", ValueType::Utf8), // Unconstrained
		make_field("active", ValueType::Boolean),
	];

	assert_ne!(
		compute_fingerprint(RowFamily::Deprecated, &fields1),
		compute_fingerprint(RowFamily::Deprecated, &fields2),
		"Mixed constrained/unconstrained should differ from all unconstrained"
	);
}

#[test]
fn test_fingerprint_max_bytes_edge_values() {
	let min_value = vec![make_constrained_field(
		"data",
		TypeConstraint::with_constraint(ValueType::Blob, Constraint::MaxBytes(MaxBytes::new(1))),
	)];
	let max_value = vec![make_constrained_field(
		"data",
		TypeConstraint::with_constraint(ValueType::Blob, Constraint::MaxBytes(MaxBytes::new(u32::MAX))),
	)];

	assert_ne!(
		compute_fingerprint(RowFamily::Deprecated, &min_value),
		compute_fingerprint(RowFamily::Deprecated, &max_value),
		"Blob(1) should differ from Blob(MAX)"
	);
}

#[test]
fn test_fingerprint_decimal_edge_precision_scale() {
	let min_precision = vec![make_constrained_field(
		"amount",
		TypeConstraint::with_constraint(
			ValueType::Decimal,
			Constraint::PrecisionScale(Precision::new(1), Scale::new(0)),
		),
	)];
	let max_precision = vec![make_constrained_field(
		"amount",
		TypeConstraint::with_constraint(
			ValueType::Decimal,
			Constraint::PrecisionScale(Precision::new(255), Scale::new(255)),
		),
	)];

	assert_ne!(
		compute_fingerprint(RowFamily::Deprecated, &min_precision),
		compute_fingerprint(RowFamily::Deprecated, &max_precision),
		"Decimal(1,0) should differ from Decimal(255,255)"
	);
}

#[test]
fn test_fingerprint_adjacent_max_bytes_values() {
	// Test that even adjacent values produce different fingerprints
	let value_99 = vec![make_constrained_field(
		"text",
		TypeConstraint::with_constraint(ValueType::Utf8, Constraint::MaxBytes(MaxBytes::new(99))),
	)];
	let value_100 = vec![make_constrained_field(
		"text",
		TypeConstraint::with_constraint(ValueType::Utf8, Constraint::MaxBytes(MaxBytes::new(100))),
	)];
	let value_101 = vec![make_constrained_field(
		"text",
		TypeConstraint::with_constraint(ValueType::Utf8, Constraint::MaxBytes(MaxBytes::new(101))),
	)];

	let fp_99 = compute_fingerprint(RowFamily::Deprecated, &value_99);
	let fp_100 = compute_fingerprint(RowFamily::Deprecated, &value_100);
	let fp_101 = compute_fingerprint(RowFamily::Deprecated, &value_101);

	assert_ne!(fp_99, fp_100, "Utf8(99) should differ from Utf8(100)");
	assert_ne!(fp_100, fp_101, "Utf8(100) should differ from Utf8(101)");
	assert_ne!(fp_99, fp_101, "Utf8(99) should differ from Utf8(101)");
}

#[test]
fn the_family_changes_the_fingerprint_so_two_kinds_cannot_share_a_shape() {
	// Without this the registry hands a series the table shape whenever their field lists match.
	let fields = vec![make_field("id", ValueType::Int4)];

	let table = compute_fingerprint(RowFamily::Table, &fields);
	let series = compute_fingerprint(RowFamily::Series, &fields);
	let ringbuffer = compute_fingerprint(RowFamily::RingBuffer, &fields);

	assert_ne!(table, series);
	assert_ne!(table, ringbuffer);
	assert_ne!(series, ringbuffer);
}

#[test]
fn the_same_family_and_fields_still_fingerprint_identically() {
	// Shape reuse across objects of one kind is what keeps the registry from growing per object.
	let fields = vec![make_field("id", ValueType::Int4)];

	assert_eq!(compute_fingerprint(RowFamily::Table, &fields), compute_fingerprint(RowFamily::Table, &fields));
}
