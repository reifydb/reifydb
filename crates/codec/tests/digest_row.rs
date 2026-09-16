// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	panic::{AssertUnwindSafe, catch_unwind},
	slice::from_ref,
};

use reifydb_codec::{
	constraint::{EncodedTypeConstraint, decode_type_constraint, encode_type_constraint},
	row::shape::{RowFamily, RowShape, RowShapeField, fingerprint::compute_fingerprint},
	tag::ValueKind,
};
use reifydb_value::value::{
	Value,
	constraint::{Constraint, TypeConstraint, bytes::MaxBytes},
	digest::Digest,
	duration::Duration,
	value_type::ValueType,
};

fn digest_type(inner: ValueType, accuracy: u32) -> ValueType {
	ValueType::Digest {
		inner: Box::new(inner),
		accuracy,
	}
}

fn option(ty: ValueType) -> ValueType {
	ValueType::Option(Box::new(ty))
}

fn digest_of(inner: ValueType, accuracy: u32, values: impl IntoIterator<Item = Value>) -> Digest {
	let mut digest = Digest::new(inner, accuracy).unwrap();
	for value in values {
		digest.add_value(&value).unwrap();
	}
	digest
}

fn spread_values(inner: &ValueType, count: i32) -> Vec<Value> {
	(0..count)
		.map(|i| {
			let sign = if i % 2 == 0 {
				1.0
			} else {
				-1.0
			};
			let magnitude = 1.03f64.powi(i % 400);
			match inner {
				ValueType::Float8 => Value::float8(sign * magnitude),
				ValueType::Int4 => Value::Int4((sign * magnitude * 1_000.0) as i32),
				ValueType::Duration => Value::Duration(
					Duration::from_milliseconds((sign * magnitude * 10.0) as i64).unwrap(),
				),
				other => panic!("no spread for {other}"),
			}
		})
		.collect()
}

fn shape_of(types: &[ValueType]) -> RowShape {
	RowShape::new(
		RowFamily::Table,
		types.iter()
			.enumerate()
			.map(|(i, ty)| RowShapeField::unconstrained(format!("c{i}"), ty.clone()))
			.collect(),
	)
}

fn write_row(shape: &RowShape, values: &[Value]) -> Vec<u8> {
	let mut row = shape.allocate_table();
	shape.set_values(&mut row, values);
	row.to_vec()
}

fn panic_message(run: impl FnOnce()) -> String {
	let payload = catch_unwind(AssertUnwindSafe(run)).expect_err("the read was expected to panic");
	match (payload.downcast_ref::<String>(), payload.downcast_ref::<&str>()) {
		(Some(message), _) => message.clone(),
		(None, Some(message)) => message.to_string(),
		(None, None) => panic!("panic payload is not a string"),
	}
}

fn digest_slot(shape: &RowShape, row: &[u8], index: usize) -> (usize, usize) {
	let at = shape.fields()[index].offset as usize;
	let offset = u32::from_le_bytes(row[at..at + 4].try_into().unwrap()) as usize;
	let length = u32::from_le_bytes(row[at + 4..at + 8].try_into().unwrap()) as usize;
	(shape.dynamic_section_start() + offset, length)
}

#[test]
fn digest_fields_round_trip_for_each_inner_type_accuracy_and_size() {
	// Every stored digest must read back equal, so a percentile from a table matches the one that was written.
	for inner in [ValueType::Float8, ValueType::Int4, ValueType::Duration] {
		for accuracy in [1_000, 10_000, 12_345, 100_000] {
			let shape = shape_of(&[digest_type(inner.clone(), accuracy)]);
			let empty = Digest::new(inner.clone(), accuracy).unwrap();
			let small = digest_of(inner.clone(), accuracy, spread_values(&inner, 3));
			let large = digest_of(inner.clone(), accuracy, spread_values(&inner, 4_000));
			assert!(
				large.bucket_count() > 50,
				"{inner} at {accuracy} ppm built only {} buckets",
				large.bucket_count()
			);
			for digest in [empty, small, large] {
				let value = Value::Digest(Box::new(digest));
				let row = write_row(&shape, from_ref(&value));
				assert_eq!(shape.get_value(&row, 0), value, "{inner} at {accuracy} ppm");
			}
		}
	}
}

#[test]
fn a_digest_field_stores_the_canonical_bytes_behind_an_offset_and_length_slot() {
	// Without the canonical bytes verbatim, stored digests would depend on a second encoding that is not versioned.
	let digest = digest_of(ValueType::Float8, 10_000, [1.0, 2.0, 2.0, -3.5].map(Value::float8));
	let shape = shape_of(&[ValueType::Int4, digest_type(ValueType::Float8, 10_000)]);
	let row = write_row(&shape, &[Value::Int4(7), Value::Digest(Box::new(digest.clone()))]);

	assert_eq!(shape.fields()[1].size, 8, "a digest slot must be the 8-byte offset and length pair");
	let (start, length) = digest_slot(&shape, &row, 1);
	assert_eq!(start, shape.dynamic_section_start(), "the only dynamic field must start the dynamic section");
	assert_eq!(&row[start..start + length], digest.encode().as_slice());
	assert_eq!(row.len(), shape.total_static_size() + digest.encode().len());
}

#[test]
fn two_digest_columns_next_to_fixed_size_columns_survive_every_rewrite() {
	// A digest missing from the dynamic reference arms leaves a stale offset when a neighbour grows or shrinks.
	let first_type = digest_type(ValueType::Float8, 10_000);
	let second_type = option(digest_type(ValueType::Duration, 50_000));
	let shape = shape_of(&[
		ValueType::Int8,
		first_type,
		ValueType::Utf8,
		ValueType::Float8,
		second_type,
		ValueType::Boolean,
	]);
	let first = digest_of(ValueType::Float8, 10_000, spread_values(&ValueType::Float8, 50));
	let second = digest_of(ValueType::Duration, 50_000, spread_values(&ValueType::Duration, 20));
	let values = vec![
		Value::Int8(-9),
		Value::Digest(Box::new(first)),
		Value::Utf8("between".to_string()),
		Value::float8(2.5),
		Value::Digest(Box::new(second.clone())),
		Value::Boolean(true),
	];
	let mut row = shape.allocate_table();
	shape.set_values(&mut row, &values);
	let read_all = |bytes: &[u8]| (0..shape.field_count()).map(|i| shape.get_value(bytes, i)).collect::<Vec<_>>();
	assert_eq!(read_all(&row), values);

	let mut expected = values.clone();
	let grown = digest_of(ValueType::Float8, 10_000, spread_values(&ValueType::Float8, 3_000));
	expected[1] = Value::Digest(Box::new(grown));
	shape.set_value(&mut row, 1, &expected[1]);
	assert_eq!(read_all(&row), expected, "growing the first digest moved its neighbours");

	expected[1] = Value::Digest(Box::new(Digest::new(ValueType::Float8, 10_000).unwrap()));
	shape.set_value(&mut row, 1, &expected[1]);
	assert_eq!(read_all(&row), expected, "shrinking the first digest moved its neighbours");

	expected[2] = Value::Utf8("a much longer string between the two digests".to_string());
	shape.set_value(&mut row, 2, &expected[2]);
	assert_eq!(read_all(&row), expected, "growing the string lost a digest");

	shape.set_value(&mut row, 1, &Value::none());
	assert!(matches!(shape.get_value(&row, 1), Value::None { .. }));
	assert_eq!(shape.get_value(&row, 4), Value::Digest(Box::new(second.clone())));
	assert_eq!(shape.get_value(&row, 2), expected[2]);

	shape.set_value(&mut row, 4, &Value::none_of(digest_type(ValueType::Duration, 50_000)));
	assert!(matches!(shape.get_value(&row, 4), Value::None { .. }));
	assert_eq!(shape.get_value(&row, 2), expected[2]);
	assert_eq!(row.len(), shape.total_static_size() + "a much longer string between the two digests".len());
}

#[test]
fn a_none_digest_reads_back_as_none_and_holds_no_dynamic_bytes() {
	let shape = shape_of(&[option(digest_type(ValueType::Int4, 1_000)), ValueType::Int4]);
	let row = write_row(&shape, &[Value::none_of(digest_type(ValueType::Int4, 1_000)), Value::Int4(3)]);
	assert!(matches!(shape.get_value(&row, 0), Value::None { .. }));
	assert_eq!(shape.get_value(&row, 1), Value::Int4(3));
	assert_eq!(row.len(), shape.total_static_size(), "a none digest must not leave bytes behind");
}

#[test]
fn a_corrupt_digest_payload_panics_naming_the_column() {
	// A silently skipped digest would drop every value it holds from the percentile.
	let digest = digest_of(ValueType::Float8, 10_000, [1.0, 2.0].map(Value::float8));
	let shape = shape_of(&[ValueType::Int4, digest_type(ValueType::Float8, 10_000)]);
	let row = write_row(&shape, &[Value::Int4(1), Value::Digest(Box::new(digest))]);
	let (start, length) = digest_slot(&shape, &row, 1);

	let mut unknown_version = row.clone();
	unknown_version[start] = 9;
	let message = panic_message(|| {
		shape.get_value(&unknown_version, 1);
	});
	assert!(message.contains("corrupt digest in column \"c1\""), "{message}");
	assert!(message.contains("version 9"), "{message}");

	let mut truncated = row.clone();
	truncated.truncate(start + length - 1);
	let at = shape.fields()[1].offset as usize;
	truncated[at + 4..at + 8].copy_from_slice(&((length - 1) as u32).to_le_bytes());
	let message = panic_message(|| {
		shape.get_value(&truncated, 1);
	});
	assert!(message.contains("corrupt digest in column \"c1\""), "{message}");

	let mut out_of_row = row.clone();
	out_of_row[at + 4..at + 8].copy_from_slice(&((length + 100) as u32).to_le_bytes());
	let message = panic_message(|| {
		shape.get_value(&out_of_row, 1);
	});
	assert!(message.contains("corrupt digest in column \"c1\""), "{message}");
}

#[test]
fn a_stored_digest_of_another_accuracy_or_inner_type_panics_naming_the_column() {
	// A digest read under the wrong accuracy would merge into another digest and return wrong percentiles.
	let declared = shape_of(&[digest_type(ValueType::Float8, 10_000)]);
	for (inner, accuracy) in [(ValueType::Float8, 20_000), (ValueType::Int4, 10_000)] {
		let writer = shape_of(&[digest_type(inner.clone(), accuracy)]);
		let digest = digest_of(inner.clone(), accuracy, spread_values(&inner, 4));
		let row = write_row(&writer, &[Value::Digest(Box::new(digest))]);
		let message = panic_message(|| {
			declared.get_value(&row, 0);
		});
		assert!(message.contains("column \"c0\""), "{message}");
		assert!(message.contains(&digest_type(inner.clone(), accuracy).to_string()), "{message}");
	}
}

#[test]
fn writing_a_digest_of_another_type_into_a_digest_field_panics_naming_the_column() {
	// Writing it would store bytes that every later read rejects as corrupt.
	let shape = shape_of(&[ValueType::Int4, digest_type(ValueType::Duration, 10_000)]);
	let wrong = digest_of(ValueType::Duration, 20_000, spread_values(&ValueType::Duration, 2));
	let message = panic_message(|| {
		write_row(&shape, &[Value::Int4(1), Value::Digest(Box::new(wrong))]);
	});
	assert!(message.contains("column \"c1\""), "{message}");
	assert!(message.contains("Digest(Duration, 0.02)"), "{message}");
}

#[test]
fn a_digest_constraint_stores_accuracy_ppm_in_p1_and_the_inner_type_tag_in_p2() {
	// The persisted row shape is the only place a table's digest parameters survive a restart.
	for (inner, accuracy) in [(ValueType::Float8, 10_000), (ValueType::Int4, 1_000), (ValueType::Duration, 100_000)]
	{
		for depth in 0..=3u8 {
			let ty = (0..depth).fold(digest_type(inner.clone(), accuracy), |ty, _| option(ty));
			let encoded = encode_type_constraint(&TypeConstraint::unconstrained(ty.clone())).unwrap();
			assert_eq!(
				encoded,
				EncodedTypeConstraint {
					base_type: (depth << 6) | ValueKind::Digest.byte(),
					constraint_type: 5,
					constraint_param1: accuracy,
					constraint_param2: u32::from(ValueKind::of_type(&inner).byte()),
				}
			);
			assert_eq!(decode_type_constraint(&encoded).unwrap(), TypeConstraint::unconstrained(ty));
		}
	}
}

#[test]
fn a_digest_constraint_rejects_parameters_that_are_not_a_valid_digest_type() {
	let valid =
		encode_type_constraint(&TypeConstraint::unconstrained(digest_type(ValueType::Float8, 10_000))).unwrap();
	let broken = [
		EncodedTypeConstraint {
			constraint_param2: u32::from(ValueKind::Utf8.byte()),
			..valid
		},
		EncodedTypeConstraint {
			constraint_param2: 0x100 | u32::from(ValueKind::Float8.byte()),
			..valid
		},
		EncodedTypeConstraint {
			constraint_param2: u32::from((1 << 6) | ValueKind::Float8.byte()),
			..valid
		},
		EncodedTypeConstraint {
			constraint_param1: 999,
			..valid
		},
		EncodedTypeConstraint {
			constraint_param1: 100_001,
			..valid
		},
		EncodedTypeConstraint {
			base_type: ValueKind::Float8.byte(),
			..valid
		},
		EncodedTypeConstraint {
			constraint_type: 0,
			..valid
		},
	];
	for encoded in broken {
		assert!(decode_type_constraint(&encoded).is_err(), "{encoded:?} decoded as a digest type");
	}

	assert!(encode_type_constraint(&TypeConstraint::unconstrained(digest_type(ValueType::Utf8, 10_000))).is_err());
	assert!(encode_type_constraint(&TypeConstraint::unconstrained(digest_type(ValueType::Float8, 999))).is_err());
	let with_bytes = TypeConstraint::with_constraint(
		digest_type(ValueType::Float8, 10_000),
		Constraint::MaxBytes(MaxBytes::new(16)),
	);
	assert!(encode_type_constraint(&with_bytes).is_err(), "p1 and p2 cannot hold a digest and a byte limit");
}

#[test]
fn a_digest_field_fingerprint_is_stable_and_tracks_inner_type_and_accuracy() {
	// Two digest types sharing a fingerprint would reuse one stored shape and misread each other's rows.
	let fingerprint =
		|ty: ValueType| compute_fingerprint(RowFamily::Table, &[RowShapeField::unconstrained("d", ty)]);
	let base = fingerprint(digest_type(ValueType::Float8, 10_000));
	assert_eq!(base, fingerprint(digest_type(ValueType::Float8, 10_000)));
	for other in [
		digest_type(ValueType::Float8, 10_001),
		digest_type(ValueType::Float4, 10_000),
		digest_type(ValueType::Int4, 10_000),
		option(digest_type(ValueType::Float8, 10_000)),
		ValueType::Blob,
	] {
		assert_ne!(base, fingerprint(other.clone()), "{other} shares a fingerprint with Digest(Float8, 0.01)");
	}
}
