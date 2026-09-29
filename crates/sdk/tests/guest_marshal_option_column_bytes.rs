// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[path = "extern_c/common.rs"]
mod common;

use std::fmt::{Display, Write as _};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::FieldRef;
use common::{round_trip_column, slice};
use reifydb_core::value::{
	batch::batch,
	column::{builder::ColumnBuilder, factory},
};
use reifydb_sdk::{
	common::extern_wasm::marshal::{marshal_columns_to_bytes, unmarshal_columns_from_bytes},
	flow::operator::{change::BorrowedColumns, extern_c::binding::arena::Arena},
};
use reifydb_value::value::{
	Value,
	column_view::ColumnView,
	system_columns::{row_numbers, user_columns},
	value_type::ValueType,
};

struct Pin {
	name: &'static str,
	wasm: &'static str,
	extern_c: &'static str,
}

const PINS: &[Pin] = &[
	Pin {
		name: "option_int4_with_nones",
		wasm: "030000000100000000000000000000003800000001000000060000030000003a0000000c00000039000000010000000000000000000000016305010000000000000003000000",
		extern_c: "rows 3 columns 1 name c kind 06 column rows 3 data 010000000000000003000000 bitvec 05 offsets []",
	},
	Pin {
		name: "option_int4_with_a_placeholder_under_none",
		wasm: "030000000100000000000000000000003800000001000000060000030000003a0000000c00000039000000010000000000000000000000016305010000006300000003000000",
		extern_c: "rows 3 columns 1 name c kind 06 column rows 3 data 010000006300000003000000 bitvec 05 offsets []",
	},
	Pin {
		name: "option_int4_zero_nones",
		wasm: "030000000100000000000000000000003800000001000000060000030000003a0000000c00000039000000010000000000000000000000016307010000000200000003000000",
		extern_c: "rows 3 columns 1 name c kind 06 column rows 3 data 010000000200000003000000 bitvec 07 offsets []",
	},
	Pin {
		name: "option_int4_sliced_at_a_bit_offset",
		wasm: "090000000100000000000000000000003800000001000000060000090000003b00000024000000390000000200000000000000000000000163b60100000000280000003200000000000000460000005000000000000000640000006e000000",
		extern_c: "rows 9 columns 1 name c kind 06 column rows 9 data 00000000280000003200000000000000460000005000000000000000640000006e000000 bitvec b601 offsets []",
	},
	Pin {
		name: "bare_int4",
		wasm: "030000000100000000000000000000003800000001000000060000030000003a0000000c00000039000000010000000000000000000000006307010000000200000003000000",
		extern_c: "rows 3 columns 1 name c kind 06 column rows 3 data 010000000200000003000000 bitvec  offsets []",
	},
];

trait Unmarshalled {
	fn outcome(self) -> Result<RecordBatch, String>;
}

impl Unmarshalled for RecordBatch {
	fn outcome(self) -> Result<RecordBatch, String> {
		Ok(self)
	}
}

impl<E: Display> Unmarshalled for Result<RecordBatch, E> {
	fn outcome(self) -> Result<RecordBatch, String> {
		self.map_err(|error| error.to_string())
	}
}

fn option_int4() -> ValueType {
	ValueType::Option(Box::new(ValueType::Int4))
}

fn option_int4_zero_nones() -> (FieldRef, ArrayRef) {
	let mut builder = ColumnBuilder::with_capacity(option_int4(), 3);
	for value in [1, 2, 3] {
		builder.push_typed(Value::Int4(value), &option_int4()).unwrap();
	}
	let buffer = builder.finish("c");
	assert_eq!(
		ColumnView::try_from(&buffer).unwrap().get_type(),
		option_int4(),
		"the zero-none fixture must be nullable"
	);
	buffer
}

fn option_int4_sliced_at_a_bit_offset() -> (FieldRef, ArrayRef) {
	let rows = (0..16).map(|i| (!(3..12).contains(&i) || i % 3 != 0).then_some(i * 10));
	slice(factory::int4_optional("c", rows), 3, 12)
}

type Column = (FieldRef, ArrayRef);

fn fixtures() -> Vec<(&'static str, Column, Column)> {
	let with_nones = factory::int4_optional("c", [Some(1), None, Some(3)]);
	let placeholder = factory::int4_with_bitvec("c", [1, 99, 3], vec![true, false, true]);
	let sliced = option_int4_sliced_at_a_bit_offset();
	let bare = factory::int4("c", [1, 2, 3]);
	vec![
		("option_int4_with_nones", with_nones.clone(), with_nones),
		("option_int4_with_a_placeholder_under_none", placeholder.clone(), placeholder),
		("option_int4_zero_nones", option_int4_zero_nones(), option_int4_zero_nones()),
		("option_int4_sliced_at_a_bit_offset", sliced.clone(), sliced),
		("bare_int4", bare.clone(), bare),
	]
}

fn columns(buffer: (FieldRef, ArrayRef)) -> RecordBatch {
	batch(vec![buffer]).unwrap()
}

fn marshal(batch: &RecordBatch) -> Vec<u8> {
	let columns: Vec<(FieldRef, ArrayRef)> =
		user_columns(batch).map(|(field, array)| (field.clone(), array.clone())).collect();
	marshal_columns_to_bytes(&columns, batch.num_rows(), row_numbers(batch).unwrap()).unwrap()
}

fn same(actual: &(FieldRef, ArrayRef), expected: &(FieldRef, ArrayRef)) -> bool {
	let (actual, expected) = (ColumnView::try_from(actual).unwrap(), ColumnView::try_from(expected).unwrap());
	actual.get_type() == expected.get_type()
		&& actual.len() == expected.len()
		&& (0..actual.len()).all(|row| actual.is_defined(row) == expected.is_defined(row))
		&& actual.as_slice::<i32>() == expected.as_slice::<i32>()
}

fn hex(bytes: &[u8]) -> String {
	let mut out = String::with_capacity(bytes.len() * 2);
	for byte in bytes {
		write!(out, "{byte:02x}").unwrap();
	}
	out
}

fn unhex(text: &str) -> Vec<u8> {
	(0..text.len()).step_by(2).map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap()).collect()
}

fn extern_c_parts(buffer: (FieldRef, ArrayRef)) -> String {
	let columns = columns(buffer);
	let mut arena = Arena::new();
	let ffi = arena.marshal_columns(&columns).unwrap();
	// SAFETY: `ffi` points into `arena` and `columns`, and both outlive every read below.
	let borrowed = unsafe { BorrowedColumns::from_extern_c(&ffi) };
	let column = borrowed.columns().next().expect("one column was marshalled");
	format!(
		"rows {} columns {} name {} kind {:02x} column rows {} data {} bitvec {} offsets {:?}",
		borrowed.row_count(),
		borrowed.column_count(),
		column.name(),
		column.type_code().byte(),
		column.row_count(),
		hex(column.data_bytes()),
		hex(column.defined_bitvec()),
		column.offsets()
	)
}

fn only_column(columns: &RecordBatch) -> (FieldRef, ArrayRef) {
	let (field, array) = user_columns(columns).next().expect("one column was unmarshalled");
	(field.clone(), array.clone())
}

fn pin(name: &str) -> &'static Pin {
	PINS.iter().find(|pin| pin.name == name).unwrap_or_else(|| panic!("no pin for fixture {name}"))
}

fn report(mismatches: Vec<String>) {
	assert!(mismatches.is_empty(), "guest marshal output drifted from the pins:\n{}", mismatches.join("\n"));
}

#[test]
fn every_fixture_has_exactly_one_pin() {
	// A fixture that loses its pin would stop guarding its guest bytes without any test going red.
	let fixture_names: Vec<&str> = fixtures().iter().map(|(name, _, _)| *name).collect();
	let pin_names: Vec<&str> = PINS.iter().map(|pin| pin.name).collect();
	assert_eq!(fixture_names, pin_names);
}

#[test]
fn wasm_marshal_writes_the_pinned_bytes() {
	// Wasm guests read these offsets and bitmaps as is, so every byte must stay exactly as pinned.
	let mismatches = fixtures()
		.into_iter()
		.filter_map(|(name, input, _)| {
			let actual = hex(&marshal(&columns(input)));
			(actual != pin(name).wasm).then(|| format!("{name} wasm: \"{actual}\""))
		})
		.collect();
	report(mismatches);
}

#[test]
fn extern_c_marshal_hands_the_guest_the_pinned_bytes() {
	// Native guests must get the defined bitvec exactly when nullable, packed from row 0, zero tail.
	let mismatches = fixtures()
		.into_iter()
		.filter_map(|(name, input, _)| {
			let actual = extern_c_parts(input);
			(actual != pin(name).extern_c).then(|| format!("{name} extern_c: \"{actual}\""))
		})
		.collect();
	report(mismatches);
}

#[test]
fn pinned_wasm_bytes_unmarshal_to_the_expected_column() {
	// Guest bytes must come back with the same nones and placeholders, nullable exactly when flagged optional.
	let mismatches = fixtures()
		.into_iter()
		.filter_map(|(name, _, expected)| {
			match unmarshal_columns_from_bytes(&unhex(pin(name).wasm)).outcome() {
				Err(err) => Some(format!("{name}: unmarshal failed: {err}")),
				Ok(columns) if user_columns(&columns).count() != 1 => {
					Some(format!("{name}: unmarshalled {} columns", user_columns(&columns).count()))
				}
				Ok(columns) => {
					let actual = only_column(&columns);
					(!same(&actual, &expected)).then(|| {
						format!("{name}: unmarshalled {actual:?}, expected {expected:?}")
					})
				}
			}
		})
		.collect();
	report(mismatches);
}

#[test]
fn unmarshalled_wasm_columns_re_marshal_to_the_pinned_bytes() {
	// Re-marshalling an unmarshalled column must give exactly the pinned bytes, never other rows.
	let mismatches = fixtures()
		.into_iter()
		.filter_map(|(name, _, _)| {
			let pinned = pin(name).wasm;
			match unmarshal_columns_from_bytes(&unhex(pinned)).outcome() {
				Err(err) => Some(format!("{name}: unmarshal failed: {err}")),
				Ok(columns) => {
					let actual = hex(&marshal(&columns));
					(actual != pinned).then(|| format!("{name} re-marshalled: \"{actual}\""))
				}
			}
		})
		.collect();
	report(mismatches);
}

#[test]
fn extern_c_round_trip_returns_the_same_column() {
	// A native guest echoing its input must hand back the same type, nones and placeholders, even with zero nones.
	let mismatches: Vec<String> = fixtures()
		.into_iter()
		.filter_map(|(name, input, _)| {
			let output = round_trip_column("c", input.clone());
			(!same(&output, &input))
				.then(|| format!("{name}: round trip gave {output:?}, expected {input:?}"))
		})
		.collect();
	report(mismatches);
}

#[test]
fn an_option_column_with_no_nones_comes_back_option_through_wasm() {
	// Without the optional flag a guest echo turns Option(int4) into int4 whenever no row is none.
	let input = option_int4_zero_nones();
	let batch = unmarshal_columns_from_bytes(&marshal(&columns(input.clone()))).unwrap();
	let output = only_column(&batch);
	assert_eq!(ColumnView::try_from(&output).unwrap().get_type(), option_int4());
	assert!(same(&output, &input), "wasm round trip gave {output:?}, expected {input:?}");
}
