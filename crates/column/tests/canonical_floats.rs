// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{
	ArrayRef, Float32Array, Float64Array,
	cast::AsArray,
	types::{Float32Type, Float64Type},
};
use reifydb_column::{
	persist::{deserialize_block, serialize_block},
	snapshot::{ColumnBlock, ColumnChunks},
};
use reifydb_core::value::column::data::{Column, canonical::Canonical};
use reifydb_value::value::value_type::{ValueType, field::FieldType};

fn block(ty: ValueType, array: ArrayRef) -> ColumnBlock {
	let schema = Arc::new(vec![("a".to_string(), ty.clone(), false)]);
	let canonical = Canonical::new(FieldType::from(ty.clone()), array).unwrap();
	ColumnBlock::new(schema, vec![ColumnChunks::single(ty, false, Column::from_canonical(canonical))])
}

fn round_trip(block: &ColumnBlock) -> ArrayRef {
	let restored = deserialize_block(&serialize_block(block).unwrap()).unwrap();
	restored.columns[0].chunks[0].to_canonical().unwrap().buffer().clone()
}

#[test]
fn persist_round_trip_canonicalizes_floats() {
	// Old blocks may hold -0.0 or any NaN, so the float deserialize must canonicalize or compare splits zero.
	let negative_nan_64 = f64::from_bits(f64::NAN.to_bits() | 0x8000_0000_0000_0000);
	let raw: ArrayRef = Arc::new(Float64Array::from(vec![-0.0, negative_nan_64, 1.5]));
	let array = round_trip(&block(ValueType::Float8, raw));
	let bits: Vec<u64> = array.as_primitive::<Float64Type>().values().iter().map(|v| v.to_bits()).collect();
	assert_eq!(bits, vec![0.0f64.to_bits(), f64::NAN.to_bits(), 1.5f64.to_bits()]);

	let negative_nan_32 = f32::from_bits(f32::NAN.to_bits() | 0x8000_0000);
	let raw: ArrayRef = Arc::new(Float32Array::from(vec![-0.0, negative_nan_32, 1.5]));
	let array = round_trip(&block(ValueType::Float4, raw));
	let bits: Vec<u32> = array.as_primitive::<Float32Type>().values().iter().map(|v| v.to_bits()).collect();
	assert_eq!(bits, vec![0.0f32.to_bits(), f32::NAN.to_bits(), 1.5f32.to_bits()]);
}
