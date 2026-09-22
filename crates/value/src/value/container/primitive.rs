// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::result::Result as StdResult;

use arrow_array::{Array, ArrowPrimitiveType, PrimitiveArray};
use arrow_buffer::{BooleanBuffer, NullBuffer, ScalarBuffer};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
	util::bitmap,
	value::{Value, is::IsNumber, to_value::ToValue},
};

pub fn serialize<A, Ser>(array: &PrimitiveArray<A>, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error>
where
	A: ArrowPrimitiveType,
	A::Native: Serialize,
	Ser: Serializer,
{
	#[derive(Serialize)]
	struct Helper<'a, T: Serialize> {
		data: &'a [T],
	}
	Helper {
		data: array.values(),
	}
	.serialize(serializer)
}

pub fn deserialize<'de, A, D>(deserializer: D) -> StdResult<PrimitiveArray<A>, D::Error>
where
	A: ArrowPrimitiveType,
	A::Native: Deserialize<'de>,
	D: Deserializer<'de>,
{
	#[derive(Deserialize)]
	struct Helper<T> {
		data: Vec<T>,
	}
	let h = Helper::<A::Native>::deserialize(deserializer)?;
	Ok(PrimitiveArray::new(ScalarBuffer::from(h.data), None))
}

pub fn get_value<A>(array: &PrimitiveArray<A>, index: usize) -> Value
where
	A: ArrowPrimitiveType,
	A::Native: IsNumber,
{
	if index < array.len() {
		array.value(index).to_value()
	} else {
		Value::none()
	}
}

pub fn as_string<A>(array: &PrimitiveArray<A>, index: usize) -> String
where
	A: ArrowPrimitiveType,
	A::Native: IsNumber,
{
	if index < array.len() {
		array.value(index).to_string()
	} else {
		"none".to_string()
	}
}

pub fn slice<A>(array: &PrimitiveArray<A>, start: usize, end: usize) -> PrimitiveArray<A>
where
	A: ArrowPrimitiveType,
{
	let end = end.min(array.len());
	let start = start.min(end);
	array.slice(start, end - start)
}

pub fn take<A>(array: &PrimitiveArray<A>, num: usize) -> PrimitiveArray<A>
where
	A: ArrowPrimitiveType,
{
	slice(array, 0, num)
}

pub fn filter<A>(array: &PrimitiveArray<A>, mask: &BooleanBuffer) -> PrimitiveArray<A>
where
	A: ArrowPrimitiveType,
{
	let values = array.values();
	let mut kept = Vec::with_capacity(mask.count_set_bits());
	for (i, keep) in mask.iter().enumerate() {
		if keep && i < values.len() {
			kept.push(values[i]);
		}
	}
	PrimitiveArray::new(ScalarBuffer::from(kept), bitmap::filter_nulls(array.nulls(), mask))
		.with_data_type(array.data_type().clone())
}

pub fn reorder<A>(array: &PrimitiveArray<A>, indices: &[usize]) -> PrimitiveArray<A>
where
	A: ArrowPrimitiveType,
{
	let values = array.values();
	let reordered: Vec<A::Native> =
		indices.iter().map(|&idx| values.get(idx).copied().unwrap_or_default()).collect();
	PrimitiveArray::new(ScalarBuffer::from(reordered), bitmap::reorder_nulls(array.nulls(), indices))
		.with_data_type(array.data_type().clone())
}

pub fn attach_nulls<A>(array: PrimitiveArray<A>, nulls: Option<NullBuffer>) -> PrimitiveArray<A>
where
	A: ArrowPrimitiveType,
{
	bitmap::assert_nulls_len(nulls.as_ref(), array.len());
	let (data_type, values, _) = array.into_parts();
	PrimitiveArray::new(values, nulls).with_data_type(data_type)
}

pub fn capacity<A>(array: &PrimitiveArray<A>) -> usize
where
	A: ArrowPrimitiveType,
{
	let buffer = array.values().inner();
	if buffer.strong_count() == 1 {
		buffer.capacity() / size_of::<A::Native>()
	} else {
		array.len()
	}
}

pub fn heap_size<A>(array: &PrimitiveArray<A>) -> usize
where
	A: ArrowPrimitiveType,
{
	capacity(array) * size_of::<A::Native>()
}
