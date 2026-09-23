// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{Array, ArrayRef, BooleanArray, UInt64Array};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use arrow_select::{
	concat::concat,
	filter::{FilterBuilder, FilterPredicate},
	interleave::interleave,
	take::take,
	zip::zip,
};

use crate::util::bitmap;

pub fn predicate(mask: &BooleanBuffer, len: usize) -> FilterPredicate {
	FilterBuilder::new(&BooleanArray::new(bitmap::resize(mask, len), None)).build()
}

pub fn shared_predicate(mask: &BooleanBuffer, len: usize) -> FilterPredicate {
	FilterBuilder::new(&BooleanArray::new(bitmap::resize(mask, len), None)).optimize().build()
}

pub fn filtered<A>(array: &A, predicate: &FilterPredicate) -> A
where
	A: Array + Clone + 'static,
{
	let selected =
		predicate.filter(array).expect("filter predicate is resized to the array length before it is built");
	downcast(selected.as_ref())
}

pub fn kept_nulls<A>(source: &A, selected: &A) -> Option<NullBuffer>
where
	A: Array,
{
	match (source.nulls().is_some(), selected.nulls().is_some()) {
		(true, false) => Some(NullBuffer::new_valid(selected.len())),
		(true, true) => selected.nulls().cloned(),
		(false, _) => None,
	}
}

pub fn indices_in_range(indices: &[usize], len: usize) -> BooleanBuffer {
	BooleanBuffer::collect_bool(indices.len(), |row| indices[row] < len)
}

pub fn taken<A>(array: &A, indices: &UInt64Array) -> A
where
	A: Array + Clone + 'static,
{
	let selected = take(array, indices, None).expect("every index is clamped into range before the take");
	downcast(selected.as_ref())
}

pub fn interleaved<A>(source: &A, filler: &dyn Array, pairs: &[(usize, usize)]) -> A
where
	A: Array + Clone + 'static,
{
	downcast(picked(&[source as &dyn Array, filler], pairs).as_ref())
}

pub fn joined(arrays: &[&dyn Array]) -> ArrayRef {
	concat(arrays).expect("all arrays share one type")
}

pub fn picked(arrays: &[&dyn Array], pairs: &[(usize, usize)]) -> ArrayRef {
	interleave(arrays, pairs).expect("all arrays share one type and every pair is clamped into range")
}

pub fn merged(mask: &BooleanArray, truthy: &dyn Array, falsy: &dyn Array) -> ArrayRef {
	zip(mask, &truthy, &falsy).expect("both sides share one type and the length of the mask")
}

pub fn downcast<A>(array: &dyn Array) -> A
where
	A: Array + Clone + 'static,
{
	array.as_any().downcast_ref::<A>().expect("an arrow kernel returns the array type it was given").clone()
}
