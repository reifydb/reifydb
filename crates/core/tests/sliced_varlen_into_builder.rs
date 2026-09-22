// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_value::value::{Value, blob::Blob};

const START: usize = 2;

fn strings() -> Vec<String> {
	["a", "bbbb", "cc", "dddddd", "eee", "ffffffffff"].into_iter().map(String::from).collect()
}

fn blobs() -> Vec<Blob> {
	strings().into_iter().map(|s| Blob::new(s.into_bytes())).collect()
}

fn rows(buffer: &ColumnBuffer) -> Vec<Value> {
	(0..buffer.len()).map(|i| buffer.get_value(i)).collect()
}

fn utf8_values(values: &[String]) -> Vec<Value> {
	values.iter().cloned().map(Value::Utf8).collect()
}

fn blob_values(values: &[Blob]) -> Vec<Value> {
	values.iter().cloned().map(Value::Blob).collect()
}

fn patterned_blobs(n: usize) -> Vec<Blob> {
	(0..n).map(|i| Blob::new((0..(i % 9) as u8).map(|b| b.wrapping_mul(i as u8)).collect())).collect()
}

fn pushed_utf8() -> String {
	"gg".to_string()
}

fn pushed_blob() -> Blob {
	Blob::new(vec![0, 7, 0])
}

#[test]
fn utf8_slice_into_builder_while_parent_is_alive() {
	// A push on a slice past row 0 while the parent shares its buffers must never shift rows or write the parent.
	let parent = ColumnBuffer::utf8(strings());
	let len = parent.len();
	let mut builder = parent.slice(START, len).into_builder();
	builder.push_value(Value::Utf8(pushed_utf8()));
	let out = builder.finish();
	let mut expected = utf8_values(&strings()[START..]);
	expected.push(Value::Utf8(pushed_utf8()));
	assert_eq!(rows(&out), expected);
	assert_eq!(rows(&parent), utf8_values(&strings()), "the parent must stay unchanged");
}

#[test]
fn utf8_slice_into_builder_after_parent_is_dropped() {
	// Arrow into_builder on a unique slice past value offset 0 breaks the array; rows must never shift.
	let parent = ColumnBuffer::utf8(strings());
	let slice = parent.slice(START, parent.len());
	drop(parent);
	let mut builder = slice.into_builder();
	builder.push_value(Value::Utf8(pushed_utf8()));
	let out = builder.finish();
	let mut expected = utf8_values(&strings()[START..]);
	expected.push(Value::Utf8(pushed_utf8()));
	assert_eq!(rows(&out), expected);
}

#[test]
fn blob_slice_into_builder_while_parent_is_alive() {
	// A push on a slice past row 0 while the parent shares its buffers must never shift rows or write the parent.
	let parent = ColumnBuffer::blob(blobs());
	let len = parent.len();
	let mut builder = parent.slice(START, len).into_builder();
	builder.push_value(Value::Blob(pushed_blob()));
	let out = builder.finish();
	let mut expected = blob_values(&blobs()[START..]);
	expected.push(Value::Blob(pushed_blob()));
	assert_eq!(rows(&out), expected);
	assert_eq!(rows(&parent), blob_values(&blobs()), "the parent must stay unchanged");
}

#[test]
fn blob_push_on_a_mid_slice_never_writes_the_next_parent_row() {
	// A push through a clone of a mid slice must copy first, otherwise it overwrites the parent row after the view.
	let parent = ColumnBuffer::blob(patterned_blobs(500));
	let s = parent.slice(100, 350);
	let mut w = s.clone().into_builder();
	w.push_value(Value::Blob(Blob::new(vec![0xAA, 0xBB])));
	let w = w.finish();
	assert_eq!(w.get_value(250), Value::Blob(Blob::new(vec![0xAA, 0xBB])));
	assert_eq!(s.len(), 250, "a push through a clone of the slice must never change the slice");
	assert_eq!(
		rows(&s),
		blob_values(&patterned_blobs(500)[100..350]),
		"a push through a clone of the slice must never change the slice"
	);
	assert_eq!(
		parent.get_value(350),
		Value::Blob(patterned_blobs(500)[350].clone()),
		"a slice push must never write the parent row"
	);
	assert_eq!(rows(&parent), blob_values(&patterned_blobs(500)), "a slice push must never write the parent row");
}

#[test]
fn blob_slice_into_builder_after_parent_is_dropped() {
	// Arrow into_builder on a unique slice past value offset 0 breaks the array; rows must never shift.
	let parent = ColumnBuffer::blob(blobs());
	let slice = parent.slice(START, parent.len());
	drop(parent);
	let mut builder = slice.into_builder();
	builder.push_value(Value::Blob(pushed_blob()));
	let out = builder.finish();
	let mut expected = blob_values(&blobs()[START..]);
	expected.push(Value::Blob(pushed_blob()));
	assert_eq!(rows(&out), expected);
}
