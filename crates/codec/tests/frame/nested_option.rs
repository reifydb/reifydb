// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{Int32Array, LargeStringArray};
use reifydb_codec::frame::{
	decode::decode_frames,
	encode::encode_frames,
	format::{COLUMN_DESCRIPTOR_SIZE, FRAME_HEADER_SIZE, MESSAGE_HEADER_SIZE},
	options::EncodeOptions,
};
use reifydb_value::value::{Value, column_view::ColumnView, frame::frame::Frame, value_type::ValueType};

use crate::common::{ColumnData, data, frame_of, optional, view_at};

const DESCRIPTOR: usize = MESSAGE_HEADER_SIZE + FRAME_HEADER_SIZE;
const TYPE_CODE_AT: usize = DESCRIPTOR;
const FLAGS_AT: usize = DESCRIPTOR + 2;

fn option(inner: ColumnData, defined: &[bool]) -> ColumnData {
	optional(inner, defined)
}

fn int4(values: Vec<i32>) -> ColumnData {
	data(ValueType::Int4, Int32Array::from(values))
}

fn frame(input: ColumnData) -> Frame {
	frame_of(vec![("c", input)])
}

fn encode(input: ColumnData, options: &EncodeOptions) -> Vec<u8> {
	encode_frames(&[frame(input)], options).expect("encode failed")
}

fn pack(bits: &[bool]) -> Vec<u8> {
	let mut bytes = vec![0u8; bits.len().div_ceil(8)];
	for (i, _) in bits.iter().enumerate().filter(|(_, bit)| **bit) {
		bytes[i / 8] |= 1 << (i % 8);
	}
	bytes
}

fn with_outer_layer(mut bytes: Vec<u8>, outer: &[bool]) -> Vec<u8> {
	// The encoder never writes depth two, so these bytes must be spliced by hand.
	let layer = pack(outer);
	let d = DESCRIPTOR;
	let name_len = u16::from_le_bytes([bytes[d + 4], bytes[d + 5]]) as usize;
	let nones_at = d + COLUMN_DESCRIPTOR_SIZE + name_len + (4 - name_len % 4) % 4;
	bytes[d] = (2 << 6) | (bytes[d] & 0x3F);
	let nones_len = u32::from_le_bytes([bytes[d + 12], bytes[d + 13], bytes[d + 14], bytes[d + 15]]);
	bytes[d + 12..d + 16].copy_from_slice(&(nones_len + layer.len() as u32).to_le_bytes());
	let frame_size_at = MESSAGE_HEADER_SIZE + 8;
	let frame_size = u32::from_le_bytes(bytes[frame_size_at..frame_size_at + 4].try_into().unwrap());
	bytes[frame_size_at..frame_size_at + 4].copy_from_slice(&(frame_size + layer.len() as u32).to_le_bytes());
	let total_size = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
	bytes[12..16].copy_from_slice(&(total_size + layer.len() as u32).to_le_bytes());
	bytes.splice(nones_at..nones_at, layer);
	bytes
}

struct Descriptor {
	type_code: u8,
	flags: u8,
	row_count: u32,
	nones: Vec<u8>,
}

fn descriptor(bytes: &[u8]) -> Descriptor {
	let d = &bytes[DESCRIPTOR..];
	let name_len = u16::from_le_bytes([d[4], d[5]]) as usize;
	let row_count = u32::from_le_bytes([d[8], d[9], d[10], d[11]]);
	let nones_len = u32::from_le_bytes([d[12], d[13], d[14], d[15]]) as usize;
	let nones_at = COLUMN_DESCRIPTOR_SIZE + name_len + (4 - name_len % 4) % 4;
	Descriptor {
		type_code: d[0],
		flags: d[2],
		row_count,
		nones: d[nones_at..nones_at + nones_len].to_vec(),
	}
}

fn layers(view: &ColumnView<'_>) -> Vec<Vec<bool>> {
	match view.is_nullable() {
		true => vec![(0..view.len()).map(|i| !view.none_at(i)).collect()],
		false => Vec::new(),
	}
}

fn decode_single(bytes: &[u8]) -> Frame {
	let mut frames = decode_frames(bytes).expect("decode failed");
	assert_eq!(frames.len(), 1);
	frames.remove(0)
}

fn assert_depth_two_rejected(bytes: &[u8]) {
	let err = decode_frames(bytes).unwrap_err().to_string();
	assert!(err.contains("has option depth 2, but a column holds at most one option layer"), "{err}");
}

#[test]
fn option_int4_writes_one_bitmap_and_depth_one_in_the_type_code() {
	let bytes = encode(option(int4(vec![1, 0, 3]), &[true, false, true]), &EncodeOptions::none());
	let d = descriptor(&bytes);
	assert_eq!(d.type_code, 0x46, "Int4 kind 6 under option depth 1");
	assert_eq!(d.flags, 0x01, "has-nones flag");
	assert_eq!(d.row_count, 3);
	assert_eq!(d.nones, vec![0b0000_0101]);

	let decoded = decode_single(&bytes);
	let decoded = view_at(&decoded, 0);
	assert_eq!(decoded.get_type(), ValueType::Option(Box::new(ValueType::Int4)));
	assert_eq!(layers(&decoded), vec![vec![true, false, true]]);
	assert_eq!(decoded.get_value(1), Value::none_of(ValueType::Int4));
	assert_eq!(decoded.get_value(2), Value::Int4(3));
}

#[test]
fn option_option_int4_bytes_with_outer_then_inner_bitmap_are_rejected() {
	// A column holds at most one option layer, so depth-two bytes must be refused, never flattened.
	let column = option(int4(vec![0, 0, 7]), &[false, false, true]);
	let bytes = with_outer_layer(encode(column, &EncodeOptions::none()), &[false, true, true]);
	let d = descriptor(&bytes);
	assert_eq!(d.type_code, 0x86, "Int4 kind 6 under option depth 2");
	assert_eq!(d.flags, 0x01);
	assert_eq!(d.nones, vec![0b0000_0110, 0b0000_0100]);

	assert_depth_two_rejected(&bytes);
}

#[test]
fn a_depth_two_column_with_ceil_rows_over_eight_byte_layers_is_rejected() {
	// Layers of ceil(rows / 8) bytes must not shift the depth check, so the refusal holds past one byte.
	let outer: Vec<bool> = (0..9).map(|i| i != 0 && i != 8).collect();
	let inner: Vec<bool> = (0..9).map(|i| outer[i] && i % 2 == 1).collect();
	let column = option(int4((0..9).collect()), &inner);
	let bytes = with_outer_layer(encode(column, &EncodeOptions::none()), &outer);
	let d = descriptor(&bytes);
	assert_eq!(d.row_count, 9);
	assert_eq!(d.nones, vec![0xFE, 0x00, 0xAA, 0x00]);
	assert_depth_two_rejected(&bytes);
}

#[test]
fn a_fully_populated_option_column_keeps_its_option_type() {
	let bytes = encode(option(int4(vec![1, 2, 3]), &[true, true, true]), &EncodeOptions::none());
	let d = descriptor(&bytes);
	assert_eq!(d.type_code, 0x46);
	assert_eq!(d.flags, 0x01);
	assert_eq!(d.nones, vec![0b0000_0111]);
	let decoded = decode_single(&bytes);
	let decoded = view_at(&decoded, 0);
	assert_eq!(decoded.get_type(), ValueType::Option(Box::new(ValueType::Int4)));
	assert_eq!(layers(&decoded), vec![vec![true, true, true]]);
}

#[test]
fn option_option_utf8_is_rejected_under_every_encoding_choice() {
	// Dict and run-length bodies must not bypass the depth check, so every encoding choice is refused.
	let strings: Vec<String> = (0..40).map(|i| format!("v{}", i % 4)).collect();
	let outer: Vec<bool> = (0..40).map(|i| i % 5 != 0).collect();
	let inner: Vec<bool> = (0..40).map(|i| outer[i] && i % 3 != 0).collect();
	let column = option(data(ValueType::Utf8, LargeStringArray::from(strings.clone())), &inner);
	for options in [EncodeOptions::none(), EncodeOptions::default(), EncodeOptions::fast()] {
		let bytes = with_outer_layer(encode(column.clone(), &options), &outer);
		let d = descriptor(&bytes);
		assert_eq!(d.type_code, 0x89, "Utf8 kind 9 under option depth 2");
		assert_eq!(d.nones.len(), 10, "two layers of five bytes each");
		assert_depth_two_rejected(&bytes);
	}
}

#[test]
fn a_nones_length_that_disagrees_with_the_depth_is_rejected() {
	let column = option(int4(vec![0, 0, 7]), &[false, false, true]);
	let mut bytes = with_outer_layer(encode(column, &EncodeOptions::none()), &[false, true, true]);
	assert_eq!(bytes[TYPE_CODE_AT], 0x86);
	bytes[TYPE_CODE_AT] = 0x46;
	let err = decode_frames(&bytes).unwrap_err().to_string();
	assert!(err.contains("nones length 2 disagrees with option depth 1 and row count 3"), "{err}");
}

#[test]
fn an_option_column_without_the_has_nones_flag_is_rejected() {
	let mut bytes = encode(option(int4(vec![1, 0, 3]), &[true, false, true]), &EncodeOptions::none());
	assert_eq!(bytes[FLAGS_AT], 0x01);
	bytes[FLAGS_AT] = 0x00;
	let err = decode_frames(&bytes).unwrap_err().to_string();
	assert!(err.contains("option depth 1 but the has-nones flag is clear"), "{err}");
}

#[test]
fn a_has_nones_flag_on_a_plain_column_type_is_rejected() {
	let mut bytes = encode(option(int4(vec![1, 0, 3]), &[true, false, true]), &EncodeOptions::none());
	bytes[TYPE_CODE_AT] = 0x06;
	let err = decode_frames(&bytes).unwrap_err().to_string();
	assert!(err.contains("option depth 0 but the has-nones flag is set"), "{err}");
}
