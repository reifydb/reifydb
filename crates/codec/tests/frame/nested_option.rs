// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::frame::{
	decode::decode_frames,
	encode::encode_frames,
	format::{COLUMN_DESCRIPTOR_SIZE, FRAME_HEADER_SIZE, MESSAGE_HEADER_SIZE},
	options::EncodeOptions,
};
use reifydb_value::{
	util::bitvec::BitVec,
	value::{
		Value,
		container::{number::NumberContainer, utf8::Utf8Container},
		frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
		value_type::ValueType,
	},
};

const DESCRIPTOR: usize = MESSAGE_HEADER_SIZE + FRAME_HEADER_SIZE;
const TYPE_CODE_AT: usize = DESCRIPTOR;
const FLAGS_AT: usize = DESCRIPTOR + 2;

fn option(inner: FrameColumnData, defined: &[bool]) -> FrameColumnData {
	FrameColumnData::Option {
		inner: Box::new(inner),
		bitvec: BitVec::from_slice(defined),
	}
}

fn int4(values: Vec<i32>) -> FrameColumnData {
	FrameColumnData::Int4(NumberContainer::new(values))
}

fn frame(data: FrameColumnData) -> Frame {
	Frame::new(vec![FrameColumn {
		name: "c".to_string(),
		data,
	}])
}

fn encode(data: FrameColumnData, options: &EncodeOptions) -> Vec<u8> {
	encode_frames(&[frame(data)], options).expect("encode failed")
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

fn layers(data: &FrameColumnData) -> Vec<Vec<bool>> {
	let mut out = Vec::new();
	let mut cur = data;
	while let FrameColumnData::Option {
		inner,
		bitvec,
	} = cur
	{
		out.push(bitvec.to_vec());
		cur = inner;
	}
	out
}

fn decode_single(bytes: &[u8]) -> FrameColumnData {
	let mut frames = decode_frames(bytes).expect("decode failed");
	assert_eq!(frames.len(), 1);
	frames.remove(0).columns.remove(0).data
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
	assert_eq!(decoded.get_type(), ValueType::Option(Box::new(ValueType::Int4)));
	assert_eq!(layers(&decoded), vec![vec![true, false, true]]);
	assert_eq!(decoded.get_value(1), Value::none_of(ValueType::Int4));
	assert_eq!(decoded.get_value(2), Value::Int4(3));
}

#[test]
fn option_option_int4_writes_outer_then_inner_bitmap_and_depth_two() {
	let column = option(option(int4(vec![0, 0, 7]), &[false, false, true]), &[false, true, true]);
	let bytes = encode(column, &EncodeOptions::none());
	let d = descriptor(&bytes);
	assert_eq!(d.type_code, 0x86, "Int4 kind 6 under option depth 2");
	assert_eq!(d.flags, 0x01);
	assert_eq!(d.nones, vec![0b0000_0110, 0b0000_0100]);

	let decoded = decode_single(&bytes);
	assert_eq!(decoded.get_type(), ValueType::Option(Box::new(ValueType::Option(Box::new(ValueType::Int4)))));
	assert_eq!(layers(&decoded), vec![vec![false, true, true], vec![false, false, true]]);
	assert_eq!(decoded.get_value(0), Value::none_of(ValueType::Option(Box::new(ValueType::Int4))));
	assert_eq!(decoded.get_value(1), Value::none_of(ValueType::Int4));
	assert_eq!(decoded.get_value(2), Value::Int4(7));
}

#[test]
fn each_layer_takes_ceil_rows_over_eight_bytes() {
	let outer: Vec<bool> = (0..9).map(|i| i != 0 && i != 8).collect();
	let inner: Vec<bool> = (0..9).map(|i| outer[i] && i % 2 == 1).collect();
	let column = option(option(int4((0..9).collect()), &inner), &outer);
	let bytes = encode(column, &EncodeOptions::none());
	let d = descriptor(&bytes);
	assert_eq!(d.row_count, 9);
	assert_eq!(d.nones, vec![0xFE, 0x00, 0xAA, 0x00]);
	assert_eq!(layers(&decode_single(&bytes)), vec![outer, inner]);
}

#[test]
fn a_fully_populated_option_column_keeps_its_option_type() {
	let bytes = encode(option(int4(vec![1, 2, 3]), &[true, true, true]), &EncodeOptions::none());
	let d = descriptor(&bytes);
	assert_eq!(d.type_code, 0x46);
	assert_eq!(d.flags, 0x01);
	assert_eq!(d.nones, vec![0b0000_0111]);
	let decoded = decode_single(&bytes);
	assert_eq!(decoded.get_type(), ValueType::Option(Box::new(ValueType::Int4)));
	assert_eq!(layers(&decoded), vec![vec![true, true, true]]);
}

#[test]
fn option_option_utf8_round_trips_under_every_encoding_choice() {
	let strings: Vec<String> = (0..40).map(|i| format!("v{}", i % 4)).collect();
	let outer: Vec<bool> = (0..40).map(|i| i % 5 != 0).collect();
	let inner: Vec<bool> = (0..40).map(|i| outer[i] && i % 3 != 0).collect();
	let column = option(option(FrameColumnData::Utf8(Utf8Container::new(strings.clone())), &inner), &outer);
	for options in [EncodeOptions::none(), EncodeOptions::default(), EncodeOptions::fast()] {
		let bytes = encode(column.clone(), &options);
		let d = descriptor(&bytes);
		assert_eq!(d.type_code, 0x89, "Utf8 kind 9 under option depth 2");
		assert_eq!(d.nones.len(), 10, "two layers of five bytes each");
		let decoded = decode_single(&bytes);
		assert_eq!(
			decoded.get_type(),
			ValueType::Option(Box::new(ValueType::Option(Box::new(ValueType::Utf8))))
		);
		assert_eq!(layers(&decoded), vec![outer.clone(), inner.clone()]);
		for i in 0..40 {
			let expected = if !outer[i] {
				Value::none_of(ValueType::Option(Box::new(ValueType::Utf8)))
			} else if !inner[i] {
				Value::none_of(ValueType::Utf8)
			} else {
				Value::Utf8(strings[i].clone())
			};
			assert_eq!(decoded.get_value(i), expected, "row {i}");
		}
	}
}

#[test]
fn a_nones_length_that_disagrees_with_the_depth_is_rejected() {
	let column = option(option(int4(vec![0, 0, 7]), &[false, false, true]), &[false, true, true]);
	let mut bytes = encode(column, &EncodeOptions::none());
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
