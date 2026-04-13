// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Round-trips `Frame` through `frames_to_json` + `frames_from_json` and asserts
//! the typed shape survives the textual representation.

use reifydb_type::{
	util::bitvec::BitVec,
	value::{
		blob::Blob,
		container::{blob::BlobContainer, bool::BoolContainer, number::NumberContainer, utf8::Utf8Container},
		frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
	},
};
use reifydb_wire_format::json::{from::frames_from_json, to::frames_to_json};

fn round_trip(frame: Frame) {
	let json = frames_to_json(&[frame.clone()]).expect("to_json failed");
	let decoded = frames_from_json(&json).expect("from_json failed");
	assert_eq!(decoded.len(), 1);
	let got = &decoded[0];
	assert_eq!(frame.columns.len(), got.columns.len());
	for (a, b) in frame.columns.iter().zip(&got.columns) {
		assert_eq!(a.name, b.name, "column name mismatch");
		assert_eq!(a.data.len(), b.data.len(), "column len mismatch");
		for i in 0..a.data.len() {
			assert_eq!(a.data.get_value(i), b.data.get_value(i), "cell {} of {} differs", i, a.name);
		}
	}
}

#[test]
fn empty_frame() {
	round_trip(Frame::new(vec![]));
}

#[test]
fn primitives() {
	round_trip(Frame::new(vec![
		FrameColumn {
			name: "b".to_string(),
			data: FrameColumnData::Bool(BoolContainer::new(vec![true, false, true])),
		},
		FrameColumn {
			name: "i4".to_string(),
			data: FrameColumnData::Int4(NumberContainer::new(vec![-1, 0, i32::MAX])),
		},
		FrameColumn {
			name: "u8".to_string(),
			data: FrameColumnData::Uint8(NumberContainer::new(vec![0, 1, u64::MAX])),
		},
		FrameColumn {
			name: "s".to_string(),
			data: FrameColumnData::Utf8(Utf8Container::new(vec![
				"".to_string(),
				"hello".to_string(),
				"日本語".to_string(),
			])),
		},
		FrameColumn {
			name: "blob".to_string(),
			data: FrameColumnData::Blob(BlobContainer::new(vec![
				Blob::new(vec![]),
				Blob::new(vec![0xde, 0xad, 0xbe, 0xef]),
				Blob::new(vec![0x00, 0xff]),
			])),
		},
	]));
}

#[test]
fn option_with_nones() {
	let inner = FrameColumnData::Int4(NumberContainer::new(vec![10, 0, 30]));
	let bitvec = BitVec::from_slice(&[true, false, true]);
	round_trip(Frame::new(vec![FrameColumn {
		name: "maybe".to_string(),
		data: FrameColumnData::Option {
			inner: Box::new(inner),
			bitvec,
		},
	}]));
}

#[test]
fn multi_frame_serialization() {
	let frames = vec![
		Frame::new(vec![FrameColumn {
			name: "a".to_string(),
			data: FrameColumnData::Int4(NumberContainer::new(vec![1, 2])),
		}]),
		Frame::new(vec![FrameColumn {
			name: "b".to_string(),
			data: FrameColumnData::Utf8(Utf8Container::new(vec!["x".to_string()])),
		}]),
	];
	let json = frames_to_json(&frames).expect("to_json failed");
	let decoded = frames_from_json(&json).expect("from_json failed");
	assert_eq!(decoded.len(), 2);
}
