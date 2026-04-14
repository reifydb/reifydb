// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_wire_format::{
	decode::decode_frames, encode::encode_frames, format::Encoding, json::from::frames_from_json,
	options::EncodeOptions,
};
use serde_json::{Value, from_str, to_string};
use std::{fs, path::Path};
use test_each_file::test_each_path;

test_each_path! { in "pkg/test/conformance/wire-format/plain" as conformance_plain => test_plain }
test_each_path! { in "pkg/test/conformance/wire-format/rle" as conformance_rle => test_rle }
test_each_path! { in "pkg/test/conformance/wire-format/delta" as conformance_delta => test_delta }
test_each_path! { in "pkg/test/conformance/wire-format/delta_rle" as conformance_delta_rle => test_delta_rle }
test_each_path! { in "pkg/test/conformance/wire-format/dict" as conformance_dict => test_dict }

fn test_plain(path: &Path) {
	run_conformance_file(path, Encoding::Plain);
}

fn test_rle(path: &Path) {
	run_conformance_file(path, Encoding::Rle);
}

fn test_delta(path: &Path) {
	run_conformance_file(path, Encoding::Delta);
}

fn test_delta_rle(path: &Path) {
	run_conformance_file(path, Encoding::DeltaRle);
}

fn test_dict(path: &Path) {
	run_conformance_file(path, Encoding::Dict);
}

fn run_conformance_file(path: &Path, encoding: Encoding) {
	let content = fs::read_to_string(path).expect("failed to read test file");
	let cases: Vec<Value> = from_str(&content).expect("failed to parse test JSON");

	for (i, case) in cases.iter().enumerate() {
		let frames_json = to_string(case.get("frames").unwrap()).unwrap();
		let expected_frames = frames_from_json(&frames_json).expect("failed to parse frames from JSON");

		let options = EncodeOptions::forced(encoding);
		let bytes = encode_frames(&expected_frames, &options)
			.unwrap_or_else(|e| panic!("failed to encode case {} in {:?}: {}", i, path, e));

		let decoded_frames = decode_frames(&bytes)
			.unwrap_or_else(|e| panic!("failed to decode case {} in {:?}: {}", i, path, e));

		assert_eq!(
			expected_frames, decoded_frames,
			"mismatch in case {} of {:?} (encoding: {:?})",
			i, path, encoding
		);
	}
}
