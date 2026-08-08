// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![allow(dead_code)]

use reifydb_codec::frame::{decode::decode_frames, encode::encode_frames, options::EncodeOptions};
use reifydb_value::value::{
	Value,
	frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
	value_type::ValueType,
};

pub fn assert_col_data_eq(a: &FrameColumnData, b: &FrameColumnData) {
	assert_eq!(a.len(), b.len(), "column length mismatch");
	for i in 0..a.len() {
		let va = a.get_value(i);
		let vb = b.get_value(i);
		assert_eq!(va, vb, "mismatch at index {}: {:?} != {:?}", i, va, vb);
	}
}

pub fn assert_frame_eq(a: &Frame, b: &Frame) {
	assert_eq!(a.row_numbers().len(), b.row_numbers().len());
	for (i, (ra, rb)) in a.row_numbers().iter().zip(b.row_numbers()).enumerate() {
		assert_eq!(ra.value(), rb.value(), "row_number mismatch at {}", i);
	}
	assert_eq!(a.created_at().len(), b.created_at().len());
	assert_eq!(a.updated_at().len(), b.updated_at().len());
	assert_eq!(a.columns.len(), b.columns.len());
	for (ca, cb) in a.columns.iter().zip(&b.columns) {
		assert_eq!(ca.name, cb.name);
		assert_col_data_eq(&ca.data, &cb.data);
	}
}

pub fn round_trip_column(name: &str, data: FrameColumnData) {
	round_trip_column_with(name, data, &EncodeOptions::default());
}

pub fn round_trip_column_with(name: &str, data: FrameColumnData, options: &EncodeOptions) {
	let frame = Frame::new(vec![FrameColumn {
		name: name.to_string(),
		data,
	}]);
	let encoded = encode_frames(&[frame.clone()], options).expect("encode failed");
	let decoded = decode_frames(&encoded).expect("decode failed");
	assert_eq!(decoded.len(), 1);
	assert_frame_eq(&frame, &decoded[0]);
}

/// Encode a column and assert it compresses to fewer bytes than plain encoding.
pub fn assert_compresses_well(name: &str, data: FrameColumnData) {
	let frame = Frame::new(vec![FrameColumn {
		name: name.to_string(),
		data,
	}]);
	let compressed = encode_frames(&[frame.clone()], &EncodeOptions::default()).expect("encode failed");
	let plain = encode_frames(&[frame], &EncodeOptions::none()).expect("encode failed");
	assert!(
		compressed.len() < plain.len(),
		"expected compression benefit: compressed={} >= plain={}",
		compressed.len(),
		plain.len()
	);
}

// The generation macros below use fully-qualified paths throughout, so they cannot collide with
// the type-specific imports in each test file that expands them.

/// The calling module must define `fn make(Vec<T>) -> FrameColumnData` and have
/// `FrameColumnData` in scope.
#[macro_export]
macro_rules! plain_tests {
	(typical: $typical:expr, boundary: $boundary:expr, single: $single:expr $(,)?) => {
		#[test]
		fn round_trip() {
			crate::common::round_trip_column("test", make($typical));
		}

		#[test]
		fn empty_column() {
			crate::common::round_trip_column("test", make(vec![]));
		}

		#[test]
		fn single_element() {
			crate::common::round_trip_column("test", make(vec![$single]));
		}

		#[test]
		fn boundary_values() {
			crate::common::round_trip_column("test", make($boundary));
		}

		#[test]
		fn option_round_trip() {
			let values = $typical;
			let len = values.len();
			let defined: Vec<bool> = (0..len).map(|i| i % 2 == 0).collect();
			crate::common::round_trip_column(
				"test",
				FrameColumnData::Option {
					inner: Box::new(make(values)),
					bitvec: reifydb_value::util::bitvec::BitVec::from_slice(&defined),
				},
			);
		}

		#[test]
		fn option_all_nones() {
			let values = $typical;
			let len = values.len();
			let defined = vec![false; len];
			crate::common::round_trip_column(
				"test",
				FrameColumnData::Option {
					inner: Box::new(make(values)),
					bitvec: reifydb_value::util::bitvec::BitVec::from_slice(&defined),
				},
			);
		}

		#[test]
		fn option_all_present() {
			let values = $typical;
			let len = values.len();
			let defined = vec![true; len];
			crate::common::round_trip_column(
				"test",
				FrameColumnData::Option {
					inner: Box::new(make(values)),
					bitvec: reifydb_value::util::bitvec::BitVec::from_slice(&defined),
				},
			);
		}

		#[test]
		fn compression_none_round_trip() {
			crate::common::round_trip_column_with(
				"test",
				make($typical),
				&reifydb_codec::frame::options::EncodeOptions::none(),
			);
		}
	};
}

#[macro_export]
macro_rules! dict_tests {
	(low_cardinality: $low:expr, high_cardinality: $high:expr $(,)?) => {
		#[test]
		fn low_cardinality_round_trip() {
			crate::common::round_trip_column("test", make($low));
		}

		#[test]
		fn low_cardinality_compresses() {
			crate::common::assert_compresses_well("test", make($low));
		}

		#[test]
		fn high_cardinality_round_trip() {
			crate::common::round_trip_column("test", make($high));
		}

		#[test]
		fn option_low_cardinality_round_trip() {
			let values = $low;
			let len = values.len();
			let defined: Vec<bool> = (0..len).map(|i| i % 3 != 0).collect();
			crate::common::round_trip_column(
				"test",
				FrameColumnData::Option {
					inner: Box::new(make(values)),
					bitvec: reifydb_value::util::bitvec::BitVec::from_slice(&defined),
				},
			);
		}

		#[test]
		fn single_value_repeated() {
			let values = $low;
			let first = values[0].clone();
			let repeated = vec![first; 100];
			crate::common::round_trip_column("test", make(repeated));
		}

		#[test]
		fn empty_column() {
			crate::common::round_trip_column("test", make(vec![]));
		}
	};
}

#[macro_export]
macro_rules! rle_tests {
	(repeated: $repeated:expr, unique: $unique:expr $(,)?) => {
		#[test]
		fn repeated_values_round_trip() {
			crate::common::round_trip_column("test", make($repeated));
		}

		#[test]
		fn repeated_values_compresses() {
			crate::common::assert_compresses_well("test", make($repeated));
		}

		#[test]
		fn unique_values_round_trip() {
			crate::common::round_trip_column("test", make($unique));
		}

		#[test]
		fn option_repeated_round_trip() {
			let values = $repeated;
			let len = values.len();
			let defined: Vec<bool> = (0..len).map(|i| i % 2 == 0).collect();
			crate::common::round_trip_column(
				"test",
				FrameColumnData::Option {
					inner: Box::new(make(values)),
					bitvec: reifydb_value::util::bitvec::BitVec::from_slice(&defined),
				},
			);
		}

		#[test]
		fn single_run_round_trip() {
			let values = $repeated;
			let first = values[0].clone();
			let single_run = vec![first; 200];
			crate::common::round_trip_column("test", make(single_run));
		}
	};
}

#[macro_export]
macro_rules! delta_tests {
	(ascending: $asc:expr, descending: $desc:expr, unsorted: $unsorted:expr $(,)?) => {
		#[test]
		fn ascending_round_trip() {
			crate::common::round_trip_column("test", make($asc));
		}

		#[test]
		fn ascending_compresses() {
			crate::common::assert_compresses_well("test", make($asc));
		}

		#[test]
		fn descending_round_trip() {
			crate::common::round_trip_column("test", make($desc));
		}

		#[test]
		fn descending_compresses() {
			crate::common::assert_compresses_well("test", make($desc));
		}

		#[test]
		fn unsorted_round_trip() {
			crate::common::round_trip_column("test", make($unsorted));
		}

		#[test]
		fn option_ascending_round_trip() {
			let values = $asc;
			let len = values.len();
			let defined: Vec<bool> = (0..len).map(|i| i % 2 == 0).collect();
			crate::common::round_trip_column(
				"test",
				FrameColumnData::Option {
					inner: Box::new(make(values)),
					bitvec: reifydb_value::util::bitvec::BitVec::from_slice(&defined),
				},
			);
		}
	};
}

/// Checks that undefined rows survive the round trip as `none` still carrying their inner type,
/// not as a bare `none` or a defaulted value.
pub fn assert_option_round_trip(col: FrameColumnData, expected_inner_type: ValueType, expected_defined: &[bool]) {
	let frame = Frame::new(vec![FrameColumn {
		name: "test".to_string(),
		data: col.clone(),
	}]);
	let encoded = encode_frames(&[frame.clone()], &EncodeOptions::default()).expect("encode failed");
	let decoded_frames = decode_frames(&encoded).expect("decode failed");
	assert_eq!(decoded_frames.len(), 1, "expected one frame");

	let decoded_col = &decoded_frames[0].columns[0].data;
	assert_eq!(
		decoded_col.get_type(),
		ValueType::Option(Box::new(expected_inner_type.clone())),
		"decoded column type should be Option(inner)"
	);
	assert_eq!(decoded_col.len(), expected_defined.len(), "length mismatch");

	for (i, &is_def) in expected_defined.iter().enumerate() {
		assert_eq!(decoded_col.is_defined(i), is_def, "is_defined mismatch at {}", i);

		let actual = decoded_col.get_value(i);
		if is_def {
			let original = col.get_value(i);
			assert_eq!(
				actual, original,
				"defined value mismatch at {}: got {:?}, expected {:?}",
				i, actual, original
			);
			assert!(!matches!(actual, Value::None { .. }), "expected a defined value at {}, got None", i);
		} else {
			match &actual {
				Value::None {
					inner,
				} => assert_eq!(
					*inner, expected_inner_type,
					"None inner_type mismatch at {}: got {:?}, expected {:?}",
					i, inner, expected_inner_type
				),
				other => panic!("expected Value::None at {}, got {:?}", i, other),
			}
		}
	}
}

/// The calling module must define `fn make(Vec<T>) -> FrameColumnData`; the inner `ValueType`
/// arrives through the `inner_type` argument.
#[macro_export]
macro_rules! nones_tests {
	(values: $values:expr, inner_type: $inner_type:expr $(,)?) => {
		macro_rules! __opt_col {
			($defined:expr) => {
				reifydb_value::value::frame::data::FrameColumnData::Option {
					inner: Box::new(make($values)),
					bitvec: reifydb_value::util::bitvec::BitVec::from_slice(&$defined),
				}
			};
		}

		#[test]
		fn all_defined() {
			let values = $values;
			let defined = vec![true; values.len()];
			crate::common::assert_option_round_trip(__opt_col!(defined), $inner_type, &defined);
		}

		#[test]
		fn all_none() {
			let values = $values;
			let defined = vec![false; values.len()];
			crate::common::assert_option_round_trip(__opt_col!(defined), $inner_type, &defined);
		}

		#[test]
		fn first_none() {
			let values = $values;
			assert!(values.len() >= 2, "nones_tests: values must have at least 2 elements");
			let mut defined = vec![true; values.len()];
			defined[0] = false;
			crate::common::assert_option_round_trip(__opt_col!(defined), $inner_type, &defined);
		}

		#[test]
		fn last_none() {
			let values = $values;
			assert!(values.len() >= 2, "nones_tests: values must have at least 2 elements");
			let mut defined = vec![true; values.len()];
			*defined.last_mut().unwrap() = false;
			crate::common::assert_option_round_trip(__opt_col!(defined), $inner_type, &defined);
		}

		#[test]
		fn alternating_from_none() {
			let values = $values;
			let defined: Vec<bool> = (0..values.len()).map(|i| i % 2 == 1).collect();
			crate::common::assert_option_round_trip(__opt_col!(defined), $inner_type, &defined);
		}

		#[test]
		fn alternating_from_defined() {
			let values = $values;
			let defined: Vec<bool> = (0..values.len()).map(|i| i % 2 == 0).collect();
			crate::common::assert_option_round_trip(__opt_col!(defined), $inner_type, &defined);
		}

		#[test]
		fn single_defined() {
			let defined = vec![true];
			let col = {
				let mut v = $values;
				v.truncate(1);
				assert_eq!(v.len(), 1);
				reifydb_value::value::frame::data::FrameColumnData::Option {
					inner: Box::new(make(v)),
					bitvec: reifydb_value::util::bitvec::BitVec::from_slice(&defined),
				}
			};
			crate::common::assert_option_round_trip(col, $inner_type, &defined);
		}

		#[test]
		fn single_none() {
			let defined = vec![false];
			let col = {
				let mut v = $values;
				v.truncate(1);
				assert_eq!(v.len(), 1);
				reifydb_value::value::frame::data::FrameColumnData::Option {
					inner: Box::new(make(v)),
					bitvec: reifydb_value::util::bitvec::BitVec::from_slice(&defined),
				}
			};
			crate::common::assert_option_round_trip(col, $inner_type, &defined);
		}

		#[test]
		fn round_trip_no_compression() {
			let values = $values;
			let defined = vec![true; values.len()];
			let col = __opt_col!(defined);
			let frame = reifydb_value::value::frame::frame::Frame::new(vec![
				reifydb_value::value::frame::column::FrameColumn {
					name: "test".to_string(),
					data: col,
				},
			]);
			let encoded = reifydb_codec::frame::encode::encode_frames(
				&[frame.clone()],
				&reifydb_codec::frame::options::EncodeOptions::none(),
			)
			.expect("encode failed");
			let decoded = reifydb_codec::frame::decode::decode_frames(&encoded).expect("decode failed");
			crate::common::assert_frame_eq(&frame, &decoded[0]);
		}
	};
}

#[macro_export]
macro_rules! delta_rle_tests {
	(constant_stride: $cs:expr, descending_stride: $ds:expr $(,)?) => {
		#[test]
		fn constant_stride_round_trip() {
			crate::common::round_trip_column("test", make($cs));
		}

		#[test]
		fn constant_stride_compresses() {
			crate::common::assert_compresses_well("test", make($cs));
		}

		#[test]
		fn descending_stride_round_trip() {
			crate::common::round_trip_column("test", make($ds));
		}

		#[test]
		fn descending_stride_compresses() {
			crate::common::assert_compresses_well("test", make($ds));
		}

		#[test]
		fn option_constant_stride_round_trip() {
			let values = $cs;
			let len = values.len();
			let defined: Vec<bool> = (0..len).map(|i| i % 2 == 0).collect();
			crate::common::round_trip_column(
				"test",
				FrameColumnData::Option {
					inner: Box::new(make(values)),
					bitvec: reifydb_value::util::bitvec::BitVec::from_slice(&defined),
				},
			);
		}
	};
}
