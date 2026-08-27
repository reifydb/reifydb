// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use postcard::{from_bytes, to_stdvec};
use serde::{Serialize, de::DeserializeOwned};
use zstd::{decode_all, encode_all};

use crate::error::{DecodeError, EncodeError};

pub fn encode<T: Serialize + ?Sized>(value: &T, level: i32) -> Result<Vec<u8>, EncodeError> {
	let raw = to_stdvec(value).map_err(|e| EncodeError::Serialization(e.to_string()))?;
	encode_all(&raw[..], level).map_err(|e| EncodeError::Compression(e.to_string()))
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, DecodeError> {
	let raw = decode_all(bytes).map_err(|e| DecodeError::Decompression(e.to_string()))?;
	from_bytes(&raw).map_err(|e| DecodeError::Deserialization(e.to_string()))
}

#[cfg(test)]
mod tests {
	use serde::Deserialize;

	use super::*;

	#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
	struct Sample {
		version: u64,
		rows: Vec<String>,
	}

	fn sample() -> Sample {
		Sample {
			version: 42,
			rows: vec!["alpha".to_string(), "beta".to_string()],
		}
	}

	#[test]
	fn round_trips_a_single_value() {
		let encoded = encode(&sample(), 1).unwrap();
		assert_eq!(decode::<Sample>(&encoded).unwrap(), sample());
	}

	#[test]
	fn round_trips_a_batch_through_the_same_functions() {
		// The row path and the block path share one shape; a Vec must not need its own codec.
		let batch = vec![sample(), sample()];
		let encoded = encode(&batch, 1).unwrap();
		assert_eq!(decode::<Vec<Sample>>(&encoded).unwrap(), batch);
	}

	#[test]
	fn encodes_an_unsized_slice_the_same_as_its_vec() {
		// Compaction hands in a &[T] borrowed from a larger buffer; it must not have to allocate a Vec.
		let batch = vec![sample(), sample()];
		assert_eq!(encode(&batch[..], 1).unwrap(), encode(&batch, 1).unwrap());
	}

	#[test]
	fn every_level_decodes_with_the_same_reader() {
		// The level is a write-side knob only; decode must never need to know which one was used.
		for level in [1, 2, 3, 9] {
			let encoded = encode(&sample(), level).unwrap();
			assert_eq!(decode::<Sample>(&encoded).unwrap(), sample(), "level {level}");
		}
	}

	#[test]
	fn output_is_compressed_not_raw_postcard() {
		// Guards against a refactor that drops the zstd layer and still round-trips.
		let big = Sample {
			version: 1,
			rows: vec!["x".repeat(4096)],
		};
		let raw = to_stdvec(&big).unwrap();
		assert!(encode(&big, 1).unwrap().len() < raw.len() / 4);
	}

	#[test]
	fn rejects_bytes_that_are_not_compressed() {
		let raw = to_stdvec(&sample()).unwrap();
		assert!(matches!(decode::<Sample>(&raw), Err(DecodeError::Decompression(_))));
	}

	#[test]
	fn rejects_a_payload_that_decompresses_to_the_wrong_type() {
		let encoded = encode(&"not a sample".to_string(), 1).unwrap();
		assert!(matches!(decode::<Sample>(&encoded), Err(DecodeError::Deserialization(_))));
	}

	#[test]
	fn rejects_truncated_input() {
		let encoded = encode(&sample(), 1).unwrap();
		let truncated = &encoded[..encoded.len() / 2];
		assert!(decode::<Sample>(truncated).is_err());
	}
}
