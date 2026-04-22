// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_type::Result;

use crate::{
	array::{Array, canonical::CanonicalArray},
	encoding::{self, Encoding, EncodingId},
};

#[derive(Clone, Debug)]
pub struct CompressConfig {
	pub sample_size: usize,
	pub sample_count: usize,
	pub max_depth: u8,
	pub min_compression_ratio: f32,
}

impl Default for CompressConfig {
	fn default() -> Self {
		Self {
			sample_size: 1024,
			sample_count: 4,
			max_depth: 3,
			min_compression_ratio: 0.85,
		}
	}
}

pub struct Compressor {
	candidates: Vec<Arc<dyn Encoding>>,
	cfg: CompressConfig,
}

impl Compressor {
	pub fn new(cfg: CompressConfig) -> Self {
		let registry = encoding::global();
		let order = [
			EncodingId::CANONICAL_BOOL, // canonical always last via fallback
			EncodingId::CONSTANT,
			EncodingId::ALL_NONE,
			EncodingId::DICT,
			EncodingId::RLE,
			EncodingId::DELTA,
			EncodingId::DELTA_RLE,
			EncodingId::FOR,
			EncodingId::BITPACK,
			EncodingId::SPARSE,
		];
		// Start with compressed candidates in a fixed order; the canonical fallback
		// happens outside the candidate loop, so we skip canonical ids here.
		let candidates = order
			.into_iter()
			.filter(|id| {
				!matches!(
					*id,
					EncodingId::CANONICAL_BOOL
						| EncodingId::CANONICAL_FIXED | EncodingId::CANONICAL_VARLEN
						| EncodingId::CANONICAL_BIGNUM
				)
			})
			.filter_map(|id| registry.get(id).cloned())
			.collect();
		Self {
			candidates,
			cfg,
		}
	}

	pub fn compress(&self, input: &CanonicalArray) -> Result<Array> {
		for candidate in &self.candidates {
			if let Some(compressed) = candidate.try_compress(input, &self.cfg)? {
				return Ok(compressed);
			}
		}
		Ok(Array::from_canonical(input.clone()))
	}
}

pub fn compress(input: &CanonicalArray) -> Result<Array> {
	Compressor::new(CompressConfig::default()).compress(input)
}

#[cfg(test)]
mod tests {
	use reifydb_core::value::column::data::ColumnData;

	use super::*;

	#[test]
	fn compress_falls_back_to_canonical_when_no_stub_applies() {
		let cd = ColumnData::int4([1i32, 2, 3, 4]);
		let canon = CanonicalArray::from_column_data(&cd).unwrap();
		let out = compress(&canon).unwrap();
		assert_eq!(out.encoding(), EncodingId::CANONICAL_FIXED);
		assert_eq!(out.len(), 4);
	}

	#[test]
	fn compress_utf8_falls_back_to_canonical_varlen() {
		let cd = ColumnData::utf8(["alpha", "bravo"]);
		let canon = CanonicalArray::from_column_data(&cd).unwrap();
		let out = compress(&canon).unwrap();
		assert_eq!(out.encoding(), EncodingId::CANONICAL_VARLEN);
		assert_eq!(out.len(), 2);
	}
}
