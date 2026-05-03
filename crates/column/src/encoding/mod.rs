// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Per-column encoding implementations. Canonical is the dense unencoded layout; the compressed family covers
//! all-none, bit-packed, constant, delta, delta-RLE, dictionary, frame-of-reference, run-length, and sparse forms.
//! Each encoding produces and consumes the same encoded-bytes contract so compute kernels can be written once and
//! work across encodings.
//!
//! Picking an encoding for a write batch is a heuristic decision driven by the column's statistics; choosing
//! poorly does not change correctness, only space and read-time cost.

pub mod canonical;
pub mod compressed;

use std::{
	collections::HashMap,
	sync::{Arc, OnceLock},
};

use canonical::CanonicalEncoding;
use compressed::{
	AllNoneEncoding, BitPackEncoding, ConstantEncoding, DeltaEncoding, DeltaRleEncoding, DictEncoding, ForEncoding,
	RleEncoding, SparseEncoding,
};
use reifydb_core::value::column::{
	data::{Column, canonical::Canonical},
	encoding::EncodingId,
	stats::StatsSet,
};
use reifydb_type::Result;

use crate::{
	compress::CompressConfig,
	compute::{Compute, DefaultCompute},
};

pub trait Encoding: Send + Sync + 'static {
	fn id(&self) -> EncodingId;

	fn try_compress(&self, input: &Canonical, cfg: &CompressConfig) -> Result<Option<Column>>;

	fn canonicalize(&self, array: &Column) -> Result<Canonical>;

	fn compute(&self) -> &dyn Compute {
		&DefaultCompute
	}

	fn derive_stats(&self, _array: &Column) -> StatsSet {
		StatsSet::new()
	}
}

pub struct EncodingRegistry {
	encodings: HashMap<EncodingId, Arc<dyn Encoding>>,
}

impl EncodingRegistry {
	pub fn empty() -> Self {
		Self {
			encodings: HashMap::new(),
		}
	}

	pub fn register(&mut self, encoding: Arc<dyn Encoding>) {
		self.encodings.insert(encoding.id(), encoding);
	}

	pub fn get(&self, id: EncodingId) -> Option<&Arc<dyn Encoding>> {
		self.encodings.get(&id)
	}

	pub fn len(&self) -> usize {
		self.encodings.len()
	}

	pub fn is_empty(&self) -> bool {
		self.encodings.is_empty()
	}

	pub fn builtins() -> Self {
		let mut r = Self::empty();
		r.register(Arc::new(CanonicalEncoding::BOOL));
		r.register(Arc::new(CanonicalEncoding::FIXED));
		r.register(Arc::new(CanonicalEncoding::VARLEN));
		r.register(Arc::new(CanonicalEncoding::BIGNUM));
		r.register(Arc::new(ConstantEncoding));
		r.register(Arc::new(AllNoneEncoding));
		r.register(Arc::new(DictEncoding));
		r.register(Arc::new(RleEncoding));
		r.register(Arc::new(DeltaEncoding));
		r.register(Arc::new(DeltaRleEncoding));
		r.register(Arc::new(ForEncoding));
		r.register(Arc::new(BitPackEncoding));
		r.register(Arc::new(SparseEncoding));
		r
	}
}

static GLOBAL: OnceLock<EncodingRegistry> = OnceLock::new();

pub fn global() -> &'static EncodingRegistry {
	GLOBAL.get_or_init(EncodingRegistry::builtins)
}
