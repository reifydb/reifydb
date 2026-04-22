// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod canonical;

use std::{collections::HashMap, sync::Arc};

use reifydb_type::Result;

use crate::{
	array::{Array, canonical::CanonicalArray},
	compress::CompressConfig,
	compute::{Compute, DefaultCompute},
	stats::StatsSet,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct EncodingId(pub &'static str);

impl EncodingId {
	pub const CANONICAL_BOOL: EncodingId = EncodingId("column.canonical.bool");
	pub const CANONICAL_FIXED: EncodingId = EncodingId("column.canonical.fixed");
	pub const CANONICAL_VARLEN: EncodingId = EncodingId("column.canonical.varlen");
	pub const CANONICAL_BIGNUM: EncodingId = EncodingId("column.canonical.bignum");
}

// One `Encoding` per concrete encoding id. Compressed encodings will fill in
// real `try_compress`/`canonicalize` bodies; canonical encodings perform an
// identity wrap and return their input back.
pub trait Encoding: Send + Sync + 'static {
	fn id(&self) -> EncodingId;

	// Try to compress the canonical input into this encoding. `Ok(None)` means
	// "this encoding doesn't apply to this input" — the compressor will try
	// the next candidate.
	fn try_compress(&self, input: &CanonicalArray, cfg: &CompressConfig) -> Result<Option<Array>>;

	// Decode an array of this encoding back to its canonical form. Must be total.
	fn canonicalize(&self, array: &Array) -> Result<CanonicalArray>;

	fn compute(&self) -> &dyn Compute {
		&DefaultCompute
	}

	fn derive_stats(&self, _array: &Array) -> StatsSet {
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

	// All built-in encodings registered. Phase 3 wires the four canonical ones;
	// Phase 6 extends this with the nine compressed encodings.
	pub fn builtins() -> Self {
		use canonical::CanonicalEncoding;
		let mut r = Self::empty();
		r.register(Arc::new(CanonicalEncoding::BOOL));
		r.register(Arc::new(CanonicalEncoding::FIXED));
		r.register(Arc::new(CanonicalEncoding::VARLEN));
		r.register(Arc::new(CanonicalEncoding::BIGNUM));
		r
	}
}
