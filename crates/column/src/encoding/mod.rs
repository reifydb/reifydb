// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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
