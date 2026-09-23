// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod all_none;
pub mod canonical;
pub mod compressed;
pub mod constant;

use std::{
	collections::HashMap,
	sync::{Arc, OnceLock},
};

use all_none::AllNoneEncoding;
use canonical::CanonicalEncoding;
use compressed::{
	BitPackEncoding, DeltaEncoding, DeltaRleEncoding, DictEncoding, ForEncoding, RleEncoding, SparseEncoding,
};
use constant::ConstantEncoding;
use reifydb_core::value::column::{
	data::{Column, canonical::Canonical},
	encoding::EncodingId,
};
use reifydb_value::{Result, value::value_type::ValueType};

use crate::{
	compress::CompressConfig,
	compute::{Compute, DefaultCompute},
	persist::PersistedArray,
};

pub trait Encoding: Send + Sync + 'static {
	fn id(&self) -> EncodingId;

	fn try_compress(&self, input: &Canonical, cfg: &CompressConfig) -> Result<Option<Column>>;

	fn persist(&self, array: &Column) -> Result<PersistedArray>;

	fn load(&self, persisted: PersistedArray, ty: &ValueType) -> Result<Column>;

	fn compute(&self) -> &dyn Compute {
		&DefaultCompute
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
