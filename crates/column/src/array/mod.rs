// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod bignum;
pub mod boolean;
pub mod canonical;
pub mod fixed;
pub mod varlen;

use std::{any::Any, sync::Arc};

use reifydb_type::{Result, value::r#type::Type};

use crate::{array::canonical::CanonicalArray, encoding::EncodingId, nones::NoneBitmap, stats::StatsSet};

pub trait ArrayData: Send + Sync + 'static {
	fn ty(&self) -> Type;
	fn is_nullable(&self) -> bool;
	fn len(&self) -> usize;
	fn is_empty(&self) -> bool {
		self.len() == 0
	}
	fn encoding(&self) -> EncodingId;
	fn stats(&self) -> &StatsSet;
	fn nones(&self) -> Option<&NoneBitmap>;
	fn children(&self) -> &[Array];
	fn metadata(&self) -> &dyn Any;
	fn to_canonical(&self) -> Result<Arc<CanonicalArray>>;
}

#[derive(Clone)]
pub struct Array(Arc<dyn ArrayData>);

impl Array {
	pub fn from_data(data: Arc<dyn ArrayData>) -> Self {
		Self(data)
	}

	pub fn from_canonical(canon: CanonicalArray) -> Self {
		Self(Arc::new(canon))
	}

	pub fn ty(&self) -> Type {
		self.0.ty()
	}

	pub fn is_nullable(&self) -> bool {
		self.0.is_nullable()
	}

	pub fn len(&self) -> usize {
		self.0.len()
	}

	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	pub fn encoding(&self) -> EncodingId {
		self.0.encoding()
	}

	pub fn stats(&self) -> &StatsSet {
		self.0.stats()
	}

	pub fn nones(&self) -> Option<&NoneBitmap> {
		self.0.nones()
	}

	pub fn children(&self) -> &[Array] {
		self.0.children()
	}

	pub fn metadata(&self) -> &dyn Any {
		self.0.metadata()
	}

	pub fn to_canonical(&self) -> Result<Arc<CanonicalArray>> {
		self.0.to_canonical()
	}
}
