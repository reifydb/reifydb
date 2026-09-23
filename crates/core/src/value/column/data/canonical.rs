// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{any::Any, sync::Arc};

use arrow_buffer::NullBuffer;
use reifydb_value::{
	Result,
	value::{Value, value_type::ValueType},
};

use crate::value::column::{buffer::ColumnBuffer, data::ColumnData, encoding::EncodingId};

#[derive(Clone, Debug)]
pub struct Canonical {
	pub ty: ValueType,
	pub nullable: bool,
	pub buffer: ColumnBuffer,
}

impl Canonical {
	pub fn new(ty: ValueType, nullable: bool, mut buffer: ColumnBuffer) -> Self {
		buffer.freeze();
		Self {
			ty,
			nullable,
			buffer,
		}
	}

	pub fn from_buffer(mut buffer: ColumnBuffer) -> Self {
		buffer.freeze();
		Self {
			ty: buffer.base_type(),
			nullable: buffer.nulls().is_some(),
			buffer,
		}
	}

	pub fn from_column_buffer(cd: &ColumnBuffer) -> Result<Self> {
		Ok(Self::from_buffer(cd.clone()))
	}

	pub fn to_buffer(&self) -> ColumnBuffer {
		self.buffer.clone()
	}

	pub fn to_column_buffer(&self) -> Result<ColumnBuffer> {
		Ok(self.to_buffer())
	}

	pub fn len(&self) -> usize {
		self.buffer.len()
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}
}

pub fn encoding_for_type(ty: &ValueType) -> EncodingId {
	match ty {
		ValueType::Boolean => EncodingId::CANONICAL_BOOL,
		ValueType::Utf8
		| ValueType::Blob
		| ValueType::Digest {
			..
		} => EncodingId::CANONICAL_VARLEN,
		ValueType::Int | ValueType::Uint | ValueType::Decimal => EncodingId::CANONICAL_BIGNUM,
		_ => EncodingId::CANONICAL_FIXED,
	}
}

impl ColumnData for Canonical {
	fn len(&self) -> usize {
		self.buffer.len()
	}

	fn encoding(&self) -> EncodingId {
		encoding_for_type(&self.ty)
	}

	fn nones(&self) -> Option<&NullBuffer> {
		self.buffer.nulls()
	}

	fn get_value(&self, idx: usize) -> Value {
		self.buffer.get_value(idx)
	}

	fn as_string(&self, idx: usize) -> String {
		self.buffer.as_string(idx)
	}

	fn as_any(&self) -> &dyn Any {
		self
	}

	fn to_canonical(&self) -> Result<Arc<Canonical>> {
		Ok(Arc::new(self.clone()))
	}
}
