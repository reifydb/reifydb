// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{any::Any, sync::Arc};

use arrow_buffer::NullBuffer;
use reifydb_value::{
	Result, reifydb_assertions,
	value::{Value, value_type::ValueType},
};

use crate::value::column::{buffer::ColumnBuffer, data::ColumnData, encoding::EncodingId, stats::StatsSet};

#[derive(Clone, Debug)]
pub struct Canonical {
	pub ty: ValueType,
	pub nullable: bool,
	pub nones: Option<NullBuffer>,
	pub buffer: ColumnBuffer,
	stats: StatsSet,
}

impl Canonical {
	pub fn new(ty: ValueType, nullable: bool, nones: Option<NullBuffer>, mut buffer: ColumnBuffer) -> Self {
		reifydb_assertions! {
			assert!(
				!matches!(buffer, ColumnBuffer::Option { .. }),
				"Canonical.buffer must not be a ColumnBuffer::Option; nullability is lifted"
			);
		}
		buffer.freeze();
		Self {
			ty,
			nullable,
			nones,
			buffer,
			stats: StatsSet::new(),
		}
	}

	pub fn from_buffer(b: ColumnBuffer) -> Self {
		match b {
			ColumnBuffer::Option {
				inner,
				bitvec,
			} => {
				let mut inner_c = Self::from_buffer(*inner);
				inner_c.nullable = true;
				inner_c.nones = Some(NullBuffer::new(bitvec));
				inner_c
			}
			mut other => {
				other.freeze();
				let ty = other.get_type();
				Self {
					ty,
					nullable: false,
					nones: None,
					buffer: other,
					stats: StatsSet::new(),
				}
			}
		}
	}

	pub fn from_column_buffer(cd: &ColumnBuffer) -> Result<Self> {
		Ok(Self::from_buffer(cd.clone()))
	}

	pub fn to_buffer(&self) -> ColumnBuffer {
		match &self.nones {
			None => self.buffer.clone(),
			Some(nones) => ColumnBuffer::Option {
				inner: Box::new(self.buffer.clone()),
				bitvec: nones.inner().clone(),
			},
		}
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

	pub fn stats(&self) -> &StatsSet {
		&self.stats
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
	fn ty(&self) -> ValueType {
		self.ty.clone()
	}

	fn is_nullable(&self) -> bool {
		self.nullable
	}

	fn len(&self) -> usize {
		self.buffer.len()
	}

	fn encoding(&self) -> EncodingId {
		encoding_for_type(&self.ty)
	}

	fn stats(&self) -> &StatsSet {
		&self.stats
	}

	fn nones(&self) -> Option<&NullBuffer> {
		self.nones.as_ref()
	}

	fn get_value(&self, idx: usize) -> Value {
		if self.nones.as_ref().map(|n| n.is_null(idx)).unwrap_or(false) {
			Value::none_of(self.ty.clone())
		} else {
			self.buffer.get_value(idx)
		}
	}

	fn as_string(&self, idx: usize) -> String {
		self.buffer.as_string(idx)
	}

	fn as_any(&self) -> &dyn Any {
		self
	}

	fn as_any_mut(&mut self) -> &mut dyn Any {
		self
	}

	fn to_canonical(&self) -> Result<Arc<Canonical>> {
		Ok(Arc::new(self.clone()))
	}
}
