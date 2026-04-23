// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{any::Any, sync::Arc};

use reifydb_type::{
	Result,
	value::{Value, r#type::Type},
};

use crate::value::column::{
	array::{Column, ColumnData},
	buffer::ColumnBuffer,
	encoding::EncodingId,
	nones::NoneBitmap,
	stats::StatsSet,
};

// Canonical (uncompressed) column representation. Wraps a `ColumnBuffer` with
// lifted nullability: definedness lives in the outer `nones` bitmap, so the
// inner `buffer` is never a `ColumnBuffer::Option` variant.
//
// The bridge between `ColumnBuffer` and `Canonical` is an Arc-bump clone on the
// `CowVec`-backed containers plus (for nullable columns) an O(n/64) polarity
// flip on the bitvec - `ColumnBuffer::Option.bitvec` uses set bit = defined
// while `NoneBitmap` uses set bit = None.
#[derive(Clone, Debug)]
pub struct Canonical {
	pub ty: Type,
	pub nullable: bool,
	pub nones: Option<NoneBitmap>,
	pub buffer: ColumnBuffer,
	stats: StatsSet,
}

impl Canonical {
	pub fn new(ty: Type, nullable: bool, nones: Option<NoneBitmap>, buffer: ColumnBuffer) -> Self {
		debug_assert!(
			!matches!(buffer, ColumnBuffer::Option { .. }),
			"Canonical.buffer must not be a ColumnBuffer::Option; nullability is lifted"
		);
		Self {
			ty,
			nullable,
			nones,
			buffer,
			stats: StatsSet::new(),
		}
	}

	// Owning constructor: move a `ColumnBuffer` into `Canonical`. If the buffer
	// is `ColumnBuffer::Option`, the definedness bitvec is inverted into a
	// `NoneBitmap` and the inner buffer is unwrapped.
	pub fn from_buffer(b: ColumnBuffer) -> Self {
		match b {
			ColumnBuffer::Option {
				inner,
				bitvec,
			} => {
				let mut inner_c = Self::from_buffer(*inner);
				inner_c.nullable = true;
				inner_c.nones = Some(NoneBitmap::from_defined_bitvec(&bitvec));
				inner_c
			}
			other => {
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

	// Borrowing constructor: Arc-bump clones the inner `CowVec`s, zero data copy.
	pub fn from_column_buffer(cd: &ColumnBuffer) -> Result<Self> {
		Ok(Self::from_buffer(cd.clone()))
	}

	pub fn into_buffer(self) -> ColumnBuffer {
		match self.nones {
			None => self.buffer,
			Some(nones) => ColumnBuffer::Option {
				inner: Box::new(self.buffer),
				bitvec: nones.to_defined_bitvec(),
			},
		}
	}

	pub fn to_buffer(&self) -> ColumnBuffer {
		match &self.nones {
			None => self.buffer.clone(),
			Some(nones) => ColumnBuffer::Option {
				inner: Box::new(self.buffer.clone()),
				bitvec: nones.to_defined_bitvec(),
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

fn encoding_for_type(ty: &Type) -> EncodingId {
	match ty {
		Type::Boolean => EncodingId::CANONICAL_BOOL,
		Type::Utf8 | Type::Blob => EncodingId::CANONICAL_VARLEN,
		Type::Int | Type::Uint | Type::Decimal => EncodingId::CANONICAL_BIGNUM,
		_ => EncodingId::CANONICAL_FIXED,
	}
}

static UNIT_METADATA: () = ();
static EMPTY_CHILDREN: Vec<Column> = Vec::new();

impl ColumnData for Canonical {
	fn ty(&self) -> Type {
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

	fn nones(&self) -> Option<&NoneBitmap> {
		self.nones.as_ref()
	}

	fn get_value(&self, idx: usize) -> Value {
		if self.nones.as_ref().map(|n| n.is_none(idx)).unwrap_or(false) {
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

	fn children(&self) -> &[Column] {
		&EMPTY_CHILDREN
	}

	fn metadata(&self) -> &dyn Any {
		&UNIT_METADATA
	}

	fn to_canonical(&self) -> Result<Arc<Canonical>> {
		Ok(Arc::new(self.clone()))
	}
}
