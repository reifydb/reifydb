// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{any::Any, sync::Arc};

use arrow_buffer::NullBuffer;
use reifydb_core::value::column::{
	builder::ColumnBuilder,
	data::{Column, ColumnData, canonical::Canonical},
	encoding::EncodingId,
	stats::StatsSet,
};
use reifydb_value::{
	Result, reifydb_assertions,
	value::{Value, value_type::ValueType},
};

use crate::{
	compress::CompressConfig,
	encoding::Encoding,
	persist::{PersistedArray, unexpected_data, unexpected_payload},
};

pub struct AllNoneEncoding;

impl AllNoneEncoding {
	pub const ID: EncodingId = EncodingId::ALL_NONE;
}

pub struct AllNoneData {
	ty: ValueType,
	len: usize,
	nones: NullBuffer,
	stats: StatsSet,
}

impl AllNoneData {
	pub fn new(ty: ValueType, len: usize) -> Self {
		Self {
			ty,
			len,
			nones: NullBuffer::new_null(len),
			stats: StatsSet::new(),
		}
	}
}

impl ColumnData for AllNoneData {
	fn ty(&self) -> ValueType {
		self.ty.clone()
	}

	fn len(&self) -> usize {
		self.len
	}

	fn encoding(&self) -> EncodingId {
		AllNoneEncoding::ID
	}

	fn is_nullable(&self) -> bool {
		true
	}

	fn nones(&self) -> Option<&NullBuffer> {
		Some(&self.nones)
	}

	fn stats(&self) -> &StatsSet {
		&self.stats
	}

	fn get_value(&self, idx: usize) -> Value {
		reifydb_assertions! {
			let len = self.len;
			assert!(
				idx < len,
				"all-none column has no row {idx}, so an out-of-bounds read would return none \
				 instead of panicking the way every other encoding does (len={len})"
			);
		}
		Value::none_of(self.ty.clone())
	}

	fn as_string(&self, idx: usize) -> String {
		reifydb_assertions! {
			let len = self.len;
			assert!(idx < len, "all-none column has no row {idx} (len={len})");
		}
		"none".to_string()
	}

	fn as_any(&self) -> &dyn Any {
		self
	}

	fn as_any_mut(&mut self) -> &mut dyn Any {
		self
	}

	fn to_canonical(&self) -> Result<Arc<Canonical>> {
		let mut buffer = ColumnBuilder::with_capacity(self.ty.clone(), self.len);
		for _ in 0..self.len {
			buffer.push_none();
		}
		Ok(Arc::new(Canonical::from_buffer(buffer.finish())))
	}
}

impl Encoding for AllNoneEncoding {
	fn id(&self) -> EncodingId {
		Self::ID
	}

	fn try_compress(&self, input: &Canonical, _cfg: &CompressConfig) -> Result<Option<Column>> {
		if input.is_empty() {
			return Ok(None);
		}
		match input.buffer.nulls() {
			Some(nones) if nones.null_count() == input.len() => {
				Ok(Some(Column::from_data(Arc::new(AllNoneData::new(input.ty.clone(), input.len())))))
			}
			_ => Ok(None),
		}
	}

	fn canonicalize(&self, array: &Column) -> Result<Canonical> {
		let arc = array.to_canonical()?;
		Ok((*arc).clone())
	}

	fn persist(&self, array: &Column) -> Result<PersistedArray> {
		let data =
			array.data().as_any().downcast_ref::<AllNoneData>().ok_or_else(|| unexpected_data(Self::ID))?;
		Ok(PersistedArray::AllNone {
			len: data.len as u64,
		})
	}

	fn load(&self, persisted: PersistedArray, ty: &ValueType) -> Result<Column> {
		match persisted {
			PersistedArray::AllNone {
				len,
			} => Ok(Column::from_data(Arc::new(AllNoneData::new(ty.clone(), len as usize)))),
			_ => Err(unexpected_payload(Self::ID)),
		}
	}
}
