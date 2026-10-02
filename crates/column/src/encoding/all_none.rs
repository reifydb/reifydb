// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{any::Any, sync::Arc};

use arrow_array::Array;
use arrow_buffer::NullBuffer;
use reifydb_core::value::column::{
	builder::ColumnBuilder,
	data::{Column, ColumnData, canonical::Canonical},
	encoding::EncodingId,
};
use reifydb_value::{Result, reifydb_assertions, value::value_type::ValueType};

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
}

impl AllNoneData {
	pub fn new(ty: ValueType, len: usize) -> Self {
		Self {
			ty,
			len,
			nones: NullBuffer::new_null(len),
		}
	}
}

impl ColumnData for AllNoneData {
	fn len(&self) -> usize {
		self.len
	}

	fn encoding(&self) -> EncodingId {
		AllNoneEncoding::ID
	}

	fn nones(&self) -> Option<NullBuffer> {
		Some(self.nones.clone())
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

	fn to_canonical(&self) -> Result<Arc<Canonical>> {
		let mut buffer = ColumnBuilder::with_capacity(self.ty.clone(), self.len);
		for _ in 0..self.len {
			buffer.push_none();
		}
		Ok(Arc::new(Canonical::from_column(&buffer.finish(""))?))
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
		match input.buffer().logical_nulls() {
			Some(nones) if nones.null_count() == input.len() => Ok(Some(Column::from_data(Arc::new(
				AllNoneData::new(input.view().base_type(), input.len()),
			)))),
			_ => Ok(None),
		}
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
