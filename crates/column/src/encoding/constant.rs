// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{any::Any, sync::Arc};

use arrow_buffer::NullBuffer;
use reifydb_core::value::column::{
	buffer::ColumnBuffer,
	builder::ColumnBuilder,
	data::{Column, ColumnData, canonical::Canonical},
	encoding::EncodingId,
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

pub struct ConstantEncoding;

impl ConstantEncoding {
	pub const ID: EncodingId = EncodingId::CONSTANT;
}

pub struct ConstantData {
	ty: ValueType,
	value: Value,
	len: usize,
}

impl ConstantData {
	pub fn new(ty: ValueType, value: Value, len: usize) -> Self {
		Self {
			ty,
			value,
			len,
		}
	}

	fn repeated(&self, count: usize) -> ColumnBuffer {
		let mut buffer = ColumnBuilder::with_capacity(self.ty.clone(), count);
		for _ in 0..count {
			buffer.push_value(self.value.clone());
		}
		buffer.finish()
	}
}

impl ColumnData for ConstantData {
	fn len(&self) -> usize {
		self.len
	}

	fn encoding(&self) -> EncodingId {
		ConstantEncoding::ID
	}

	fn nones(&self) -> Option<&NullBuffer> {
		None
	}

	fn get_value(&self, idx: usize) -> Value {
		reifydb_assertions! {
			let len = self.len;
			assert!(
				idx < len,
				"constant column has no row {idx}, so an out-of-bounds read would return the \
				 constant instead of panicking the way every other encoding does (len={len})"
			);
		}
		self.value.clone()
	}

	fn as_string(&self, idx: usize) -> String {
		reifydb_assertions! {
			let len = self.len;
			assert!(idx < len, "constant column has no row {idx} (len={len})");
		}
		self.repeated(1).as_string(0)
	}

	fn as_any(&self) -> &dyn Any {
		self
	}

	fn to_canonical(&self) -> Result<Arc<Canonical>> {
		Ok(Arc::new(Canonical::from_buffer(self.repeated(self.len))))
	}
}

impl Encoding for ConstantEncoding {
	fn id(&self) -> EncodingId {
		Self::ID
	}

	fn try_compress(&self, input: &Canonical, _cfg: &CompressConfig) -> Result<Option<Column>> {
		if input.is_empty() || input.nullable {
			return Ok(None);
		}
		let first = input.buffer.get_value(0);
		for idx in 1..input.len() {
			if input.buffer.get_value(idx) != first {
				return Ok(None);
			}
		}
		Ok(Some(Column::from_data(Arc::new(ConstantData::new(input.ty.clone(), first, input.len())))))
	}

	fn persist(&self, array: &Column) -> Result<PersistedArray> {
		let data = array
			.data()
			.as_any()
			.downcast_ref::<ConstantData>()
			.ok_or_else(|| unexpected_data(Self::ID))?;
		Ok(PersistedArray::Constant {
			value: data.value.clone(),
			len: data.len as u64,
		})
	}

	fn load(&self, persisted: PersistedArray, ty: &ValueType) -> Result<Column> {
		match persisted {
			PersistedArray::Constant {
				value,
				len,
			} => Ok(Column::from_data(Arc::new(ConstantData::new(ty.clone(), value, len as usize)))),
			_ => Err(unexpected_payload(Self::ID)),
		}
	}
}
