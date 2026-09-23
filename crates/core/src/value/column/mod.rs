// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt;

use reifydb_value::{fragment::Fragment, value::value_type::ValueType};

use crate::value::column::buffer::ColumnBuffer;

pub mod buffer;
pub mod builder;
pub mod cast;
pub mod columns;
pub mod data;
pub mod encoding;
pub mod frame;
pub mod headers;
pub mod push;
pub mod row_ref;
pub mod transform;
pub mod view;

pub struct ColumnWithName {
	pub name: Fragment,
	pub data: ColumnBuffer,
}

impl Clone for ColumnWithName {
	fn clone(&self) -> Self {
		Self {
			name: self.name.clone(),
			data: self.data.clone(),
		}
	}
}

impl PartialEq for ColumnWithName {
	fn eq(&self, other: &Self) -> bool {
		self.name == other.name && self.data == other.data
	}
}

impl fmt::Debug for ColumnWithName {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("ColumnWithName").field("name", &self.name).field("data", &self.data).finish()
	}
}

impl ColumnWithName {
	pub fn new(name: impl Into<Fragment>, mut data: ColumnBuffer) -> Self {
		data.freeze();
		Self {
			name: name.into(),
			data,
		}
	}

	pub fn get_type(&self) -> ValueType {
		self.data.get_type()
	}

	pub fn with_new_data(&self, data: ColumnBuffer) -> ColumnWithName {
		ColumnWithName {
			name: self.name.clone(),
			data,
		}
	}

	pub fn name(&self) -> &Fragment {
		&self.name
	}

	pub fn name_owned(&self) -> Fragment {
		self.name.clone()
	}

	pub fn data(&self) -> &ColumnBuffer {
		&self.data
	}
}

impl ColumnWithName {
	pub fn int1(name: impl Into<Fragment>, data: impl IntoIterator<Item = i8>) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::int1(data),
		}
	}

	pub fn int4(name: impl Into<Fragment>, data: impl IntoIterator<Item = i32>) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::int4(data),
		}
	}

	pub fn bool(name: impl Into<Fragment>, data: impl IntoIterator<Item = bool>) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::bool(data),
		}
	}

	pub fn utf8(name: impl Into<Fragment>, data: impl IntoIterator<Item = String>) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::utf8(data),
		}
	}

	pub fn undefined_typed(name: impl Into<Fragment>, ty: ValueType, row_count: usize) -> Self {
		ColumnWithName {
			name: name.into(),
			data: ColumnBuffer::none_typed(ty, row_count),
		}
	}
}
