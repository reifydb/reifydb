// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ops::Deref;

use reifydb_type::{Type, Value};

use super::{EncodedRow, EncodedRowLayout, EncodedRowLayoutInner};

/// An encoded row layout that includes field names
#[derive(Debug, Clone)]
pub struct EncodedRowNamedLayout {
	layout: EncodedRowLayout,
	names: Vec<String>,
}

impl EncodedRowNamedLayout {
	pub fn new(fields: impl IntoIterator<Item = (String, Type)>) -> Self {
		let (names, types): (Vec<String>, Vec<Type>) =
			fields.into_iter().map(|(name, r#type)| (name, r#type)).unzip();

		let layout = EncodedRowLayout::new(&types);

		Self {
			layout,
			names,
		}
	}

	pub fn get_name(&self, index: usize) -> Option<&str> {
		self.names.get(index).map(|s| s.as_str())
	}

	pub fn names(&self) -> &[String] {
		&self.names
	}

	pub fn get_value(&self, row: &EncodedRow, index: usize) -> Value {
		self.layout.get_value(row, index)
	}

	pub fn layout(&self) -> &EncodedRowLayout {
		&self.layout
	}

	pub fn allocate_row(&self) -> EncodedRow {
		self.layout.allocate_row()
	}

	pub fn set_values(&self, row: &mut EncodedRow, values: &[Value]) {
		debug_assert_eq!(self.layout.fields.len(), values.len());
		self.layout.set_values(row, values)
	}
}

impl Deref for EncodedRowNamedLayout {
	type Target = EncodedRowLayoutInner;

	fn deref(&self) -> &Self::Target {
		&self.layout
	}
}
