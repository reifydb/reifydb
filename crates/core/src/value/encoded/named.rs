// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ops::Deref;

use reifydb_type::{Type, Value};

use super::{EncodedValues, EncodedValuesLayout, EncodedValuesLayoutInner};

/// An encoded named layout that includes field names
#[derive(Debug, Clone)]
pub struct EncodedValuesNamedLayout {
	layout: EncodedValuesLayout,
	names: Vec<String>,
}

impl EncodedValuesNamedLayout {
	pub fn new(fields: impl IntoIterator<Item = (String, Type)>) -> Self {
		let (names, types): (Vec<String>, Vec<Type>) =
			fields.into_iter().map(|(name, r#type)| (name, r#type)).unzip();

		let layout = EncodedValuesLayout::new(&types);

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

	pub fn get_value(&self, row: &EncodedValues, index: usize) -> Value {
		self.layout.get_value(row, index)
	}

	pub fn layout(&self) -> &EncodedValuesLayout {
		&self.layout
	}

	pub fn allocate_row(&self) -> EncodedValues {
		self.layout.allocate()
	}

	pub fn set_values(&self, row: &mut EncodedValues, values: &[Value]) {
		debug_assert_eq!(self.layout.fields.len(), values.len());
		self.layout.set_values(row, values)
	}
}

impl Deref for EncodedValuesNamedLayout {
	type Target = EncodedValuesLayoutInner;

	fn deref(&self) -> &Self::Target {
		&self.layout
	}
}
