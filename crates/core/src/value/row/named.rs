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
	/// Create a new named layout from field definitions
	pub fn new(fields: impl IntoIterator<Item = (String, Type)>) -> Self {
		let (names, types): (Vec<String>, Vec<Type>) = fields.into_iter().map(|(name, ty)| (name, ty)).unzip();
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
}

impl Deref for EncodedRowNamedLayout {
	type Target = EncodedRowLayoutInner;

	fn deref(&self) -> &Self::Target {
		&self.layout
	}
}
