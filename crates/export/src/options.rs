// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

#[derive(Debug, Clone, PartialEq)]
pub struct ExportOptions {
	pub selection: ExportSelection,
	pub contents: ExportContents,
	pub insert_batch_size: usize,
	pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExportSelection {
	All,
	Namespaces(Vec<String>),
	Shapes(Vec<QualifiedShape>),
	Kinds(Vec<ShapeKind>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct QualifiedShape {
	pub namespace: String,
	pub name: String,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ExportContents {
	SchemaAndData,
	SchemaOnly,
	DataOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShapeKind {
	Table,
	RingBuffer,
	Series,
	Dictionary,
	Enum,
}

pub const DEFAULT_INSERT_BATCH_SIZE: usize = 500;

impl ExportOptions {
	pub fn all() -> Self {
		Self {
			selection: ExportSelection::All,
			contents: ExportContents::SchemaAndData,
			insert_batch_size: DEFAULT_INSERT_BATCH_SIZE,
			if_not_exists: false,
		}
	}

	pub fn namespace(mut self, name: impl Into<String>) -> Self {
		let name = name.into();
		match &mut self.selection {
			ExportSelection::Namespaces(names) => names.push(name),
			_ => self.selection = ExportSelection::Namespaces(vec![name]),
		}
		self
	}

	pub fn shape(mut self, namespace: impl Into<String>, name: impl Into<String>) -> Self {
		let shape = QualifiedShape {
			namespace: namespace.into(),
			name: name.into(),
		};
		match &mut self.selection {
			ExportSelection::Shapes(shapes) => shapes.push(shape),
			_ => self.selection = ExportSelection::Shapes(vec![shape]),
		}
		self
	}

	pub fn kind(mut self, kind: ShapeKind) -> Self {
		match &mut self.selection {
			ExportSelection::Kinds(kinds) => {
				if !kinds.contains(&kind) {
					kinds.push(kind);
				}
			}
			_ => self.selection = ExportSelection::Kinds(vec![kind]),
		}
		self
	}

	pub fn schema_only(mut self) -> Self {
		self.contents = ExportContents::SchemaOnly;
		self
	}

	pub fn data_only(mut self) -> Self {
		self.contents = ExportContents::DataOnly;
		self
	}

	pub fn batch_size(mut self, size: usize) -> Self {
		self.insert_batch_size = size.max(1);
		self
	}

	pub fn if_not_exists(mut self, enabled: bool) -> Self {
		self.if_not_exists = enabled;
		self
	}

	pub fn includes_schema(&self) -> bool {
		!matches!(self.contents, ExportContents::DataOnly)
	}

	pub fn includes_data(&self) -> bool {
		!matches!(self.contents, ExportContents::SchemaOnly)
	}
}

impl Default for ExportOptions {
	fn default() -> Self {
		Self::all()
	}
}
