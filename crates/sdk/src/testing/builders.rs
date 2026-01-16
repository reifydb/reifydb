// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	encoded::{layout::EncodedValuesLayout, named::EncodedValuesNamedLayout},
	interface::catalog::{flow::FlowNodeId, id::TableId, primitive::PrimitiveId},
	row::Row,
	value::column::columns::Columns,
};
use reifydb_type::value::{Value, row_number::RowNumber, r#type::Type};

use crate::flow::{FlowChange, FlowChangeOrigin, FlowDiff};

/// Builder for creating test rows
pub struct TestRowBuilder {
	row_number: RowNumber,
	values: Vec<Value>,
	layout: Option<EncodedValuesLayout>,
	named_layout: Option<EncodedValuesNamedLayout>,
}

impl TestRowBuilder {
	/// Create a new row builder with the given row number
	pub fn new(row_number: impl Into<RowNumber>) -> Self {
		Self {
			row_number: row_number.into(),
			values: Vec::new(),
			layout: None,
			named_layout: None,
		}
	}

	/// Set the values for the row
	pub fn with_values(mut self, values: Vec<Value>) -> Self {
		self.values = values;
		self
	}

	/// Add a single value to the row
	pub fn add_value(mut self, value: Value) -> Self {
		self.values.push(value);
		self
	}

	/// Set the layout for the row (inferred from values if not set)
	pub fn with_layout(mut self, layout: EncodedValuesLayout) -> Self {
		self.layout = Some(layout);
		self.named_layout = None;
		self
	}

	/// Set a named layout for the row
	pub fn with_named_layout(mut self, layout: EncodedValuesNamedLayout) -> Self {
		self.named_layout = Some(layout);
		self.layout = None;
		self
	}

	/// Build the row
	pub fn build(self) -> Row {
		if let Some(named_layout) = self.named_layout {
			// Use named layout
			let mut encoded = named_layout.allocate();
			named_layout.set_values(&mut encoded, &self.values);

			return Row {
				number: self.row_number,
				encoded,
				layout: named_layout,
			};
		}

		// Create a named layout from unnamed layout or infer from values
		let types: Vec<Type> = if let Some(layout) = self.layout {
			(0..layout.fields.len()).map(|i| layout.fields[i].r#type).collect()
		} else {
			self.values.iter().map(|v| v.get_type()).collect()
		};

		// Generate field names as String
		let fields: Vec<(String, Type)> =
			types.iter().enumerate().map(|(i, t)| (format!("field{}", i), *t)).collect();

		let named_layout = EncodedValuesNamedLayout::new(fields);
		let mut encoded = named_layout.allocate();
		named_layout.set_values(&mut encoded, &self.values);

		Row {
			number: self.row_number,
			encoded,
			layout: named_layout,
		}
	}
}

/// Builder for creating test flow changes
pub struct TestFlowChangeBuilder {
	origin: FlowChangeOrigin,
	diffs: Vec<FlowDiff>,
	version: CommitVersion,
}

impl TestFlowChangeBuilder {
	/// Create a new flow change builder with default origin and version
	pub fn new() -> Self {
		Self {
			origin: FlowChangeOrigin::External(PrimitiveId::Table(TableId(1))),
			diffs: Vec::new(),
			version: CommitVersion(1),
		}
	}

	/// Set the origin as an external source
	pub fn changed_by_source(mut self, source: PrimitiveId) -> Self {
		self.origin = FlowChangeOrigin::External(source);
		self
	}

	/// Set the origin as an internal node
	pub fn changed_by_node(mut self, node: FlowNodeId) -> Self {
		self.origin = FlowChangeOrigin::Internal(node);
		self
	}

	/// Set the version
	pub fn with_version(mut self, version: CommitVersion) -> Self {
		self.version = version;
		self
	}

	/// Add an insert diff
	pub fn insert(mut self, row: Row) -> Self {
		self.diffs.push(FlowDiff::Insert {
			post: Columns::from_row(&row),
		});
		self
	}

	/// Add an insert diff with values (convenience method)
	pub fn insert_row(self, row_number: impl Into<RowNumber>, values: Vec<Value>) -> Self {
		let row = TestRowBuilder::new(row_number).with_values(values).build();
		self.insert(row)
	}

	/// Add an update diff
	pub fn update(mut self, pre: Row, post: Row) -> Self {
		self.diffs.push(FlowDiff::Update {
			pre: Columns::from_row(&pre),
			post: Columns::from_row(&post),
		});
		self
	}

	/// Add an update diff with values (convenience method)
	pub fn update_row(
		self,
		row_number: impl Into<RowNumber>,
		pre_values: Vec<Value>,
		post_values: Vec<Value>,
	) -> Self {
		let row_number = row_number.into();
		let pre = TestRowBuilder::new(row_number).with_values(pre_values).build();
		let post = TestRowBuilder::new(row_number).with_values(post_values).build();
		self.update(pre, post)
	}

	/// Add a remove diff
	pub fn remove(mut self, row: Row) -> Self {
		self.diffs.push(FlowDiff::Remove {
			pre: Columns::from_row(&row),
		});
		self
	}

	/// Add a remove diff with values (convenience method)
	pub fn remove_row(self, row_number: impl Into<RowNumber>, values: Vec<Value>) -> Self {
		let row = TestRowBuilder::new(row_number).with_values(values).build();
		self.remove(row)
	}

	/// Build the flow change
	pub fn build(self) -> FlowChange {
		FlowChange {
			origin: self.origin,
			diffs: self.diffs,
			version: self.version,
		}
	}
}

/// Builder for creating test layouts
pub struct TestLayoutBuilder {
	types: Vec<Type>,
	names: Option<Vec<String>>,
}

impl TestLayoutBuilder {
	/// Create a new layout builder
	pub fn new() -> Self {
		Self {
			types: Vec::new(),
			names: None,
		}
	}

	/// Add a type to the layout
	pub fn add_type(mut self, ty: Type) -> Self {
		self.types.push(ty);
		self
	}

	/// Add a named field to the layout
	pub fn add_field(mut self, name: impl Into<String>, ty: Type) -> Self {
		if self.names.is_none() {
			self.names = Some(Vec::new());
		}
		self.names.as_mut().unwrap().push(name.into());
		self.types.push(ty);
		self
	}

	/// Build an unnamed layout
	pub fn build(self) -> EncodedValuesLayout {
		EncodedValuesLayout::new(&self.types)
	}

	/// Build a named layout
	pub fn build_named(self) -> EncodedValuesNamedLayout {
		let names = self.names.unwrap_or_else(|| {
			// Generate default names if not provided
			(0..self.types.len()).map(|i| format!("field{}", i)).collect()
		});

		let fields: Vec<(String, Type)> = names.into_iter().zip(self.types.into_iter()).collect();

		EncodedValuesNamedLayout::new(fields)
	}
}

/// Helper functions for common test data patterns
pub mod helpers {
	use reifydb_core::{
		encoded::{layout::EncodedValuesLayout, named::EncodedValuesNamedLayout},
		row::Row,
	};
	use reifydb_type::value::{row_number::RowNumber, r#type::Type};

	use super::*;
	use crate::flow::FlowChange;

	/// Create a simple counter layout (single int8 field)
	pub fn counter_layout() -> EncodedValuesLayout {
		TestLayoutBuilder::new().add_type(Type::Int8).build()
	}

	/// Create a key-value layout (utf8 key, int8 value)
	pub fn key_value_layout() -> EncodedValuesLayout {
		TestLayoutBuilder::new().add_type(Type::Utf8).add_type(Type::Int8).build()
	}

	/// Create a named key-value layout
	pub fn named_key_value_layout() -> EncodedValuesNamedLayout {
		TestLayoutBuilder::new().add_field("key", Type::Utf8).add_field("value", Type::Int8).build_named()
	}

	/// Create a test row with a single int8 value
	pub fn int_row(row_number: impl Into<RowNumber>, value: i8) -> Row {
		TestRowBuilder::new(row_number).with_values(vec![Value::Int8(value as i64)]).build()
	}

	/// Create a test row with a UTF8 key and int8 value
	pub fn key_value_row(row_number: impl Into<RowNumber>, key: &str, value: i8) -> Row {
		TestRowBuilder::new(row_number)
			.with_values(vec![Value::Utf8(key.into()), Value::Int8(value as i64)])
			.build()
	}

	/// Create an empty flow change from a table source
	pub fn empty_change() -> FlowChange {
		TestFlowChangeBuilder::new().build()
	}

	/// Create a flow change with a single insert
	pub fn insert_change(row: Row) -> FlowChange {
		TestFlowChangeBuilder::new().insert(row).build()
	}

	/// Create a flow change with multiple inserts
	pub fn batch_insert_change(rows: Vec<Row>) -> FlowChange {
		let mut builder = TestFlowChangeBuilder::new();
		for row in rows {
			builder = builder.insert(row);
		}
		builder.build()
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{common::CommitVersion, interface::catalog::primitive::PrimitiveId};
	use reifydb_type::value::{row_number::RowNumber, r#type::Type};

	use super::{helpers::*, *};
	use crate::flow::FlowChangeOrigin;

	#[test]
	fn test_row_builder() {
		let row = TestRowBuilder::new(42)
			.add_value(Value::Int8(10i64))
			.add_value(Value::Utf8("test".into()))
			.build();

		assert_eq!(row.number, RowNumber(42));
		assert_eq!(row.layout.names().len(), 2);
	}

	#[test]
	fn test_flow_change_builder() {
		let change = TestFlowChangeBuilder::new()
			.changed_by_source(PrimitiveId::table(100))
			.with_version(CommitVersion(5))
			.insert_row(1, vec![Value::Int8(42i64)])
			.update_row(2, vec![Value::Int8(10i64)], vec![Value::Int8(20i64)])
			.remove_row(3, vec![Value::Int8(30i64)])
			.build();

		assert_eq!(change.version, CommitVersion(5));
		assert_eq!(change.diffs.len(), 3);

		match &change.origin {
			FlowChangeOrigin::External(source) => {
				assert_eq!(*source, PrimitiveId::table(100));
			}
			_ => panic!("Expected external origin"),
		}
	}

	#[test]
	fn test_layout_builder() {
		let unnamed = TestLayoutBuilder::new().add_type(Type::Int8).add_type(Type::Utf8).build();

		assert_eq!(unnamed.fields.len(), 2);

		let named = TestLayoutBuilder::new()
			.add_field("count", Type::Int8)
			.add_field("name", Type::Utf8)
			.build_named();

		assert_eq!(named.fields().fields.len(), 2);
		assert_eq!(named.names()[0].as_str(), "count");
		assert_eq!(named.names()[1].as_str(), "name");
	}

	#[test]
	fn test_helpers() {
		let row = int_row(1, 42);
		assert_eq!(row.number, RowNumber(1));

		let kv_row = key_value_row(2, "test", 100);
		assert_eq!(kv_row.number, RowNumber(2));

		let change = insert_change(row.clone());
		assert_eq!(change.diffs.len(), 1);

		let batch = batch_insert_change(vec![row.clone(), kv_row.clone()]);
		assert_eq!(batch.diffs.len(), 2);
	}
}
