// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	encoded::schema::{Schema, SchemaField},
	interface::{
		catalog::{flow::FlowNodeId, id::TableId, primitive::PrimitiveId},
		change::{Change, ChangeOrigin, Diff},
	},
	row::Row,
	value::column::columns::Columns,
};
use reifydb_type::value::{Value, row_number::RowNumber, r#type::Type};

/// Builder for creating test rows
pub struct TestRowBuilder {
	row_number: RowNumber,
	values: Vec<Value>,
	schema: Option<Schema>,
}

impl TestRowBuilder {
	/// Create a new row builder with the given row number
	pub fn new(row_number: impl Into<RowNumber>) -> Self {
		Self {
			row_number: row_number.into(),
			values: Vec::new(),
			schema: None,
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

	/// Set the schema for the row (inferred from values if not set)
	pub fn with_schema(mut self, schema: Schema) -> Self {
		self.schema = Some(schema);
		self
	}

	/// Build the row
	pub fn build(self) -> Row {
		// Use provided schema or infer from values
		let schema = if let Some(schema) = self.schema {
			schema
		} else {
			// Infer types from values and create schema
			let fields: Vec<SchemaField> = self
				.values
				.iter()
				.enumerate()
				.map(|(i, v)| SchemaField::unconstrained(format!("field{}", i), v.get_type()))
				.collect();
			Schema::new(fields)
		};

		let mut encoded = schema.allocate();
		schema.set_values(&mut encoded, &self.values);

		Row {
			number: self.row_number,
			encoded,
			schema,
		}
	}
}

/// Builder for creating test flow changes
pub struct TestChangeBuilder {
	origin: ChangeOrigin,
	diffs: Vec<Diff>,
	version: CommitVersion,
}

impl TestChangeBuilder {
	/// Create a new flow change builder with default origin and version
	pub fn new() -> Self {
		Self {
			origin: ChangeOrigin::Primitive(PrimitiveId::Table(TableId(1))),
			diffs: Vec::new(),
			version: CommitVersion(1),
		}
	}

	/// Set the origin as an external source
	pub fn changed_by_source(mut self, source: PrimitiveId) -> Self {
		self.origin = ChangeOrigin::Primitive(source);
		self
	}

	/// Set the origin as an internal node
	pub fn changed_by_node(mut self, node: FlowNodeId) -> Self {
		self.origin = ChangeOrigin::Flow(node);
		self
	}

	/// Set the version
	pub fn with_version(mut self, version: CommitVersion) -> Self {
		self.version = version;
		self
	}

	/// Add an insert diff
	pub fn insert(mut self, row: Row) -> Self {
		self.diffs.push(Diff::Insert {
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
		self.diffs.push(Diff::Update {
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
		self.diffs.push(Diff::Remove {
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
	pub fn build(self) -> Change {
		Change {
			origin: self.origin,
			diffs: self.diffs,
			version: self.version,
		}
	}
}

/// Builder for creating test schemas
pub struct TestLayoutBuilder {
	fields: Vec<SchemaField>,
}

impl TestLayoutBuilder {
	/// Create a new schema builder
	pub fn new() -> Self {
		Self {
			fields: Vec::new(),
		}
	}

	/// Add a type to the schema with auto-generated name
	pub fn add_type(mut self, ty: Type) -> Self {
		let field_name = format!("field{}", self.fields.len());
		self.fields.push(SchemaField::unconstrained(field_name, ty));
		self
	}

	/// Add a named field to the schema
	pub fn add_field(mut self, name: impl Into<String>, ty: Type) -> Self {
		self.fields.push(SchemaField::unconstrained(name, ty));
		self
	}

	/// Build the schema
	pub fn build(self) -> Schema {
		Schema::new(self.fields)
	}

	/// Build the schema (alias for backwards compatibility)
	pub fn build_named(self) -> Schema {
		self.build()
	}
}

/// Helper functions for common test data patterns
pub mod helpers {
	use reifydb_core::{encoded::schema::Schema, interface::change::Change, row::Row};
	use reifydb_type::value::{row_number::RowNumber, r#type::Type};

	use super::*;

	/// Create a simple counter schema (single int8 field)
	pub fn counter_layout() -> Schema {
		TestLayoutBuilder::new().add_type(Type::Int8).build()
	}

	/// Create a key-value schema (utf8 key, int8 value)
	pub fn key_value_layout() -> Schema {
		TestLayoutBuilder::new().add_type(Type::Utf8).add_type(Type::Int8).build()
	}

	/// Create a named key-value schema
	pub fn named_key_value_layout() -> Schema {
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
	pub fn empty_change() -> Change {
		TestChangeBuilder::new().build()
	}

	/// Create a flow change with a single insert
	pub fn insert_change(row: Row) -> Change {
		TestChangeBuilder::new().insert(row).build()
	}

	/// Create a flow change with multiple inserts
	pub fn batch_insert_change(rows: Vec<Row>) -> Change {
		let mut builder = TestChangeBuilder::new();
		for row in rows {
			builder = builder.insert(row);
		}
		builder.build()
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		common::CommitVersion,
		interface::{catalog::primitive::PrimitiveId, change::ChangeOrigin},
	};
	use reifydb_type::value::{row_number::RowNumber, r#type::Type};

	use super::{helpers::*, *};

	#[test]
	fn test_row_builder() {
		let row = TestRowBuilder::new(42)
			.add_value(Value::Int8(10i64))
			.add_value(Value::Utf8("test".into()))
			.build();

		assert_eq!(row.number, RowNumber(42));
		assert_eq!(row.schema.field_count(), 2);
	}

	#[test]
	fn test_flow_change_builder() {
		let change = TestChangeBuilder::new()
			.changed_by_source(PrimitiveId::table(100))
			.with_version(CommitVersion(5))
			.insert_row(1, vec![Value::Int8(42i64)])
			.update_row(2, vec![Value::Int8(10i64)], vec![Value::Int8(20i64)])
			.remove_row(3, vec![Value::Int8(30i64)])
			.build();

		assert_eq!(change.version, CommitVersion(5));
		assert_eq!(change.diffs.len(), 3);

		match &change.origin {
			ChangeOrigin::Primitive(source) => {
				assert_eq!(*source, PrimitiveId::table(100));
			}
			_ => panic!("Expected external origin"),
		}
	}

	#[test]
	fn test_layout_builder() {
		let unnamed = TestLayoutBuilder::new().add_type(Type::Int8).add_type(Type::Utf8).build();

		assert_eq!(unnamed.field_count(), 2);

		let named = TestLayoutBuilder::new()
			.add_field("count", Type::Int8)
			.add_field("name", Type::Utf8)
			.build_named();

		assert_eq!(named.field_count(), 2);
		assert_eq!(named.get_field_name(0).unwrap(), "count");
		assert_eq!(named.get_field_name(1).unwrap(), "name");
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
