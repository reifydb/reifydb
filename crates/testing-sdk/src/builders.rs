// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use reifydb_codec::row::{
	bytes::RowBuilder,
	shape::{RowFamily, RowShape, RowShapeField},
};
use reifydb_core::{
	common::{ChangeVersion, CommitVersion},
	interface::{
		catalog::{id::TableId, object::ObjectId},
		change::{Change, ChangeOrigin, Diff, Diffs},
	},
	value::{
		batch::{batch, concat},
		column::builder::ColumnBuilder,
	},
};
use reifydb_testing_chaos::operator::event::Row;
use reifydb_value::value::{
	Value,
	constraint::Constraint,
	container::temporal_array::datetime_array,
	datetime::DateTime,
	diff_type::DiffType,
	row_number::RowNumber,
	system_columns::{SystemColumn, stamp_system_columns},
	value_type::ValueType,
};

pub struct TestRowBuilder {
	row_number: RowNumber,
	values: Vec<Value>,
	shape: Option<RowShape>,
	time: Option<DateTime>,
}

impl TestRowBuilder {
	pub fn new(row_number: impl Into<RowNumber>) -> Self {
		Self {
			row_number: row_number.into(),
			values: Vec::new(),
			shape: None,
			time: None,
		}
	}

	pub fn with_time(mut self, time: DateTime) -> Self {
		self.time = Some(time);
		self
	}

	pub fn with_values(mut self, values: Vec<Value>) -> Self {
		self.values = values;
		self
	}

	pub fn add_value(mut self, value: Value) -> Self {
		self.values.push(value);
		self
	}

	pub fn with_shape(mut self, shape: RowShape) -> Self {
		self.shape = Some(shape);
		self
	}

	pub fn build(self) -> Row {
		let shape = if let Some(shape) = self.shape {
			shape
		} else {
			let fields: Vec<RowShapeField> = self
				.values
				.iter()
				.enumerate()
				.map(|(i, v)| RowShapeField::unconstrained(format!("field{}", i), v.get_type()))
				.collect();
			RowShape::new(RowFamily::Table, fields)
		};

		let mut encoded = shape.allocate_table();
		shape.set_values(&mut encoded, &self.values);
		if let Some(time) = self.time {
			encoded.set_time(time);
		}

		Row {
			number: self.row_number,
			encoded: encoded.freeze_bytes(),
			shape,
		}
	}
}

pub struct TestOperatorRowBuilder {
	row_number: RowNumber,
	values: Vec<Value>,
	fields: Option<Vec<RowShapeField>>,
	time: Option<DateTime>,
}

impl TestOperatorRowBuilder {
	pub fn new(row_number: impl Into<RowNumber>) -> Self {
		Self {
			row_number: row_number.into(),
			values: Vec::new(),
			fields: None,
			time: None,
		}
	}

	pub fn with_time(mut self, time: DateTime) -> Self {
		self.time = Some(time);
		self
	}

	pub fn with_values(mut self, values: Vec<Value>) -> Self {
		self.values = values;
		self
	}

	pub fn add_value(mut self, value: Value) -> Self {
		self.values.push(value);
		self
	}

	pub fn with_fields(mut self, fields: Vec<RowShapeField>) -> Self {
		self.fields = Some(fields);
		self
	}

	pub fn build(self) -> Row {
		let fields = self.fields.unwrap_or_else(|| {
			self.values
				.iter()
				.enumerate()
				.map(|(i, v)| RowShapeField::unconstrained(format!("field{}", i), v.get_type()))
				.collect()
		});
		let shape = RowShape::new(RowFamily::Operator, fields);

		let mut encoded = shape.allocate_operator();
		shape.set_values(&mut encoded, &self.values);
		if let Some(time) = self.time {
			encoded.set_time(time);
		}

		Row {
			number: self.row_number,
			encoded: encoded.freeze_bytes(),
			shape,
		}
	}
}

pub struct TestChangeBuilder {
	origin: ChangeOrigin,
	diffs: Diffs,
	version: CommitVersion,
	changed_at: DateTime,
	run: Option<(DiffType, Vec<RecordBatch>, Vec<RecordBatch>)>,
}

impl Default for TestChangeBuilder {
	fn default() -> Self {
		Self::new()
	}
}

impl TestChangeBuilder {
	pub fn new() -> Self {
		Self {
			origin: ChangeOrigin::Object(ObjectId::Table(TableId(1))),
			diffs: Diffs::new(),
			version: CommitVersion(1),
			changed_at: DateTime::default(),
			run: None,
		}
	}

	pub fn changed_by_object(mut self, object: ObjectId) -> Self {
		self.origin = ChangeOrigin::Object(object);
		self
	}

	pub fn with_version(mut self, version: CommitVersion) -> Self {
		self.version = version;
		self
	}

	pub fn insert(mut self, row: Row) -> Self {
		self.extend_run(DiffType::Insert, None, Some(row_batch(&row)));
		self
	}

	pub fn insert_row(self, row_number: impl Into<RowNumber>, values: Vec<Value>) -> Self {
		let row = TestOperatorRowBuilder::new(row_number).with_values(values).build();
		self.insert(row)
	}

	pub fn update(mut self, pre: Row, post: Row) -> Self {
		self.extend_run(DiffType::Update, Some(row_batch(&pre)), Some(row_batch(&post)));
		self
	}

	pub fn update_row(
		self,
		row_number: impl Into<RowNumber>,
		pre_values: Vec<Value>,
		post_values: Vec<Value>,
	) -> Self {
		let row_number = row_number.into();
		let pre = TestOperatorRowBuilder::new(row_number).with_values(pre_values).build();
		let post = TestOperatorRowBuilder::new(row_number).with_values(post_values).build();
		self.update(pre, post)
	}

	pub fn remove(mut self, row: Row) -> Self {
		self.extend_run(DiffType::Remove, Some(row_batch(&row)), None);
		self
	}

	pub fn remove_row(self, row_number: impl Into<RowNumber>, values: Vec<Value>) -> Self {
		let row = TestOperatorRowBuilder::new(row_number).with_values(values).build();
		self.remove(row)
	}

	pub fn build(mut self) -> Change {
		self.close_run();
		Change {
			origin: self.origin,
			diffs: self.diffs,
			version: ChangeVersion::from(self.version),
			changed_at: self.changed_at,
		}
	}
}

impl TestChangeBuilder {
	fn extend_run(&mut self, kind: DiffType, pre: Option<RecordBatch>, post: Option<RecordBatch>) {
		let same_schema = |next: &Option<RecordBatch>, run: &[RecordBatch]| match (next, run.last()) {
			(Some(next), Some(last)) => next.schema_ref() == last.schema_ref(),
			(None, None) => true,
			_ => false,
		};
		let joins = matches!(&self.run, Some((run_kind, pres, posts))
			if *run_kind == kind && same_schema(&pre, pres) && same_schema(&post, posts));
		if !joins {
			self.close_run();
			self.run = Some((kind, Vec::new(), Vec::new()));
		}
		let (_, pres, posts) = self.run.as_mut().expect("a run is open after the check above");
		pres.extend(pre);
		posts.extend(post);
	}

	fn close_run(&mut self) {
		let Some((kind, pres, posts)) = self.run.take() else {
			return;
		};
		self.diffs.push(match kind {
			DiffType::Insert => Diff::insert(glued(&posts)),
			DiffType::Update => Diff::update(glued(&pres), glued(&posts)),
			DiffType::Remove => Diff::remove(glued(&pres)),
		});
	}
}

fn row_batch(row: &Row) -> RecordBatch {
	let shape = &row.shape;
	let mut columns = Vec::with_capacity(shape.fields().len());
	for (index, field) in shape.fields().iter().enumerate() {
		let value = shape.get_value(&row.encoded, index);
		let column_type = match value {
			Value::None {
				..
			} => field.constraint.get_type(),
			Value::Decimal(_) => field.constraint.get_type().inner_type().clone(),
			_ => value.get_type(),
		};
		let mut builder = ColumnBuilder::with_capacity(column_type, 1);
		builder.push_value(value);
		if let Some(Constraint::Dictionary(dictionary, _)) = field.constraint.constraint() {
			builder.set_dictionary_id(*dictionary);
		}
		columns.push(builder.finish(&field.name));
	}
	let carried = shape.family().system_columns();
	let mut stamps: Vec<(SystemColumn, ArrayRef)> =
		vec![(SystemColumn::RowNumbers, Arc::new(UInt64Array::from(vec![row.number.0])))];
	if carried.contains(&SystemColumn::CreatedAt) {
		stamps.push((SystemColumn::CreatedAt, Arc::new(datetime_array([shape.created_at(&row.encoded)]))));
	}
	if carried.contains(&SystemColumn::UpdatedAt) {
		stamps.push((SystemColumn::UpdatedAt, Arc::new(datetime_array([shape.updated_at(&row.encoded)]))));
	}
	if let Some(time) = shape.time(&row.encoded) {
		stamps.push((SystemColumn::Time, Arc::new(datetime_array([time]))));
	}
	match batch(columns).and_then(|columns| stamp_system_columns(columns, stamps)) {
		Ok(batch) => batch,
		Err(e) => panic!("test change row {} does not build a batch: {e}", row.number.0),
	}
}

fn glued(batches: &[RecordBatch]) -> RecordBatch {
	match concat(batches) {
		Ok(batch) => batch,
		Err(e) => panic!("test change rows of one shape do not glue into one batch: {e}"),
	}
}

pub struct TestLayoutBuilder {
	fields: Vec<RowShapeField>,
}

impl Default for TestLayoutBuilder {
	fn default() -> Self {
		Self::new()
	}
}

impl TestLayoutBuilder {
	pub fn new() -> Self {
		Self {
			fields: Vec::new(),
		}
	}

	pub fn add_type(mut self, ty: ValueType) -> Self {
		let field_name = format!("field{}", self.fields.len());
		self.fields.push(RowShapeField::unconstrained(field_name, ty));
		self
	}

	pub fn add_field(mut self, name: impl Into<String>, ty: ValueType) -> Self {
		self.fields.push(RowShapeField::unconstrained(name, ty));
		self
	}

	pub fn build(self) -> RowShape {
		RowShape::new(RowFamily::Table, self.fields)
	}

	pub fn build_named(self) -> RowShape {
		self.build()
	}
}

pub mod helpers {
	use reifydb_core::interface::change::Change;
	use reifydb_value::value::row_number::RowNumber;

	use super::*;

	pub fn int_row(row_number: impl Into<RowNumber>, value: i8) -> Row {
		TestRowBuilder::new(row_number).with_values(vec![Value::Int8(value as i64)]).build()
	}

	pub fn key_value_row(row_number: impl Into<RowNumber>, key: &str, value: i8) -> Row {
		TestRowBuilder::new(row_number)
			.with_values(vec![Value::Utf8(key.into()), Value::Int8(value as i64)])
			.build()
	}

	pub fn insert_change(row: Row) -> Change {
		TestChangeBuilder::new().insert(row).build()
	}

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
		common::{ChangeVersion, CommitVersion},
		interface::{catalog::object::ObjectId, change::ChangeOrigin},
	};
	use reifydb_value::value::{row_number::RowNumber, value_type::ValueType};

	use super::{helpers::*, *};

	#[test]
	fn test_row_builder() {
		let row = TestRowBuilder::new(42)
			.add_value(Value::Int8(10i64))
			.add_value(Value::Utf8("test".into()))
			.build();

		assert_eq!(row.number, RowNumber(42));
		assert_eq!(row.shape.field_count(), 2);
	}

	#[test]
	fn test_flow_change_builder() {
		let change = TestChangeBuilder::new()
			.changed_by_object(ObjectId::table(100))
			.with_version(CommitVersion(5))
			.insert_row(1, vec![Value::Int8(42i64)])
			.update_row(2, vec![Value::Int8(10i64)], vec![Value::Int8(20i64)])
			.remove_row(3, vec![Value::Int8(30i64)])
			.build();

		assert_eq!(change.version, ChangeVersion::from(CommitVersion(5)));
		assert_eq!(change.diffs.len(), 3);

		match &change.origin {
			ChangeOrigin::Object(object) => {
				assert_eq!(*object, ObjectId::table(100));
			}
			_ => panic!("Expected external origin"),
		}
	}

	#[test]
	fn test_layout_builder() {
		let unnamed = TestLayoutBuilder::new().add_type(ValueType::Int8).add_type(ValueType::Utf8).build();

		assert_eq!(unnamed.field_count(), 2);

		let named = TestLayoutBuilder::new()
			.add_field("count", ValueType::Int8)
			.add_field("name", ValueType::Utf8)
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
