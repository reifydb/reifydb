// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	collections::HashMap,
	hash::Hash,
	ops::{Deref, Index, IndexMut},
};

use indexmap::IndexMap;
use reifydb_type::{
	fragment::Fragment,
	util::cowvec::CowVec,
	value::{
		Value, constraint::Constraint, container::undefined::UndefinedContainer, row_number::RowNumber,
		r#type::Type,
	},
};

use crate::{
	encoded::schema::{Schema, SchemaField},
	interface::{
		catalog::{table::TableDef, view::ViewDef},
		resolved::{ResolvedRingBuffer, ResolvedTable, ResolvedView},
	},
	row::Row,
	value::column::{Column, ColumnData, headers::ColumnHeaders},
};

#[derive(Debug, Clone)]
pub struct Columns {
	pub row_numbers: CowVec<RowNumber>,
	pub columns: CowVec<Column>,
}

impl Deref for Columns {
	type Target = [Column];

	fn deref(&self) -> &Self::Target {
		self.columns.deref()
	}
}

impl Index<usize> for Columns {
	type Output = Column;

	fn index(&self, index: usize) -> &Self::Output {
		self.columns.index(index)
	}
}

impl IndexMut<usize> for Columns {
	fn index_mut(&mut self, index: usize) -> &mut Self::Output {
		&mut self.columns.make_mut()[index]
	}
}

impl Columns {
	/// Create a 1-column, 1-row Columns from a single Value.
	/// Used to store scalar values inside `Variable::Scalar(Columns)`.
	pub fn scalar(value: Value) -> Self {
		let data = match value {
			Value::Undefined => ColumnData::undefined(1),
			Value::Boolean(v) => ColumnData::bool([v]),
			Value::Float4(v) => ColumnData::float4([v.into()]),
			Value::Float8(v) => ColumnData::float8([v.into()]),
			Value::Int1(v) => ColumnData::int1([v]),
			Value::Int2(v) => ColumnData::int2([v]),
			Value::Int4(v) => ColumnData::int4([v]),
			Value::Int8(v) => ColumnData::int8([v]),
			Value::Int16(v) => ColumnData::int16([v]),
			Value::Utf8(v) => ColumnData::utf8([v]),
			Value::Uint1(v) => ColumnData::uint1([v]),
			Value::Uint2(v) => ColumnData::uint2([v]),
			Value::Uint4(v) => ColumnData::uint4([v]),
			Value::Uint8(v) => ColumnData::uint8([v]),
			Value::Uint16(v) => ColumnData::uint16([v]),
			Value::Date(v) => ColumnData::date([v]),
			Value::DateTime(v) => ColumnData::datetime([v]),
			Value::Time(v) => ColumnData::time([v]),
			Value::Duration(v) => ColumnData::duration([v]),
			Value::IdentityId(v) => ColumnData::identity_id([v]),
			Value::Uuid4(v) => ColumnData::uuid4([v]),
			Value::Uuid7(v) => ColumnData::uuid7([v]),
			Value::Blob(v) => ColumnData::blob([v]),
			Value::Int(v) => ColumnData::int(vec![v]),
			Value::Uint(v) => ColumnData::uint(vec![v]),
			Value::Decimal(v) => ColumnData::decimal(vec![v]),
			Value::DictionaryId(v) => ColumnData::dictionary_id(vec![v]),
			Value::Any(v) => ColumnData::any(vec![v]),
		};
		let column = Column {
			name: Fragment::internal("value"),
			data,
		};
		Self {
			row_numbers: CowVec::new(Vec::new()),
			columns: CowVec::new(vec![column]),
		}
	}

	/// Extract the single value from a 1-column, 1-row Columns.
	/// Panics if the Columns does not have exactly 1 column and 1 row.
	pub fn scalar_value(&self) -> Value {
		debug_assert_eq!(self.len(), 1, "scalar_value() requires exactly 1 column, got {}", self.len());
		debug_assert_eq!(
			self.row_count(),
			1,
			"scalar_value() requires exactly 1 row, got {}",
			self.row_count()
		);
		self.columns[0].data().get_value(0)
	}

	pub fn new(columns: Vec<Column>) -> Self {
		let n = columns.first().map_or(0, |c| c.data().len());
		assert!(columns.iter().all(|c| c.data().len() == n));

		Self {
			row_numbers: CowVec::new(Vec::new()),
			columns: CowVec::new(columns),
		}
	}

	pub fn with_row_numbers(columns: Vec<Column>, row_numbers: Vec<RowNumber>) -> Self {
		let n = columns.first().map_or(0, |c| c.data().len());
		assert!(columns.iter().all(|c| c.data().len() == n));
		assert_eq!(row_numbers.len(), n, "row_numbers length must match column data length");

		Self {
			row_numbers: CowVec::new(row_numbers),
			columns: CowVec::new(columns),
		}
	}

	pub fn single_row<'b>(rows: impl IntoIterator<Item = (&'b str, Value)>) -> Columns {
		let mut columns = Vec::new();
		let mut index = HashMap::new();

		for (idx, (name, value)) in rows.into_iter().enumerate() {
			let data = match value {
				Value::Undefined => ColumnData::undefined(1),
				Value::Boolean(v) => ColumnData::bool([v]),
				Value::Float4(v) => ColumnData::float4([v.into()]),
				Value::Float8(v) => ColumnData::float8([v.into()]),
				Value::Int1(v) => ColumnData::int1([v]),
				Value::Int2(v) => ColumnData::int2([v]),
				Value::Int4(v) => ColumnData::int4([v]),
				Value::Int8(v) => ColumnData::int8([v]),
				Value::Int16(v) => ColumnData::int16([v]),
				Value::Utf8(v) => ColumnData::utf8([v.clone()]),
				Value::Uint1(v) => ColumnData::uint1([v]),
				Value::Uint2(v) => ColumnData::uint2([v]),
				Value::Uint4(v) => ColumnData::uint4([v]),
				Value::Uint8(v) => ColumnData::uint8([v]),
				Value::Uint16(v) => ColumnData::uint16([v]),
				Value::Date(v) => ColumnData::date([v.clone()]),
				Value::DateTime(v) => ColumnData::datetime([v.clone()]),
				Value::Time(v) => ColumnData::time([v.clone()]),
				Value::Duration(v) => ColumnData::duration([v.clone()]),
				Value::IdentityId(v) => ColumnData::identity_id([v]),
				Value::Uuid4(v) => ColumnData::uuid4([v]),
				Value::Uuid7(v) => ColumnData::uuid7([v]),
				Value::Blob(v) => ColumnData::blob([v.clone()]),
				Value::Int(v) => ColumnData::int(vec![v]),
				Value::Uint(v) => ColumnData::uint(vec![v]),
				Value::Decimal(v) => ColumnData::decimal(vec![v]),
				Value::DictionaryId(v) => ColumnData::dictionary_id(vec![v]),
				Value::Any(v) => ColumnData::any(vec![v]),
			};

			let column = Column {
				name: Fragment::internal(name.to_string()),
				data,
			};
			index.insert(name, idx);
			columns.push(column);
		}

		Self {
			row_numbers: CowVec::new(Vec::new()),
			columns: CowVec::new(columns),
		}
	}

	pub fn apply_headers(&mut self, headers: &ColumnHeaders) {
		// Apply the column names from headers to this Columns instance
		for (i, name) in headers.columns.iter().enumerate() {
			if i < self.len() {
				let column = &mut self[i];
				let data = std::mem::replace(column.data_mut(), ColumnData::undefined(0));

				*column = Column {
					name: name.clone(),
					data,
				};
			}
		}
	}
}

impl Columns {
	/// Get the row number (for single-row Columns). Panics if Columns has 0 or multiple rows.
	pub fn number(&self) -> RowNumber {
		assert_eq!(self.row_count(), 1, "number() requires exactly 1 row, got {}", self.row_count());
		if self.row_numbers.is_empty() {
			RowNumber(0)
		} else {
			self.row_numbers[0]
		}
	}

	pub fn shape(&self) -> (usize, usize) {
		let row_count = if !self.row_numbers.is_empty() {
			self.row_numbers.len()
		} else {
			self.get(0).map(|c| c.data().len()).unwrap_or(0)
		};
		(row_count, self.len())
	}

	pub fn into_iter(self) -> impl Iterator<Item = Column> {
		self.columns.into_iter()
	}

	pub fn is_empty(&self) -> bool {
		self.shape().0 == 0
	}

	pub fn row(&self, i: usize) -> Vec<Value> {
		self.iter().map(|c| c.data().get_value(i)).collect()
	}

	pub fn column(&self, name: &str) -> Option<&Column> {
		self.iter().find(|col| col.name().text() == name)
	}

	pub fn row_count(&self) -> usize {
		if !self.row_numbers.is_empty() {
			self.row_numbers.len()
		} else {
			self.first().map_or(0, |col| col.data().len())
		}
	}

	pub fn get_row(&self, index: usize) -> Vec<Value> {
		self.iter().map(|col| col.data().get_value(index)).collect()
	}
}

impl Column {
	pub fn extend(&mut self, other: Column) -> reifydb_type::Result<()> {
		self.data_mut().extend(other.data().clone())
	}
}

impl Columns {
	pub fn from_rows(names: &[&str], result_rows: &[Vec<Value>]) -> Self {
		let column_count = names.len();

		let mut columns: Vec<Column> = names
			.iter()
			.map(|name| Column {
				name: Fragment::internal(name.to_string()),
				data: ColumnData::Undefined(UndefinedContainer::new(0)),
			})
			.collect();

		for row in result_rows {
			assert_eq!(row.len(), column_count, "row length does not match column count");
			for (i, value) in row.iter().enumerate() {
				columns[i].data_mut().push_value(value.clone());
			}
		}

		Columns::new(columns)
	}
}

impl Columns {
	pub fn empty() -> Self {
		Self {
			row_numbers: CowVec::new(vec![]),
			columns: CowVec::new(vec![]),
		}
	}

	pub fn from_table(table: &ResolvedTable) -> Self {
		let _source = table.clone();

		let columns: Vec<Column> = table
			.columns()
			.iter()
			.map(|col| {
				let column_ident = Fragment::internal(&col.name);
				Column {
					name: column_ident,
					data: ColumnData::with_capacity(col.constraint.get_type(), 0),
				}
			})
			.collect();

		Self {
			row_numbers: CowVec::new(Vec::new()),
			columns: CowVec::new(columns),
		}
	}

	/// Create empty Columns (0 rows) with schema from a TableDef
	pub fn from_table_def(table: &TableDef) -> Self {
		let columns: Vec<Column> = table
			.columns
			.iter()
			.map(|col| Column {
				name: Fragment::internal(&col.name),
				data: ColumnData::with_capacity(col.constraint.get_type(), 0),
			})
			.collect();

		Self {
			row_numbers: CowVec::new(Vec::new()),
			columns: CowVec::new(columns),
		}
	}

	/// Create empty Columns (0 rows) with schema from a ViewDef
	pub fn from_view_def(view: &ViewDef) -> Self {
		let columns: Vec<Column> = view
			.columns
			.iter()
			.map(|col| Column {
				name: Fragment::internal(&col.name),
				data: ColumnData::with_capacity(col.constraint.get_type(), 0),
			})
			.collect();

		Self {
			row_numbers: CowVec::new(Vec::new()),
			columns: CowVec::new(columns),
		}
	}

	pub fn from_ringbuffer(ringbuffer: &ResolvedRingBuffer) -> Self {
		let _source = ringbuffer.clone();

		let columns: Vec<Column> = ringbuffer
			.columns()
			.iter()
			.map(|col| {
				let column_ident = Fragment::internal(&col.name);
				Column {
					name: column_ident,
					data: ColumnData::with_capacity(col.constraint.get_type(), 0),
				}
			})
			.collect();

		Self {
			row_numbers: CowVec::new(Vec::new()),
			columns: CowVec::new(columns),
		}
	}

	pub fn from_view(view: &ResolvedView) -> Self {
		let _source = view.clone();

		let columns: Vec<Column> = view
			.columns()
			.iter()
			.map(|col| {
				let column_ident = Fragment::internal(&col.name);
				Column {
					name: column_ident,
					data: ColumnData::with_capacity(col.constraint.get_type(), 0),
				}
			})
			.collect();

		Self {
			row_numbers: CowVec::new(Vec::new()),
			columns: CowVec::new(columns),
		}
	}
}

impl Columns {
	/// Extract a subset of rows by indices, returning a new Columns
	pub fn extract_by_indices(&self, indices: &[usize]) -> Columns {
		if indices.is_empty() {
			return Columns::empty();
		}

		let new_columns: Vec<Column> = self
			.columns
			.iter()
			.map(|col| {
				let mut new_data = ColumnData::with_capacity(col.data().get_type(), indices.len());
				for &idx in indices {
					new_data.push_value(col.data().get_value(idx));
				}
				Column {
					name: col.name.clone(),
					data: new_data,
				}
			})
			.collect();

		if self.row_numbers.is_empty() {
			Columns::new(new_columns)
		} else {
			let new_row_numbers: Vec<RowNumber> = indices.iter().map(|&i| self.row_numbers[i]).collect();
			Columns::with_row_numbers(new_columns, new_row_numbers)
		}
	}

	/// Extract a single row by index, returning a new Columns with 1 row
	pub fn extract_row(&self, index: usize) -> Columns {
		self.extract_by_indices(&[index])
	}

	/// Partition Columns into groups based on keys (one key per row).
	/// Returns an IndexMap preserving insertion order of first occurrence.
	pub fn partition_by_keys<K: Hash + Eq + Clone>(&self, keys: &[K]) -> IndexMap<K, Columns> {
		assert_eq!(keys.len(), self.row_count(), "keys length must match row count");

		// Group indices by key
		let mut key_to_indices: IndexMap<K, Vec<usize>> = IndexMap::new();
		for (idx, key) in keys.iter().enumerate() {
			key_to_indices.entry(key.clone()).or_default().push(idx);
		}

		// Convert to Columns
		key_to_indices.into_iter().map(|(key, indices)| (key, self.extract_by_indices(&indices))).collect()
	}

	/// Create Columns from a Row by decoding its encoded values
	pub fn from_row(row: &Row) -> Self {
		let mut columns = Vec::new();

		for (idx, field) in row.schema.fields().iter().enumerate() {
			let value = row.schema.get_value(&row.encoded, idx);

			// Use the field type for the column data, handling undefined values
			let column_type = if value.get_type() == Type::Undefined {
				field.constraint.get_type()
			} else {
				value.get_type()
			};

			let mut data = if column_type == Type::Undefined {
				ColumnData::undefined(0)
			} else {
				ColumnData::with_capacity(column_type, 1)
			};
			data.push_value(value);

			if column_type == Type::DictionaryId {
				if let ColumnData::DictionaryId(container) = &mut data {
					if let Some(Constraint::Dictionary(dict_id, _)) = field.constraint.constraint()
					{
						container.set_dictionary_id(*dict_id);
					}
				}
			}

			let name = row.schema.get_field_name(idx).expect("Schema missing name for field");

			columns.push(Column {
				name: Fragment::internal(name),
				data,
			});
		}

		Self {
			row_numbers: CowVec::new(vec![row.number]),
			columns: CowVec::new(columns),
		}
	}

	/// Convert Columns back to a Row (assumes single row)
	/// Panics if Columns contains more than 1 row
	pub fn to_single_row(&self) -> Row {
		assert_eq!(self.row_count(), 1, "to_row() requires exactly 1 row, got {}", self.row_count());
		assert_eq!(
			self.row_numbers.len(),
			1,
			"to_row() requires exactly 1 row number, got {}",
			self.row_numbers.len()
		);

		let row_number = self.row_numbers.first().unwrap().clone();

		// Build schema fields for the layout
		let fields: Vec<SchemaField> = self
			.columns
			.iter()
			.map(|col| SchemaField::unconstrained(col.name().text().to_string(), col.data().get_type()))
			.collect();

		let layout = Schema::new(fields);
		let mut encoded = layout.allocate();

		// Get values and set them
		let values: Vec<Value> = self.columns.iter().map(|col| col.data().get_value(0)).collect();
		layout.set_values(&mut encoded, &values);

		Row {
			number: row_number,
			encoded,
			schema: layout,
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_type::value::{date::Date, datetime::DateTime, duration::Duration, time::Time};

	use super::*;

	#[test]
	fn test_single_row_temporal_types() {
		let date = Date::from_ymd(2025, 1, 15).unwrap();
		let datetime = DateTime::from_timestamp(1642694400).unwrap();
		let time = Time::from_hms(14, 30, 45).unwrap();
		let duration = Duration::from_days(30);

		let columns = Columns::single_row([
			("date_col", Value::Date(date.clone())),
			("datetime_col", Value::DateTime(datetime.clone())),
			("time_col", Value::Time(time.clone())),
			("interval_col", Value::Duration(duration.clone())),
		]);

		assert_eq!(columns.len(), 4);
		assert_eq!(columns.shape(), (1, 4));

		// Check that the values are correctly stored
		assert_eq!(columns.column("date_col").unwrap().data().get_value(0), Value::Date(date));
		assert_eq!(columns.column("datetime_col").unwrap().data().get_value(0), Value::DateTime(datetime));
		assert_eq!(columns.column("time_col").unwrap().data().get_value(0), Value::Time(time));
		assert_eq!(columns.column("interval_col").unwrap().data().get_value(0), Value::Duration(duration));
	}

	#[test]
	fn test_single_row_mixed_types() {
		let date = Date::from_ymd(2025, 7, 15).unwrap();
		let time = Time::from_hms(9, 15, 30).unwrap();

		let columns = Columns::single_row([
			("bool_col", Value::Boolean(true)),
			("int_col", Value::Int4(42)),
			("str_col", Value::Utf8("hello".to_string())),
			("date_col", Value::Date(date.clone())),
			("time_col", Value::Time(time.clone())),
			("undefined_col", Value::Undefined),
		]);

		assert_eq!(columns.len(), 6);
		assert_eq!(columns.shape(), (1, 6));

		// Check all values are correctly stored
		assert_eq!(columns.column("bool_col").unwrap().data().get_value(0), Value::Boolean(true));
		assert_eq!(columns.column("int_col").unwrap().data().get_value(0), Value::Int4(42));
		assert_eq!(columns.column("str_col").unwrap().data().get_value(0), Value::Utf8("hello".to_string()));
		assert_eq!(columns.column("date_col").unwrap().data().get_value(0), Value::Date(date));
		assert_eq!(columns.column("time_col").unwrap().data().get_value(0), Value::Time(time));
		assert_eq!(columns.column("undefined_col").unwrap().data().get_value(0), Value::Undefined);
	}

	#[test]
	fn test_single_row_normal_column_names_work() {
		let columns = Columns::single_row([("normal_column", Value::Int4(42))]);
		assert_eq!(columns.len(), 1);
		assert_eq!(columns.column("normal_column").unwrap().data().get_value(0), Value::Int4(42));
	}
}
