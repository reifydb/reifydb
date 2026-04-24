// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	hash::Hash,
	ops::{Index, IndexMut},
};

use indexmap::IndexMap;
use reifydb_type::{
	fragment::Fragment,
	util::cowvec::CowVec,
	value::{Value, constraint::Constraint, datetime::DateTime, row_number::RowNumber, r#type::Type},
};

use crate::{
	encoded::shape::{RowShape, RowShapeField},
	interface::catalog::column::Column as CatalogColumn,
	row::Row,
	value::column::{ColumnBuffer, ColumnWithName, array::Column, headers::ColumnHeaders},
};

#[derive(Debug, Clone)]
pub struct Columns {
	pub row_numbers: CowVec<RowNumber>,
	pub created_at: CowVec<DateTime>,
	pub updated_at: CowVec<DateTime>,
	pub columns: CowVec<ColumnBuffer>,
	pub names: CowVec<Fragment>,
}

#[derive(Debug, Clone, Copy)]
pub struct ColumnRef<'a> {
	name: &'a Fragment,
	data: &'a ColumnBuffer,
}

impl Index<usize> for Columns {
	type Output = ColumnBuffer;

	fn index(&self, index: usize) -> &Self::Output {
		&self.columns[index]
	}
}

impl IndexMut<usize> for Columns {
	fn index_mut(&mut self, index: usize) -> &mut Self::Output {
		&mut self.columns.make_mut()[index]
	}
}

impl<'a> ColumnRef<'a> {
	pub fn new(name: &'a Fragment, data: &'a ColumnBuffer) -> Self {
		Self {
			name,
			data,
		}
	}

	pub fn name(&self) -> &'a Fragment {
		self.name
	}

	pub fn data(&self) -> &'a ColumnBuffer {
		self.data
	}

	pub fn get_type(&self) -> Type {
		self.data.get_type()
	}

	pub fn column(&self) -> Column {
		Column::from_column_buffer(self.data.clone())
	}

	pub fn with_new_data(&self, data: ColumnBuffer) -> ColumnWithName {
		ColumnWithName::new(self.name.clone(), data)
	}
}

fn value_to_buffer(value: Value) -> ColumnBuffer {
	match value {
		Value::None {
			..
		} => ColumnBuffer::none_typed(Type::Boolean, 1),
		Value::Boolean(v) => ColumnBuffer::bool([v]),
		Value::Float4(v) => ColumnBuffer::float4([v.into()]),
		Value::Float8(v) => ColumnBuffer::float8([v.into()]),
		Value::Int1(v) => ColumnBuffer::int1([v]),
		Value::Int2(v) => ColumnBuffer::int2([v]),
		Value::Int4(v) => ColumnBuffer::int4([v]),
		Value::Int8(v) => ColumnBuffer::int8([v]),
		Value::Int16(v) => ColumnBuffer::int16([v]),
		Value::Utf8(v) => ColumnBuffer::utf8([v]),
		Value::Uint1(v) => ColumnBuffer::uint1([v]),
		Value::Uint2(v) => ColumnBuffer::uint2([v]),
		Value::Uint4(v) => ColumnBuffer::uint4([v]),
		Value::Uint8(v) => ColumnBuffer::uint8([v]),
		Value::Uint16(v) => ColumnBuffer::uint16([v]),
		Value::Date(v) => ColumnBuffer::date([v]),
		Value::DateTime(v) => ColumnBuffer::datetime([v]),
		Value::Time(v) => ColumnBuffer::time([v]),
		Value::Duration(v) => ColumnBuffer::duration([v]),
		Value::IdentityId(v) => ColumnBuffer::identity_id([v]),
		Value::Uuid4(v) => ColumnBuffer::uuid4([v]),
		Value::Uuid7(v) => ColumnBuffer::uuid7([v]),
		Value::Blob(v) => ColumnBuffer::blob([v]),
		Value::Int(v) => ColumnBuffer::int(vec![v]),
		Value::Uint(v) => ColumnBuffer::uint(vec![v]),
		Value::Decimal(v) => ColumnBuffer::decimal(vec![v]),
		Value::DictionaryId(v) => ColumnBuffer::dictionary_id(vec![v]),
		Value::Any(v) => ColumnBuffer::any(vec![v]),
		Value::Type(v) => ColumnBuffer::any(vec![Box::new(Value::Type(v))]),
		Value::List(v) => ColumnBuffer::any(vec![Box::new(Value::List(v))]),
		Value::Record(v) => ColumnBuffer::any(vec![Box::new(Value::Record(v))]),
		Value::Tuple(v) => ColumnBuffer::any(vec![Box::new(Value::Tuple(v))]),
	}
}

impl Columns {
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
		self.columns[0].get_value(0)
	}

	pub fn new(columns: Vec<ColumnWithName>) -> Self {
		let n = columns.first().map_or(0, |c| c.data.len());
		assert!(columns.iter().all(|c| c.data.len() == n));

		let mut names = Vec::with_capacity(columns.len());
		let mut buffers = Vec::with_capacity(columns.len());
		for c in columns {
			names.push(c.name);
			buffers.push(c.data);
		}

		Self {
			row_numbers: CowVec::new(Vec::new()),
			created_at: CowVec::new(Vec::new()),
			updated_at: CowVec::new(Vec::new()),
			columns: CowVec::new(buffers),
			names: CowVec::new(names),
		}
	}

	pub fn with_system_columns(
		columns: Vec<ColumnWithName>,
		row_numbers: Vec<RowNumber>,
		created_at: Vec<DateTime>,
		updated_at: Vec<DateTime>,
	) -> Self {
		let n = columns.first().map_or(0, |c| c.data.len());
		assert!(columns.iter().all(|c| c.data.len() == n));
		assert_eq!(row_numbers.len(), n, "row_numbers length must match column data length");
		assert_eq!(created_at.len(), n, "created_at length must match column data length");
		assert_eq!(updated_at.len(), n, "updated_at length must match column data length");

		let mut names = Vec::with_capacity(columns.len());
		let mut buffers = Vec::with_capacity(columns.len());
		for c in columns {
			names.push(c.name);
			buffers.push(c.data);
		}

		Self {
			row_numbers: CowVec::new(row_numbers),
			created_at: CowVec::new(created_at),
			updated_at: CowVec::new(updated_at),
			columns: CowVec::new(buffers),
			names: CowVec::new(names),
		}
	}

	pub fn single_row<'b>(rows: impl IntoIterator<Item = (&'b str, Value)>) -> Columns {
		let mut names = Vec::new();
		let mut buffers = Vec::new();
		for (name, value) in rows {
			names.push(Fragment::internal(name.to_string()));
			buffers.push(value_to_buffer(value));
		}
		Self {
			row_numbers: CowVec::new(Vec::new()),
			created_at: CowVec::new(Vec::new()),
			updated_at: CowVec::new(Vec::new()),
			columns: CowVec::new(buffers),
			names: CowVec::new(names),
		}
	}

	pub fn with_row_numbers(mut self, row_numbers: Vec<RowNumber>) -> Self {
		let n = row_numbers.len();
		self.row_numbers = CowVec::new(row_numbers);
		if self.created_at.len() != n {
			let now = DateTime::default();
			self.created_at = CowVec::new(vec![now; n]);
			self.updated_at = CowVec::new(vec![now; n]);
		}
		self
	}

	pub fn from_catalog_columns(cols: &[CatalogColumn]) -> Self {
		let mut names = Vec::with_capacity(cols.len());
		let mut buffers = Vec::with_capacity(cols.len());
		for col in cols {
			names.push(Fragment::internal(&col.name));
			buffers.push(ColumnBuffer::with_capacity(col.constraint.get_type(), 0));
		}
		Self {
			row_numbers: CowVec::new(Vec::new()),
			created_at: CowVec::new(Vec::new()),
			updated_at: CowVec::new(Vec::new()),
			columns: CowVec::new(buffers),
			names: CowVec::new(names),
		}
	}

	pub fn apply_headers(&mut self, headers: &ColumnHeaders) {
		let n = self.len();
		let names = self.names.make_mut();
		for (i, name) in headers.columns.iter().enumerate() {
			if i < n {
				names[i] = name.clone();
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
			self.columns.first().map(|c| c.len()).unwrap_or(0)
		};
		(row_count, self.len())
	}

	pub fn len(&self) -> usize {
		self.columns.len()
	}

	pub fn is_empty(&self) -> bool {
		self.columns.is_empty()
	}

	pub fn iter(&self) -> impl Iterator<Item = ColumnRef<'_>> + '_ {
		self.names.iter().zip(self.columns.iter()).map(|(n, d)| ColumnRef::new(n, d))
	}

	pub fn first(&self) -> Option<ColumnRef<'_>> {
		self.get(0)
	}

	pub fn last(&self) -> Option<ColumnRef<'_>> {
		let n = self.len();
		if n == 0 {
			None
		} else {
			self.get(n - 1)
		}
	}

	pub fn get(&self, index: usize) -> Option<ColumnRef<'_>> {
		if index < self.len() {
			Some(ColumnRef::new(&self.names[index], &self.columns[index]))
		} else {
			None
		}
	}

	pub fn name_at(&self, index: usize) -> &Fragment {
		&self.names[index]
	}

	pub fn data_at(&self, index: usize) -> &ColumnBuffer {
		&self.columns[index]
	}

	pub fn data_at_mut(&mut self, index: usize) -> &mut ColumnBuffer {
		&mut self.columns.make_mut()[index]
	}

	pub fn row(&self, i: usize) -> Vec<Value> {
		self.columns.iter().map(|c| c.get_value(i)).collect()
	}

	pub fn column(&self, name: &str) -> Option<ColumnRef<'_>> {
		self.names.iter().position(|n| n.text() == name).and_then(|i| self.get(i))
	}

	pub fn row_count(&self) -> usize {
		if !self.row_numbers.is_empty() {
			self.row_numbers.len()
		} else {
			self.columns.first().map_or(0, |col| col.len())
		}
	}

	pub fn has_rows(&self) -> bool {
		self.row_count() > 0
	}

	pub fn is_scalar(&self) -> bool {
		self.len() == 1 && self.row_count() == 1
	}

	pub fn get_row(&self, index: usize) -> Vec<Value> {
		self.columns.iter().map(|col| col.get_value(index)).collect()
	}
}

impl Columns {
	pub fn from_rows(names: &[&str], result_rows: &[Vec<Value>]) -> Self {
		let column_count = names.len();

		let mut name_vec: Vec<Fragment> =
			names.iter().map(|name| Fragment::internal(name.to_string())).collect();
		let mut buffers: Vec<ColumnBuffer> =
			(0..column_count).map(|_| ColumnBuffer::none_typed(Type::Boolean, 0)).collect();

		for row in result_rows {
			assert_eq!(row.len(), column_count, "row length does not match column count");
			for (i, value) in row.iter().enumerate() {
				buffers[i].push_value(value.clone());
			}
		}

		let _ = &mut name_vec;
		Self {
			row_numbers: CowVec::new(Vec::new()),
			created_at: CowVec::new(Vec::new()),
			updated_at: CowVec::new(Vec::new()),
			columns: CowVec::new(buffers),
			names: CowVec::new(name_vec),
		}
	}
}

impl Columns {
	pub fn empty() -> Self {
		Self {
			row_numbers: CowVec::new(vec![]),
			created_at: CowVec::new(vec![]),
			updated_at: CowVec::new(vec![]),
			columns: CowVec::new(vec![]),
			names: CowVec::new(vec![]),
		}
	}
}

impl Columns {
	/// Extract a subset of rows by indices, returning a new Columns
	pub fn extract_by_indices(&self, indices: &[usize]) -> Columns {
		if indices.is_empty() {
			return Columns::empty();
		}

		let mut new_buffers: Vec<ColumnBuffer> = Vec::with_capacity(self.columns.len());
		for col in self.columns.iter() {
			let mut new_data = ColumnBuffer::with_capacity(col.get_type(), indices.len());
			for &idx in indices {
				new_data.push_value(col.get_value(idx));
			}
			new_buffers.push(new_data);
		}

		let new_row_numbers: Vec<RowNumber> = if self.row_numbers.is_empty() {
			Vec::new()
		} else {
			indices.iter().map(|&i| self.row_numbers[i]).collect()
		};
		let new_created_at: Vec<DateTime> = if self.created_at.is_empty() {
			Vec::new()
		} else {
			indices.iter().map(|&i| self.created_at[i]).collect()
		};
		let new_updated_at: Vec<DateTime> = if self.updated_at.is_empty() {
			Vec::new()
		} else {
			indices.iter().map(|&i| self.updated_at[i]).collect()
		};
		Columns {
			row_numbers: CowVec::new(new_row_numbers),
			created_at: CowVec::new(new_created_at),
			updated_at: CowVec::new(new_updated_at),
			columns: CowVec::new(new_buffers),
			names: self.names.clone(),
		}
	}

	/// Extract a single row by index, returning a new Columns with 1 row
	pub fn extract_row(&self, index: usize) -> Columns {
		self.extract_by_indices(&[index])
	}

	/// Append rows from `source` at the given `indices` to `self`.
	///
	/// If `self` is empty (no columns), it is initialized to match the shape
	/// of `source` and populated from the selected indices. Otherwise the
	/// per-column data, row_numbers, created_at, and updated_at are extended
	/// in place.
	///
	/// Panics if `self` and `source` have different column counts (only
	/// checked when `self` is non-empty).
	pub fn append_rows_by_indices(&mut self, source: &Columns, indices: &[usize]) {
		if indices.is_empty() {
			return;
		}

		if self.columns.is_empty() {
			*self = source.extract_by_indices(indices);
			return;
		}

		assert_eq!(
			self.columns.len(),
			source.columns.len(),
			"append_rows: column count mismatch (self={}, source={})",
			self.columns.len(),
			source.columns.len(),
		);

		let self_cols = self.columns.make_mut();
		for (i, src_col) in source.columns.iter().enumerate() {
			for &idx in indices {
				self_cols[i].push_value(src_col.get_value(idx));
			}
		}

		if !source.row_numbers.is_empty() {
			let rns = self.row_numbers.make_mut();
			for &idx in indices {
				rns.push(source.row_numbers[idx]);
			}
		}
		if !source.created_at.is_empty() {
			let cr = self.created_at.make_mut();
			for &idx in indices {
				cr.push(source.created_at[idx]);
			}
		}
		if !source.updated_at.is_empty() {
			let up = self.updated_at.make_mut();
			for &idx in indices {
				up.push(source.updated_at[idx]);
			}
		}
	}

	/// Remove the row whose row_number equals `row_number`, if present.
	/// Returns true if a row was removed.
	pub fn remove_row(&mut self, row_number: RowNumber) -> bool {
		let pos = self.row_numbers.iter().position(|&r| r == row_number);
		let Some(idx) = pos else {
			return false;
		};

		let kept_indices: Vec<usize> = (0..self.row_count()).filter(|&i| i != idx).collect();
		*self = self.extract_by_indices(&kept_indices);
		true
	}

	/// Project to a subset of columns by name, preserving the order of the provided names.
	/// Columns not found in self are silently skipped.
	pub fn project_by_names(&self, names: &[String]) -> Columns {
		let mut new_names = Vec::new();
		let mut new_buffers = Vec::new();

		for name in names {
			if let Some(pos) = self.names.iter().position(|n| n.text() == name.as_str()) {
				new_names.push(self.names[pos].clone());
				new_buffers.push(self.columns[pos].clone());
			}
		}

		if new_buffers.is_empty() {
			return Columns::empty();
		}

		Columns {
			row_numbers: self.row_numbers.clone(),
			created_at: self.created_at.clone(),
			updated_at: self.updated_at.clone(),
			columns: CowVec::new(new_buffers),
			names: CowVec::new(new_names),
		}
	}

	/// Partition Columns into groups based on keys (one key per row).
	/// Returns an IndexMap preserving insertion order of first occurrence.
	pub fn partition_by_keys<K: Hash + Eq + Clone>(&self, keys: &[K]) -> IndexMap<K, Columns> {
		assert_eq!(keys.len(), self.row_count(), "keys length must match row count");

		let mut key_to_indices: IndexMap<K, Vec<usize>> = IndexMap::new();
		for (idx, key) in keys.iter().enumerate() {
			key_to_indices.entry(key.clone()).or_default().push(idx);
		}

		key_to_indices.into_iter().map(|(key, indices)| (key, self.extract_by_indices(&indices))).collect()
	}

	/// Create Columns from a Row by decoding its encoded values
	pub fn from_row(row: &Row) -> Self {
		let mut names = Vec::new();
		let mut buffers = Vec::new();

		for (idx, field) in row.shape.fields().iter().enumerate() {
			let value = row.shape.get_value(&row.encoded, idx);

			let column_type = if matches!(value, Value::None { .. }) {
				field.constraint.get_type()
			} else {
				value.get_type()
			};

			let mut data = if column_type.is_option() {
				ColumnBuffer::none_typed(column_type.clone(), 0)
			} else {
				ColumnBuffer::with_capacity(column_type.clone(), 1)
			};
			data.push_value(value);

			if column_type == Type::DictionaryId
				&& let ColumnBuffer::DictionaryId(container) = &mut data
				&& let Some(Constraint::Dictionary(dict_id, _)) = field.constraint.constraint()
			{
				container.set_dictionary_id(*dict_id);
			}

			let name = row.shape.get_field_name(idx).expect("RowShape missing name for field");

			names.push(Fragment::internal(name));
			buffers.push(data);
		}

		Self {
			row_numbers: CowVec::new(vec![row.number]),
			created_at: CowVec::new(vec![DateTime::from_nanos(row.encoded.created_at_nanos())]),
			updated_at: CowVec::new(vec![DateTime::from_nanos(row.encoded.updated_at_nanos())]),
			columns: CowVec::new(buffers),
			names: CowVec::new(names),
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

		let row_number = *self.row_numbers.first().unwrap();

		let fields: Vec<RowShapeField> = self
			.names
			.iter()
			.zip(self.columns.iter())
			.map(|(name, data)| RowShapeField::unconstrained(name.text().to_string(), data.get_type()))
			.collect();

		let layout = RowShape::new(fields);
		let mut encoded = layout.allocate();

		let values: Vec<Value> = self.columns.iter().map(|col| col.get_value(0)).collect();
		layout.set_values(&mut encoded, &values);

		Row {
			number: row_number,
			encoded,
			shape: layout,
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
		let duration = Duration::from_days(30).unwrap();

		let columns = Columns::single_row([
			("date_col", Value::Date(date.clone())),
			("datetime_col", Value::DateTime(datetime.clone())),
			("time_col", Value::Time(time.clone())),
			("interval_col", Value::Duration(duration.clone())),
		]);

		assert_eq!(columns.len(), 4);
		assert_eq!(columns.shape(), (1, 4));

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
			("none_col", Value::none()),
		]);

		assert_eq!(columns.len(), 6);
		assert_eq!(columns.shape(), (1, 6));

		assert_eq!(columns.column("bool_col").unwrap().data().get_value(0), Value::Boolean(true));
		assert_eq!(columns.column("int_col").unwrap().data().get_value(0), Value::Int4(42));
		assert_eq!(columns.column("str_col").unwrap().data().get_value(0), Value::Utf8("hello".to_string()));
		assert_eq!(columns.column("date_col").unwrap().data().get_value(0), Value::Date(date));
		assert_eq!(columns.column("time_col").unwrap().data().get_value(0), Value::Time(time));
		assert_eq!(columns.column("none_col").unwrap().data().get_value(0), Value::none());
	}

	#[test]
	fn test_single_row_normal_column_names_work() {
		let columns = Columns::single_row([("normal_column", Value::Int4(42))]);
		assert_eq!(columns.len(), 1);
		assert_eq!(columns.column("normal_column").unwrap().data().get_value(0), Value::Int4(42));
	}
}
