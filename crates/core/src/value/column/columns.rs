// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	hash::Hash,
	ops::{Index, IndexMut},
};

use indexmap::IndexMap;
use reifydb_codec::row::{
	bytes::EncodedBytes,
	shape::{RowFamily, RowShape},
};
use reifydb_value::{
	Result,
	fragment::Fragment,
	reifydb_assertions,
	value::{
		Value,
		constraint::Constraint,
		datetime::{CREATED_AT_COLUMN_NAME, DateTime, TIME_COLUMN_NAME, UPDATED_AT_COLUMN_NAME},
		partition::Partition,
		row_number::{ROW_NUMBER_COLUMN_NAME, RowNumber},
		system_columns::{RowStamps, SystemColumns},
		value_type::ValueType,
	},
};
use serde::{Deserialize, Serialize};

use crate::{
	interface::catalog::column::Column as CatalogColumn,
	return_internal_error,
	row::Row,
	value::column::{ColumnBuffer, ColumnWithName, data::Column, headers::ColumnHeaders},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Columns {
	pub system: SystemColumns,
	pub columns: Vec<ColumnBuffer>,
	pub names: Vec<Fragment>,
}

impl Columns {
	#[inline]
	pub fn row_numbers(&self) -> &[RowNumber] {
		self.system.row_numbers()
	}

	#[inline]
	pub fn partitions(&self) -> &[Partition] {
		self.system.partitions()
	}

	#[inline]
	pub fn created_at(&self) -> &[DateTime] {
		self.system.created_at()
	}

	#[inline]
	pub fn updated_at(&self) -> &[DateTime] {
		self.system.updated_at()
	}

	#[inline]
	pub fn time(&self) -> &[DateTime] {
		self.system.time()
	}

	pub fn system_column(&self, name: &str) -> Option<ColumnBuffer> {
		let name = name.strip_prefix('#').unwrap_or(name);

		if name == ROW_NUMBER_COLUMN_NAME && !self.row_numbers().is_empty() {
			let values: Vec<u64> = self.row_numbers().iter().map(|r| r.value()).collect();
			return Some(ColumnBuffer::uint8(values));
		}
		if name == CREATED_AT_COLUMN_NAME && !self.created_at().is_empty() {
			return Some(ColumnBuffer::datetime(self.created_at().to_vec()));
		}
		if name == UPDATED_AT_COLUMN_NAME && !self.updated_at().is_empty() {
			return Some(ColumnBuffer::datetime(self.updated_at().to_vec()));
		}
		if name == TIME_COLUMN_NAME && !self.time().is_empty() {
			return Some(ColumnBuffer::datetime(self.time().to_vec()));
		}
		None
	}
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
		&mut self.columns[index]
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

	pub fn get_type(&self) -> ValueType {
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
			inner,
		} => ColumnBuffer::none_typed(inner, 1),
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
		Value::Any(v) => ColumnBuffer::any(vec![*v]),
		Value::Type(v) => ColumnBuffer::any(vec![Value::Type(v)]),
		Value::List(v) => ColumnBuffer::any(vec![Value::List(v)]),
		Value::Record(v) => ColumnBuffer::any(vec![Value::Record(v)]),
		Value::Tuple(v) => ColumnBuffer::any(vec![Value::Tuple(v)]),
	}
}

impl Columns {
	pub fn scalar_value(&self) -> Value {
		reifydb_assertions! {
			assert_eq!(self.len(), 1, "scalar_value() requires exactly 1 column, got {}", self.len());
			assert_eq!(
				self.row_count(),
				1,
				"scalar_value() requires exactly 1 row, got {}",
				self.row_count()
			);
		}
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
			system: SystemColumns::empty(),
			columns: buffers,
			names,
		}
	}

	pub fn with_system(columns: Vec<ColumnWithName>, system: SystemColumns) -> Self {
		let n = columns.first().map_or(0, |c| c.data.len());
		assert!(columns.iter().all(|c| c.data.len() == n));
		system.assert_invariants(n, "Columns::with_system");

		let mut names = Vec::with_capacity(columns.len());
		let mut buffers = Vec::with_capacity(columns.len());
		for c in columns {
			names.push(c.name);
			buffers.push(c.data);
		}

		Self {
			system,
			columns: buffers,
			names,
		}
	}

	pub fn single_row<'b>(rows: impl IntoIterator<Item = (&'b str, Value)>) -> Columns {
		let mut names = Vec::new();
		let mut buffers = Vec::new();
		for (name, value) in rows {
			names.push(Fragment::internal(name));
			buffers.push(value_to_buffer(value));
		}
		Self {
			system: SystemColumns::empty(),
			columns: buffers,
			names,
		}
	}

	pub fn with_row_numbers(mut self, row_numbers: Vec<RowNumber>) -> Self {
		let n = row_numbers.len();
		self.system = SystemColumns::new(
			row_numbers,
			self.system.partitions().to_vec(),
			self.system.created_at().to_vec(),
			self.system.updated_at().to_vec(),
			self.system.time().to_vec(),
		);
		self.system.assert_invariants(n, "Columns::with_row_numbers");
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
			system: SystemColumns::empty(),
			columns: buffers,
			names,
		}
	}

	pub fn apply_headers(&mut self, headers: &ColumnHeaders) {
		let n = self.len();
		let names = &mut self.names;
		for (i, name) in headers.columns.iter().enumerate() {
			if i < n {
				names[i] = name.clone();
			}
		}
	}
}

impl Columns {
	pub fn number(&self) -> RowNumber {
		assert_eq!(self.row_count(), 1, "number() requires exactly 1 row, got {}", self.row_count());
		if self.row_numbers().is_empty() {
			RowNumber(0)
		} else {
			self.row_numbers()[0]
		}
	}

	pub fn shape(&self) -> (usize, usize) {
		let row_count = if !self.row_numbers().is_empty() {
			self.row_numbers().len()
		} else {
			self.columns.first().map(|c| c.len()).unwrap_or(0)
		};
		(row_count, self.len())
	}

	pub fn heap_size(&self) -> usize {
		let data: usize = self.columns.iter().map(|c| c.heap_size()).sum();
		let names: usize = self.names.iter().map(|n| n.text().len()).sum();
		data + names + self.system.heap_size()
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
		&mut self.columns[index]
	}

	pub fn row(&self, i: usize) -> Vec<Value> {
		self.columns.iter().map(|c| c.get_value(i)).collect()
	}

	pub fn column(&self, name: &str) -> Option<ColumnRef<'_>> {
		self.names.iter().position(|n| n.text() == name).and_then(|i| self.get(i))
	}

	pub fn row_count(&self) -> usize {
		if !self.row_numbers().is_empty() {
			self.row_numbers().len()
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

	#[track_caller]
	pub fn assert_invariants(&self, ctx: &str) {
		let n = self.columns.first().map_or(0, |c| c.len());
		for (i, col) in self.columns.iter().enumerate() {
			assert_eq!(
				col.len(),
				n,
				"{ctx}: Columns column[{i}] has length {} but columns[0] has length {n}",
				col.len(),
			);
		}
		self.system.assert_invariants(n, ctx);
	}
}

impl Columns {
	pub fn from_rows(names: &[&str], result_rows: &[Vec<Value>]) -> Self {
		let column_count = names.len();

		let mut name_vec: Vec<Fragment> = names.iter().map(Fragment::internal).collect();
		let mut buffers: Vec<ColumnBuffer> =
			(0..column_count).map(|_| ColumnBuffer::none_typed(ValueType::Boolean, 0)).collect();

		for row in result_rows {
			assert_eq!(row.len(), column_count, "row length does not match column count");
			for (i, value) in row.iter().enumerate() {
				buffers[i].push_value(value.clone());
			}
		}

		let _ = &mut name_vec;
		Self {
			system: SystemColumns::empty(),
			columns: buffers,
			names: name_vec,
		}
	}

	pub fn from_encoded_bytes(shape: &RowShape, ids: &[RowNumber], bytes_slice: &[EncodedBytes]) -> Self {
		assert_eq!(ids.len(), bytes_slice.len(), "ids length must match rows length");
		let fields = shape.fields();
		let row_count = bytes_slice.len();

		let mut columns_vec: Vec<ColumnWithName> = Vec::with_capacity(fields.len());
		for field in fields.iter() {
			let mut data = ColumnBuffer::with_capacity(field.constraint.get_type(), row_count);
			if field.constraint.get_type() == ValueType::DictionaryId
				&& let ColumnBuffer::DictionaryId(container) = &mut data
				&& let Some(Constraint::Dictionary(dict_id, _)) = field.constraint.constraint()
			{
				container.set_dictionary_id(*dict_id);
			}
			columns_vec.push(ColumnWithName {
				name: Fragment::internal(&field.name),
				data,
			});
		}

		for encoded in bytes_slice {
			for (i, _) in fields.iter().enumerate() {
				columns_vec[i].data.push_value(shape.get_value(encoded, i));
			}
		}

		let row_numbers: Vec<RowNumber> = ids.to_vec();
		let (created_at, updated_at): (Vec<DateTime>, Vec<DateTime>) = match shape.family() {
			RowFamily::Pod => (Vec::new(), Vec::new()),
			_ => (
				bytes_slice.iter().map(|r| shape.created_at(r)).collect(),
				bytes_slice.iter().map(|r| shape.updated_at(r)).collect(),
			),
		};
		let time: Vec<DateTime> = bytes_slice.iter().filter_map(|r| shape.time(r)).collect();

		Self::with_system(
			columns_vec,
			SystemColumns::new(row_numbers, Vec::new(), created_at, updated_at, time),
		)
	}
}

impl Columns {
	pub fn empty() -> Self {
		Self {
			system: SystemColumns::empty(),
			columns: Vec::new(),
			names: Vec::new(),
		}
	}
}

impl Default for Columns {
	fn default() -> Self {
		Self::empty()
	}
}

impl Columns {
	pub fn extract_by_indices(&self, indices: &[usize]) -> Columns {
		if indices.is_empty() {
			return Columns::empty();
		}

		let mut new_buffers: Vec<ColumnBuffer> = Vec::with_capacity(self.columns.len());
		for col in self.columns.iter() {
			let mut new_data = col.empty_like(indices.len());
			for &idx in indices {
				new_data.push_value(col.get_value(idx));
			}
			new_buffers.push(new_data);
		}

		Columns {
			system: self.system.permute(indices),
			columns: new_buffers,
			names: self.names.clone(),
		}
	}

	pub fn extract_row(&self, index: usize) -> Columns {
		self.extract_by_indices(&[index])
	}

	pub fn append(&mut self, source: Columns) -> Result<()> {
		if source.row_count() == 0 {
			return Ok(());
		}
		if self.columns.is_empty() {
			*self = source;
			return Ok(());
		}

		self.validate_append_compatibility(&source)?;
		self.system.extend(&source.system)?;
		self.extend_data_columns(source.columns)?;
		Ok(())
	}

	#[inline]
	fn validate_append_compatibility(&self, source: &Columns) -> Result<()> {
		if self.columns.len() != source.columns.len() {
			return_internal_error!(
				"Columns::append: column count mismatch (self={}, source={})",
				self.columns.len(),
				source.columns.len()
			);
		}
		Ok(())
	}

	#[inline]
	fn extend_data_columns(&mut self, source_columns: Vec<ColumnBuffer>) -> Result<()> {
		let dest_cols = &mut self.columns;
		reifydb_assertions! {
			let dest_len = dest_cols.len();
			let src_len = source_columns.len();
			assert!(
				dest_len == src_len,
				"append extends destination columns by source index, so a source with more columns than \
				 the destination would index dest_cols out of bounds and panic mid-append, leaving self \
				 partially extended (dest_len={dest_len}, src_len={src_len})"
			);
		}
		for (i, src_col) in source_columns.into_iter().enumerate() {
			dest_cols[i].extend(src_col)?;
		}
		Ok(())
	}

	pub fn concat(batches: Vec<Columns>) -> Result<Option<Columns>> {
		let mut iter = batches.into_iter();
		let mut merged = match iter.next() {
			Some(first) => first,
			None => return Ok(None),
		};
		for cols in iter {
			merged.append(cols)?;
		}
		if merged.row_count() == 0 {
			return Ok(None);
		}
		Ok(Some(merged))
	}

	pub fn remove_row(&mut self, row_number: RowNumber) -> bool {
		let pos = self.row_numbers().iter().position(|&r| r == row_number);
		let Some(idx) = pos else {
			return false;
		};

		let kept_indices: Vec<usize> = (0..self.row_count()).filter(|&i| i != idx).collect();
		*self = self.extract_by_indices(&kept_indices);
		true
	}

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
			system: self.system.clone(),
			columns: new_buffers,
			names: new_names,
		}
	}

	pub fn partition_by_keys<K: Hash + Eq + Clone>(&self, keys: &[K]) -> IndexMap<K, Columns> {
		assert_eq!(keys.len(), self.row_count(), "keys length must match row count");

		let mut key_to_indices: IndexMap<K, Vec<usize>> = IndexMap::new();
		for (idx, key) in keys.iter().enumerate() {
			key_to_indices.entry(key.clone()).or_default().push(idx);
		}

		key_to_indices.into_iter().map(|(key, indices)| (key, self.extract_by_indices(&indices))).collect()
	}

	pub fn from_row(row: &Row) -> Self {
		let mut out = Columns::empty();
		out.reset_from_row(row);
		out
	}

	pub fn reset_from_row(&mut self, row: &Row) {
		let field_count = row.shape.fields().len();

		self.system.clear();
		self.columns.clear();
		self.names.clear();

		self.columns.reserve(field_count);
		self.names.reserve(field_count);

		let (created_at, updated_at) = match row.shape.family() {
			RowFamily::Pod => (None, None),
			_ => (Some(row.shape.created_at(&row.encoded)), Some(row.shape.updated_at(&row.encoded))),
		};

		self.system.push(RowStamps {
			row_number: Some(row.number),
			partition: None,
			created_at,
			updated_at,
			time: row.shape.time(&row.encoded),
		});

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

			if column_type == ValueType::DictionaryId
				&& let ColumnBuffer::DictionaryId(container) = &mut data
				&& let Some(Constraint::Dictionary(dict_id, _)) = field.constraint.constraint()
			{
				container.set_dictionary_id(*dict_id);
			}

			let name = row.shape.get_field_name(idx).expect("RowShape missing name for field");

			self.names.push(Fragment::internal(name));
			self.columns.push(data);
		}
	}
}

#[cfg(test)]
pub mod tests {
	use std::str::FromStr;

	use reifydb_value::value::{
		blob::Blob,
		constraint::{bytes::MaxBytes, precision::Precision, scale::Scale},
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		dictionary::{DictionaryEntryId, DictionaryId},
		duration::Duration,
		identity::IdentityId,
		int::Int,
		time::Time,
		uint::Uint,
		uuid::{Uuid4, Uuid7},
	};
	use uuid::{Timestamp, Uuid};

	use super::*;

	fn uuid7_at(a: u64, b: u16) -> Uuid7 {
		Uuid7::from(Uuid::new_v7(Timestamp::from_gregorian_time(a, b)))
	}

	/// Compares `get_value` of the extraction against the source, so it covers every `ColumnBuffer`
	/// variant without hand-constructing each `Value`.
	fn assert_extract_preserves_values(buffer: ColumnBuffer, indices: &[usize]) {
		let original = Columns::new(vec![ColumnWithName::new("c", buffer)]);
		let extracted = original.extract_by_indices(indices);

		assert_eq!(extracted.len(), 1, "column count must be preserved");
		assert_eq!(extracted.row_count(), indices.len(), "row count must equal number of indices");

		let src = original.data_at(0);
		let dst = extracted.data_at(0);
		assert_eq!(dst.get_type(), src.get_type(), "value type must be preserved");
		for (j, &idx) in indices.iter().enumerate() {
			assert_eq!(
				dst.get_value(j),
				src.get_value(idx),
				"value at extracted row {j} must equal source row {idx}"
			);
		}
	}

	#[test]
	fn extract_by_indices_preserves_bool_values() {
		assert_extract_preserves_values(ColumnBuffer::bool([true, false, true, false]), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_float4_values() {
		assert_extract_preserves_values(ColumnBuffer::float4([1.0f32, 2.5, -3.0, 4.25]), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_float8_values() {
		assert_extract_preserves_values(ColumnBuffer::float8([1.0f64, 2.5, -3.0, 4.25]), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_int1_values() {
		assert_extract_preserves_values(ColumnBuffer::int1([-1i8, 2, -3, 4]), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_int2_values() {
		assert_extract_preserves_values(ColumnBuffer::int2([-1i16, 2, -3, 4]), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_int4_values() {
		assert_extract_preserves_values(ColumnBuffer::int4([-1i32, 2, -3, 4]), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_int8_values() {
		assert_extract_preserves_values(ColumnBuffer::int8([-1i64, 2, -3, 4]), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_int16_values() {
		assert_extract_preserves_values(ColumnBuffer::int16([-1i128, 2, -3, 4]), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_uint1_values() {
		assert_extract_preserves_values(ColumnBuffer::uint1([1u8, 2, 3, 4]), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_uint2_values() {
		assert_extract_preserves_values(ColumnBuffer::uint2([1u16, 2, 3, 4]), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_uint4_values() {
		assert_extract_preserves_values(ColumnBuffer::uint4([1u32, 2, 3, 4]), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_uint8_values() {
		assert_extract_preserves_values(ColumnBuffer::uint8([1u64, 2, 3, 4]), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_uint16_values() {
		assert_extract_preserves_values(ColumnBuffer::uint16([1u128, 2, 3, 4]), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_utf8_values() {
		assert_extract_preserves_values(ColumnBuffer::utf8(["a", "bb", "ccc", "dddd"]), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_date_values() {
		let data = [
			Date::from_ymd(2025, 1, 1).unwrap(),
			Date::from_ymd(2025, 6, 15).unwrap(),
			Date::from_ymd(2024, 12, 31).unwrap(),
			Date::from_ymd(2000, 2, 29).unwrap(),
		];
		assert_extract_preserves_values(ColumnBuffer::date(data), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_datetime_values() {
		let data = [
			DateTime::from_epoch_secs(1000).unwrap(),
			DateTime::from_epoch_secs(2000).unwrap(),
			DateTime::from_epoch_secs(3000).unwrap(),
			DateTime::from_epoch_secs(4000).unwrap(),
		];
		assert_extract_preserves_values(ColumnBuffer::datetime(data), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_time_values() {
		let data = [
			Time::from_hms(0, 0, 0).unwrap(),
			Time::from_hms(12, 30, 45).unwrap(),
			Time::from_hms(23, 59, 59).unwrap(),
			Time::from_hms(6, 15, 0).unwrap(),
		];
		assert_extract_preserves_values(ColumnBuffer::time(data), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_duration_values() {
		let data = [
			Duration::from_days(1).unwrap(),
			Duration::from_days(7).unwrap(),
			Duration::from_days(30).unwrap(),
			Duration::from_days(365).unwrap(),
		];
		assert_extract_preserves_values(ColumnBuffer::duration(data), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_identity_id_values() {
		let data = [IdentityId::root(), IdentityId::system(), IdentityId::anonymous(), IdentityId::root()];
		assert_extract_preserves_values(ColumnBuffer::identity_id(data), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_uuid4_values() {
		let data = [Uuid4::generate(), Uuid4::generate(), Uuid4::generate(), Uuid4::generate()];
		assert_extract_preserves_values(ColumnBuffer::uuid4(data), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_uuid7_values() {
		let data = [uuid7_at(1, 1), uuid7_at(1, 2), uuid7_at(2, 1), uuid7_at(2, 2)];
		assert_extract_preserves_values(ColumnBuffer::uuid7(data), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_blob_values() {
		let data = [
			Blob::new(vec![1]),
			Blob::new(vec![2, 3]),
			Blob::new(vec![4, 5, 6]),
			Blob::new(vec![7, 8, 9, 10]),
		];
		assert_extract_preserves_values(ColumnBuffer::blob(data), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_int_values() {
		let data = [Int::from(-1i64), Int::from(2i64), Int::from(-3i64), Int::from(4i64)];
		assert_extract_preserves_values(ColumnBuffer::int(data), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_uint_values() {
		let data = [Uint::from(1u64), Uint::from(2u64), Uint::from(3u64), Uint::from(4u64)];
		assert_extract_preserves_values(ColumnBuffer::uint(data), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_decimal_values() {
		let data = [
			Decimal::from_str("1.50").unwrap(),
			Decimal::from_str("2.25").unwrap(),
			Decimal::from_str("-3.75").unwrap(),
			Decimal::from_str("4.00").unwrap(),
		];
		assert_extract_preserves_values(ColumnBuffer::decimal(data), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_any_values() {
		let data = [Value::Int4(1), Value::Utf8("two".to_string()), Value::Boolean(true), Value::none()];
		assert_extract_preserves_values(ColumnBuffer::any(data), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_dictionary_id_values() {
		let data = [
			DictionaryEntryId::U2(10),
			DictionaryEntryId::U2(20),
			DictionaryEntryId::U2(30),
			DictionaryEntryId::U2(40),
		];
		assert_extract_preserves_values(ColumnBuffer::dictionary_id(data), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_option_values_including_none() {
		let mut buffer = ColumnBuffer::with_capacity(ValueType::Option(Box::new(ValueType::Int4)), 0);
		buffer.push_value(Value::Int4(1));
		buffer.push_value(Value::none());
		buffer.push_value(Value::Int4(3));
		buffer.push_value(Value::none());
		assert_extract_preserves_values(buffer, &[3, 1, 2, 0]);
	}

	#[test]
	fn extract_by_indices_empty_indices_yields_empty_columns() {
		let original = Columns::new(vec![ColumnWithName::int4("c", [1, 2, 3])]);
		let extracted = original.extract_by_indices(&[]);
		assert_eq!(extracted.row_count(), 0);
		assert!(extracted.is_empty());
	}

	#[test]
	fn extract_by_indices_full_identity_reproduces_all_rows() {
		assert_extract_preserves_values(ColumnBuffer::int4([10, 20, 30, 40]), &[0, 1, 2, 3]);
	}

	#[test]
	fn heap_size_grows_with_row_count() {
		let small = Columns::new(vec![ColumnWithName::int4("c", [1i32, 2, 3, 4])]);
		let large = Columns::new(vec![ColumnWithName::int4("c", 0..4000i32)]);
		assert!(
			large.heap_size() > small.heap_size() + 4000,
			"heap_size must scale with the number of buffered rows (small={}, large={})",
			small.heap_size(),
			large.heap_size()
		);
	}

	#[test]
	fn heap_size_counts_utf8_payload_not_just_row_count() {
		// Two columns with the same row count but different string payloads must not report the same
		// footprint; a budget ignoring varlen content lets a wide-string result blow past the cap.
		let short = Columns::new(vec![ColumnWithName::new("c", ColumnBuffer::utf8(["a", "b", "c"]))]);
		let long_value = "x".repeat(4096);
		let long = Columns::new(vec![ColumnWithName::new(
			"c",
			ColumnBuffer::utf8([long_value.clone(), long_value.clone(), long_value.clone()]),
		)]);
		assert_eq!(short.row_count(), long.row_count(), "same row count is the point of the test");
		assert!(
			long.heap_size() >= short.heap_size() + 3 * 4096,
			"heap_size must account for utf8 payload bytes (short={}, long={})",
			short.heap_size(),
			long.heap_size()
		);
	}

	#[test]
	fn extract_by_indices_duplicate_index_duplicates_row() {
		let original = Columns::new(vec![ColumnWithName::int4("c", [10, 20, 30])]);
		let extracted = original.extract_by_indices(&[1, 1, 1]);
		assert_eq!(extracted.row_count(), 3);
		assert_eq!(extracted.data_at(0).get_value(0), Value::Int4(20));
		assert_eq!(extracted.data_at(0).get_value(1), Value::Int4(20));
		assert_eq!(extracted.data_at(0).get_value(2), Value::Int4(20));
	}

	#[test]
	fn extract_by_indices_extracts_multiple_columns_consistently() {
		let original = Columns::new(vec![
			ColumnWithName::int4("id", [1, 2, 3, 4]),
			ColumnWithName::utf8(
				"name",
				["a".to_string(), "b".to_string(), "c".to_string(), "d".to_string()],
			),
			ColumnWithName::bool("flag", [true, false, true, false]),
		]);
		let extracted = original.extract_by_indices(&[2, 0]);

		assert_eq!(extracted.len(), 3);
		assert_eq!(extracted.row_count(), 2);
		assert_eq!(extracted.column("id").unwrap().data().get_value(0), Value::Int4(3));
		assert_eq!(extracted.column("id").unwrap().data().get_value(1), Value::Int4(1));
		assert_eq!(extracted.column("name").unwrap().data().get_value(0), Value::Utf8("c".to_string()));
		assert_eq!(extracted.column("name").unwrap().data().get_value(1), Value::Utf8("a".to_string()));
		assert_eq!(extracted.column("flag").unwrap().data().get_value(0), Value::Boolean(true));
		assert_eq!(extracted.column("flag").unwrap().data().get_value(1), Value::Boolean(true));
	}

	#[test]
	fn extract_by_indices_extracts_system_columns_in_order() {
		let columns = vec![ColumnWithName::int4("id", [10, 20, 30, 40])];
		let row_numbers = vec![RowNumber::from(1), RowNumber::from(2), RowNumber::from(3), RowNumber::from(4)];
		let created_at = vec![
			DateTime::from_epoch_secs(1000).unwrap(),
			DateTime::from_epoch_secs(2000).unwrap(),
			DateTime::from_epoch_secs(3000).unwrap(),
			DateTime::from_epoch_secs(4000).unwrap(),
		];
		let updated_at = vec![
			DateTime::from_epoch_secs(1100).unwrap(),
			DateTime::from_epoch_secs(2200).unwrap(),
			DateTime::from_epoch_secs(3300).unwrap(),
			DateTime::from_epoch_secs(4400).unwrap(),
		];
		let time = created_at.clone();
		let original = Columns::with_system(
			columns,
			SystemColumns::new(row_numbers, Vec::new(), created_at, updated_at, time),
		);

		let extracted = original.extract_by_indices(&[3, 0]);

		let rns: Vec<RowNumber> = extracted.row_numbers().iter().cloned().collect();
		assert_eq!(rns, vec![RowNumber::from(4), RowNumber::from(1)], "row_numbers must follow indices");
		assert_eq!(
			extracted.created_at().iter().cloned().collect::<Vec<_>>(),
			vec![DateTime::from_epoch_secs(4000).unwrap(), DateTime::from_epoch_secs(1000).unwrap()],
			"created_at must follow indices"
		);
		assert_eq!(
			extracted.updated_at().iter().cloned().collect::<Vec<_>>(),
			vec![DateTime::from_epoch_secs(4400).unwrap(), DateTime::from_epoch_secs(1100).unwrap()],
			"updated_at must follow indices"
		);
	}

	/// Regression: the change accumulator coalesces row-keyed inserts by calling `extract_row`
	/// per row, and a deferred view over a dictionary-encoded column then decodes using the
	/// buffer's `dictionary_id`. If extraction drops that metadata the view can no longer resolve
	/// the dictionary and inserts are silently lost. This pins that `extract_by_indices` carries
	/// the `dictionary_id` through.
	#[test]
	fn extract_by_indices_preserves_dictionary_id_metadata() {
		let mut buffer = ColumnBuffer::dictionary_id([
			DictionaryEntryId::U2(10),
			DictionaryEntryId::U2(20),
			DictionaryEntryId::U2(30),
		]);
		match &mut buffer {
			ColumnBuffer::DictionaryId(container) => container.set_dictionary_id(DictionaryId(42)),
			_ => unreachable!("dictionary_id factory must build a DictionaryId buffer"),
		}

		let original = Columns::new(vec![ColumnWithName::new("token", buffer)]);
		let extracted = original.extract_by_indices(&[2, 0]);

		match extracted.data_at(0) {
			ColumnBuffer::DictionaryId(container) => {
				assert_eq!(
					container.dictionary_id(),
					Some(DictionaryId(42)),
					"dictionary_id metadata must survive extraction"
				);
			}
			other => panic!("expected DictionaryId buffer, got {:?}", other.get_type()),
		}
	}

	#[test]
	fn extract_by_indices_preserves_utf8_max_bytes_metadata() {
		let mut buffer = ColumnBuffer::utf8(["a", "bb", "ccc"]);
		match &mut buffer {
			ColumnBuffer::Utf8 {
				max_bytes,
				..
			} => *max_bytes = MaxBytes::new(255),
			_ => unreachable!(),
		}

		let original = Columns::new(vec![ColumnWithName::new("c", buffer)]);
		let extracted = original.extract_by_indices(&[2, 0]);

		match extracted.data_at(0) {
			ColumnBuffer::Utf8 {
				max_bytes,
				..
			} => assert_eq!(*max_bytes, MaxBytes::new(255), "Utf8 max_bytes must survive extraction"),
			other => panic!("expected Utf8 buffer, got {:?}", other.get_type()),
		}
	}

	#[test]
	fn extract_by_indices_preserves_blob_max_bytes_metadata() {
		let mut buffer = ColumnBuffer::blob([Blob::new(vec![1]), Blob::new(vec![2, 3]), Blob::new(vec![4])]);
		match &mut buffer {
			ColumnBuffer::Blob {
				max_bytes,
				..
			} => *max_bytes = MaxBytes::new(1024),
			_ => unreachable!(),
		}

		let original = Columns::new(vec![ColumnWithName::new("c", buffer)]);
		let extracted = original.extract_by_indices(&[2, 0]);

		match extracted.data_at(0) {
			ColumnBuffer::Blob {
				max_bytes,
				..
			} => assert_eq!(*max_bytes, MaxBytes::new(1024), "Blob max_bytes must survive extraction"),
			other => panic!("expected Blob buffer, got {:?}", other.get_type()),
		}
	}

	#[test]
	fn extract_by_indices_preserves_int_max_bytes_metadata() {
		let mut buffer = ColumnBuffer::int([Int::from(1i64), Int::from(2i64), Int::from(3i64)]);
		match &mut buffer {
			ColumnBuffer::Int {
				max_bytes,
				..
			} => *max_bytes = MaxBytes::new(16),
			_ => unreachable!(),
		}

		let original = Columns::new(vec![ColumnWithName::new("c", buffer)]);
		let extracted = original.extract_by_indices(&[2, 0]);

		match extracted.data_at(0) {
			ColumnBuffer::Int {
				max_bytes,
				..
			} => assert_eq!(*max_bytes, MaxBytes::new(16), "Int max_bytes must survive extraction"),
			other => panic!("expected Int buffer, got {:?}", other.get_type()),
		}
	}

	#[test]
	fn extract_by_indices_preserves_uint_max_bytes_metadata() {
		let mut buffer = ColumnBuffer::uint([Uint::from(1u64), Uint::from(2u64), Uint::from(3u64)]);
		match &mut buffer {
			ColumnBuffer::Uint {
				max_bytes,
				..
			} => *max_bytes = MaxBytes::new(8),
			_ => unreachable!(),
		}

		let original = Columns::new(vec![ColumnWithName::new("c", buffer)]);
		let extracted = original.extract_by_indices(&[2, 0]);

		match extracted.data_at(0) {
			ColumnBuffer::Uint {
				max_bytes,
				..
			} => assert_eq!(*max_bytes, MaxBytes::new(8), "Uint max_bytes must survive extraction"),
			other => panic!("expected Uint buffer, got {:?}", other.get_type()),
		}
	}

	#[test]
	fn extract_by_indices_preserves_decimal_precision_and_scale_metadata() {
		let mut buffer = ColumnBuffer::decimal([
			Decimal::from_str("1.50").unwrap(),
			Decimal::from_str("2.25").unwrap(),
			Decimal::from_str("3.75").unwrap(),
		]);
		match &mut buffer {
			ColumnBuffer::Decimal {
				precision,
				scale,
				..
			} => {
				*precision = Precision::new(10);
				*scale = Scale::new(2);
			}
			_ => unreachable!(),
		}

		let original = Columns::new(vec![ColumnWithName::new("c", buffer)]);
		let extracted = original.extract_by_indices(&[2, 0]);

		match extracted.data_at(0) {
			ColumnBuffer::Decimal {
				precision,
				scale,
				..
			} => {
				assert_eq!(*precision, Precision::new(10), "Decimal precision must survive extraction");
				assert_eq!(*scale, Scale::new(2), "Decimal scale must survive extraction");
			}
			other => panic!("expected Decimal buffer, got {:?}", other.get_type()),
		}
	}

	#[test]
	fn test_single_row_temporal_types() {
		let date = Date::from_ymd(2025, 1, 15).unwrap();
		let datetime = DateTime::from_epoch_secs(1642694400).unwrap();
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
	fn test_single_row_none_of_int4_is_int4_typed() {
		// value_to_buffer must keep the `inner` type a `Value::None` carries; forcing one hardcoded
		// column type would silently mistype every all-none column.
		let columns = Columns::single_row([("n", Value::none_of(ValueType::Int4))]);
		match columns.column("n").unwrap().data().get_value(0) {
			Value::None {
				inner,
			} => assert_eq!(inner, ValueType::Int4),
			other => panic!("expected Value::None, got {other:?}"),
		}
	}

	#[test]
	fn test_single_row_none_of_utf8_is_utf8_typed() {
		let columns = Columns::single_row([("n", Value::none_of(ValueType::Utf8))]);
		match columns.column("n").unwrap().data().get_value(0) {
			Value::None {
				inner,
			} => assert_eq!(inner, ValueType::Utf8),
			other => panic!("expected Value::None, got {other:?}"),
		}
	}

	#[test]
	fn test_single_row_bare_none_is_any_typed() {
		let columns = Columns::single_row([("n", Value::none())]);
		match columns.column("n").unwrap().data().get_value(0) {
			Value::None {
				inner,
			} => assert_eq!(inner, ValueType::Any),
			other => panic!("expected Value::None, got {other:?}"),
		}
	}

	#[test]
	fn test_single_row_none_of_nested_option_collapses_to_base_type() {
		// none_typed unwraps a nested Option(inner) to its base type, so an Option<Option<Duration>>
		// none lands in a Duration-typed column.
		let inner_ty = ValueType::Option(Box::new(ValueType::Duration));
		let columns = Columns::single_row([("n", Value::none_of(inner_ty))]);
		match columns.column("n").unwrap().data().get_value(0) {
			Value::None {
				inner,
			} => assert_eq!(inner, ValueType::Duration),
			other => panic!("expected Value::None, got {other:?}"),
		}
	}

	#[test]
	fn test_single_row_none_of_boolean_is_boolean_typed() {
		// Boolean is value_to_buffer's fallback type, so this case alone proves nothing; it is kept
		// for symmetry with the other inner types above.
		let columns = Columns::single_row([("n", Value::none_of(ValueType::Boolean))]);
		match columns.column("n").unwrap().data().get_value(0) {
			Value::None {
				inner,
			} => assert_eq!(inner, ValueType::Boolean),
			other => panic!("expected Value::None, got {other:?}"),
		}
	}

	#[test]
	fn test_single_row_normal_column_names_work() {
		let columns = Columns::single_row([("normal_column", Value::Int4(42))]);
		assert_eq!(columns.len(), 1);
		assert_eq!(columns.column("normal_column").unwrap().data().get_value(0), Value::Int4(42));
	}

	#[test]
	fn with_row_numbers_leaves_an_absent_sidecar_absent() {
		// A timeless batch must stay timeless; a filled #time reads downstream as a real time zero.
		let columns = Columns::new(vec![ColumnWithName::new("v", ColumnBuffer::int4([1, 2, 3]))])
			.with_row_numbers(vec![RowNumber(1), RowNumber(2), RowNumber(3)]);

		assert_eq!(columns.system.row_numbers().len(), 3);
		assert!(columns.system.time().is_empty(), "#time must stay absent");
		assert!(columns.system.created_at().is_empty(), "created_at must stay absent");
		assert!(columns.system.updated_at().is_empty(), "updated_at must stay absent");
	}

	#[test]
	fn with_row_numbers_keeps_a_populated_sidecar() {
		let stamps = vec![DateTime::from_nanos(10), DateTime::from_nanos(20)];
		let columns = Columns::with_system(
			vec![ColumnWithName::new("v", ColumnBuffer::int4([1, 2]))],
			SystemColumns::new(
				vec![RowNumber(7), RowNumber(8)],
				Vec::new(),
				Vec::new(),
				Vec::new(),
				stamps.clone(),
			),
		)
		.with_row_numbers(vec![RowNumber(1), RowNumber(2)]);

		assert_eq!(columns.system.time(), stamps.as_slice());
		assert_eq!(columns.system.row_numbers(), &[RowNumber(1), RowNumber(2)]);
	}

	#[test]
	#[should_panic(expected = "Columns::with_row_numbers")]
	fn with_row_numbers_panics_on_a_partial_sidecar() {
		// A sidecar that covers only some rows must stop the write, never be padded to fit.
		let columns = Columns::with_system(
			vec![ColumnWithName::new("v", ColumnBuffer::int4([1, 2, 3]))],
			SystemColumns::new(
				vec![RowNumber(1), RowNumber(2), RowNumber(3)],
				Vec::new(),
				Vec::new(),
				Vec::new(),
				vec![DateTime::from_nanos(10), DateTime::from_nanos(20), DateTime::from_nanos(30)],
			),
		);

		let _ = columns.with_row_numbers(vec![RowNumber(1), RowNumber(2)]);
	}
}
