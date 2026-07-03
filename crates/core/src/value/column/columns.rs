// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	hash::Hash,
	mem,
	ops::{Index, IndexMut},
};

use indexmap::IndexMap;
use reifydb_value::{
	Result,
	fragment::Fragment,
	reifydb_assertions,
	util::cowvec::CowVec,
	value::{Value, constraint::Constraint, datetime::DateTime, row_number::RowNumber, value_type::ValueType},
};
use serde::{Deserialize, Serialize};

use crate::{
	encoded::{
		row::EncodedRow,
		shape::{RowShape, RowShapeField},
	},
	interface::catalog::column::Column as CatalogColumn,
	return_internal_error,
	row::Row,
	value::column::{
		ColumnBuffer, ColumnWithName, buffer::pool::ColumnBufferPool, data::Column, headers::ColumnHeaders,
	},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
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
		Value::Any(v) => ColumnBuffer::any(vec![v]),
		Value::Type(v) => ColumnBuffer::any(vec![Box::new(Value::Type(v))]),
		Value::List(v) => ColumnBuffer::any(vec![Box::new(Value::List(v))]),
		Value::Record(v) => ColumnBuffer::any(vec![Box::new(Value::Record(v))]),
		Value::Tuple(v) => ColumnBuffer::any(vec![Box::new(Value::Tuple(v))]),
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
			names.push(Fragment::internal(name));
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
		assert!(
			self.row_numbers.is_empty() || self.row_numbers.len() == n,
			"{ctx}: Columns.row_numbers.len() = {} but columns[0].len() = {n}",
			self.row_numbers.len(),
		);
		assert!(
			self.created_at.is_empty() || self.created_at.len() == n,
			"{ctx}: Columns.created_at.len() = {} but columns[0].len() = {n}",
			self.created_at.len(),
		);
		assert!(
			self.updated_at.is_empty() || self.updated_at.len() == n,
			"{ctx}: Columns.updated_at.len() = {} but columns[0].len() = {n}",
			self.updated_at.len(),
		);
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
			row_numbers: CowVec::new(Vec::new()),
			created_at: CowVec::new(Vec::new()),
			updated_at: CowVec::new(Vec::new()),
			columns: CowVec::new(buffers),
			names: CowVec::new(name_vec),
		}
	}

	pub fn from_encoded_rows(shape: &RowShape, ids: &[RowNumber], rows: &[EncodedRow]) -> Self {
		assert_eq!(ids.len(), rows.len(), "ids length must match rows length");
		let fields = shape.fields();
		let row_count = rows.len();

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

		for encoded in rows {
			for (i, _) in fields.iter().enumerate() {
				columns_vec[i].data.push_value(shape.get_value(encoded, i));
			}
		}

		let row_numbers: Vec<RowNumber> = ids.to_vec();
		let created_at: Vec<DateTime> =
			rows.iter().map(|r| DateTime::from_nanos(r.created_at_nanos())).collect();
		let updated_at: Vec<DateTime> =
			rows.iter().map(|r| DateTime::from_nanos(r.updated_at_nanos())).collect();

		Self::with_system_columns(columns_vec, row_numbers, created_at, updated_at)
	}
}

impl Columns {
	pub fn empty() -> Self {
		Self {
			row_numbers: CowVec::new(Vec::new()),
			created_at: CowVec::new(Vec::new()),
			updated_at: CowVec::new(Vec::new()),
			columns: CowVec::new(Vec::new()),
			names: CowVec::new(Vec::new()),
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

	pub fn extract_row(&self, index: usize) -> Columns {
		self.extract_by_indices(&[index])
	}

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

	pub fn append_all(&mut self, source: Columns) -> Result<()> {
		if source.row_count() == 0 {
			return Ok(());
		}
		if self.columns.is_empty() {
			*self = source;
			return Ok(());
		}

		self.validate_append_compatibility(&source)?;
		self.extend_data_columns(source.columns)?;
		self.extend_system_columns(&source.row_numbers, &source.created_at, &source.updated_at);
		Ok(())
	}

	#[inline]
	fn validate_append_compatibility(&self, source: &Columns) -> Result<()> {
		if self.columns.len() != source.columns.len() {
			return_internal_error!(
				"Columns::append_all: column count mismatch (self={}, source={})",
				self.columns.len(),
				source.columns.len()
			);
		}

		if self.row_numbers.is_empty() != source.row_numbers.is_empty() {
			return_internal_error!(
				"Columns::append_all: row_numbers population mismatch (self_empty={}, source_empty={})",
				self.row_numbers.is_empty(),
				source.row_numbers.is_empty()
			);
		}
		if self.created_at.is_empty() != source.created_at.is_empty() {
			return_internal_error!(
				"Columns::append_all: created_at population mismatch (self_empty={}, source_empty={})",
				self.created_at.is_empty(),
				source.created_at.is_empty()
			);
		}
		if self.updated_at.is_empty() != source.updated_at.is_empty() {
			return_internal_error!(
				"Columns::append_all: updated_at population mismatch (self_empty={}, source_empty={})",
				self.updated_at.is_empty(),
				source.updated_at.is_empty()
			);
		}
		Ok(())
	}

	#[inline]
	fn extend_data_columns(&mut self, source_columns: CowVec<ColumnBuffer>) -> Result<()> {
		let dest_cols = self.columns.make_mut();
		let source_cols = source_columns.into_inner();
		reifydb_assertions! {
			let dest_len = dest_cols.len();
			let src_len = source_cols.len();
			assert!(
				dest_len == src_len,
				"append_all extends destination columns by source index, so a source with more columns than \
				 the destination would index dest_cols out of bounds and panic mid-append, leaving self \
				 partially extended (dest_len={dest_len}, src_len={src_len})"
			);
		}
		for (i, src_col) in source_cols.into_iter().enumerate() {
			dest_cols[i].extend(src_col)?;
		}
		Ok(())
	}

	#[inline]
	fn extend_system_columns(
		&mut self,
		source_row_numbers: &CowVec<RowNumber>,
		source_created_at: &CowVec<DateTime>,
		source_updated_at: &CowVec<DateTime>,
	) {
		if !source_row_numbers.is_empty() {
			self.row_numbers.extend_from_slice(source_row_numbers.as_slice());
		}
		if !source_created_at.is_empty() {
			self.created_at.extend_from_slice(source_created_at.as_slice());
		}
		if !source_updated_at.is_empty() {
			self.updated_at.extend_from_slice(source_updated_at.as_slice());
		}
	}

	pub fn concat(batches: Vec<Columns>) -> Result<Option<Columns>> {
		let mut iter = batches.into_iter();
		let mut merged = match iter.next() {
			Some(first) => first,
			None => return Ok(None),
		};
		for cols in iter {
			merged.append_all(cols)?;
		}
		if merged.row_count() == 0 {
			return Ok(None);
		}
		Ok(Some(merged))
	}

	pub fn remove_row(&mut self, row_number: RowNumber) -> bool {
		let pos = self.row_numbers.iter().position(|&r| r == row_number);
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
			row_numbers: self.row_numbers.clone(),
			created_at: self.created_at.clone(),
			updated_at: self.updated_at.clone(),
			columns: CowVec::new(new_buffers),
			names: CowVec::new(new_names),
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

		self.row_numbers.clear();
		self.created_at.clear();
		self.updated_at.clear();
		self.columns.clear();
		self.names.clear();

		self.columns.make_mut().reserve(field_count);
		self.names.make_mut().reserve(field_count);

		self.row_numbers.push(row.number);
		self.created_at.push(DateTime::from_nanos(row.encoded.created_at_nanos()));
		self.updated_at.push(DateTime::from_nanos(row.encoded.updated_at_nanos()));

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

	pub fn reset_from_row_with_pool(&mut self, row: &Row, pool: &ColumnBufferPool) {
		let field_count = row.shape.fields().len();

		self.row_numbers.clear();
		self.created_at.clear();
		self.updated_at.clear();
		self.names.clear();

		self.row_numbers.push(row.number);
		self.created_at.push(DateTime::from_nanos(row.encoded.created_at_nanos()));
		self.updated_at.push(DateTime::from_nanos(row.encoded.updated_at_nanos()));

		let columns_vec = self.columns.make_mut();
		let names_vec = self.names.make_mut();

		while columns_vec.len() > field_count {
			if let Some(buf) = columns_vec.pop() {
				pool.release(buf);
			}
		}

		columns_vec.reserve(field_count);
		names_vec.reserve(field_count);

		for (idx, field) in row.shape.fields().iter().enumerate() {
			let value = row.shape.get_value(&row.encoded, idx);

			let column_type = if matches!(value, Value::None { .. }) {
				field.constraint.get_type()
			} else {
				value.get_type()
			};

			if idx < columns_vec.len() {
				if columns_vec[idx].get_type() == column_type {
					columns_vec[idx].clear();
				} else {
					let replacement = if column_type.is_option() {
						ColumnBuffer::none_typed(column_type.clone(), 0)
					} else {
						pool.acquire(&column_type, 1)
					};
					let old = mem::replace(&mut columns_vec[idx], replacement);
					pool.release(old);
				}
			} else {
				let fresh = if column_type.is_option() {
					ColumnBuffer::none_typed(column_type.clone(), 0)
				} else {
					pool.acquire(&column_type, 1)
				};
				columns_vec.push(fresh);
			}

			columns_vec[idx].push_value(value);

			if column_type == ValueType::DictionaryId
				&& let ColumnBuffer::DictionaryId(container) = &mut columns_vec[idx]
				&& let Some(Constraint::Dictionary(dict_id, _)) = field.constraint.constraint()
			{
				container.set_dictionary_id(*dict_id);
			}

			let name = row.shape.get_field_name(idx).expect("RowShape missing name for field");
			names_vec.push(Fragment::internal(name));
		}
	}

	pub fn push_row(&mut self, row: &Row) {
		let field_count = row.shape.fields().len();

		if self.columns.is_empty() {
			self.columns.make_mut().reserve(field_count);
			self.names.make_mut().reserve(field_count);
			self.row_numbers.push(row.number);
			self.created_at.push(DateTime::from_nanos(row.encoded.created_at_nanos()));
			self.updated_at.push(DateTime::from_nanos(row.encoded.updated_at_nanos()));

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
		} else if self.columns.len() == field_count {
			let columns_vec = self.columns.make_mut();
			for (idx, column) in columns_vec.iter_mut().enumerate() {
				let value = row.shape.get_value(&row.encoded, idx);
				column.push_value(value);
			}
			self.row_numbers.push(row.number);
			self.created_at.push(DateTime::from_nanos(row.encoded.created_at_nanos()));
			self.updated_at.push(DateTime::from_nanos(row.encoded.updated_at_nanos()));
		}
	}

	pub fn push_rows(&mut self, rows: &[Row]) {
		let Some(first) = rows.first() else {
			return;
		};
		if !self.columns.is_empty() {
			for row in rows {
				self.push_row(row);
			}
			return;
		}

		let capacity = rows.len();
		let field_count = first.shape.fields().len();
		self.columns.make_mut().reserve(field_count);
		self.names.make_mut().reserve(field_count);
		self.row_numbers.make_mut().reserve(capacity);
		self.created_at.make_mut().reserve(capacity);
		self.updated_at.make_mut().reserve(capacity);

		for (idx, field) in first.shape.fields().iter().enumerate() {
			let value = first.shape.get_value(&first.encoded, idx);

			let column_type = if matches!(value, Value::None { .. }) {
				field.constraint.get_type()
			} else {
				value.get_type()
			};

			let mut data = ColumnBuffer::with_capacity(column_type.clone(), capacity);

			if column_type == ValueType::DictionaryId
				&& let ColumnBuffer::DictionaryId(container) = &mut data
				&& let Some(Constraint::Dictionary(dict_id, _)) = field.constraint.constraint()
			{
				container.set_dictionary_id(*dict_id);
			}

			let name = first.shape.get_field_name(idx).expect("RowShape missing name for field");
			self.names.push(Fragment::internal(name));
			self.columns.push(data);
		}

		let columns_vec = self.columns.make_mut();
		for row in rows {
			for (idx, column) in columns_vec.iter_mut().enumerate() {
				column.push_value(row.shape.get_value(&row.encoded, idx));
			}
		}
		for row in rows {
			self.row_numbers.push(row.number);
			self.created_at.push(DateTime::from_nanos(row.encoded.created_at_nanos()));
			self.updated_at.push(DateTime::from_nanos(row.encoded.updated_at_nanos()));
		}
	}

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
		ordered_f64::OrderedF64,
		time::Time,
		uint::Uint,
		uuid::{Uuid4, Uuid7},
	};
	use uuid::{Timestamp, Uuid};

	use super::*;

	fn uuid7_at(a: u64, b: u16) -> Uuid7 {
		Uuid7::from(Uuid::new_v7(Timestamp::from_gregorian_time(a, b)))
	}

	/// Builds a one-column `Columns` from `buffer`, extracts `indices`, and asserts the extracted
	/// column reports the right row count, keeps its value type, and reproduces the source value at
	/// every requested index in order. This is the type-agnostic core check: it compares
	/// `get_value` of the extraction against `get_value` of the source so it works for every
	/// `ColumnBuffer` variant without hand-constructing each `Value`.
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
			DateTime::from_timestamp(1000).unwrap(),
			DateTime::from_timestamp(2000).unwrap(),
			DateTime::from_timestamp(3000).unwrap(),
			DateTime::from_timestamp(4000).unwrap(),
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
		let data = [
			Box::new(Value::Int4(1)),
			Box::new(Value::Utf8("two".to_string())),
			Box::new(Value::Boolean(true)),
			Box::new(Value::none()),
		];
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
			DateTime::from_timestamp(1000).unwrap(),
			DateTime::from_timestamp(2000).unwrap(),
			DateTime::from_timestamp(3000).unwrap(),
			DateTime::from_timestamp(4000).unwrap(),
		];
		let updated_at = vec![
			DateTime::from_timestamp(1100).unwrap(),
			DateTime::from_timestamp(2200).unwrap(),
			DateTime::from_timestamp(3300).unwrap(),
			DateTime::from_timestamp(4400).unwrap(),
		];
		let original = Columns::with_system_columns(columns, row_numbers, created_at, updated_at);

		let extracted = original.extract_by_indices(&[3, 0]);

		let rns: Vec<RowNumber> = extracted.row_numbers.iter().cloned().collect();
		assert_eq!(rns, vec![RowNumber::from(4), RowNumber::from(1)], "row_numbers must follow indices");
		assert_eq!(
			extracted.created_at.iter().cloned().collect::<Vec<_>>(),
			vec![DateTime::from_timestamp(4000).unwrap(), DateTime::from_timestamp(1000).unwrap()],
			"created_at must follow indices"
		);
		assert_eq!(
			extracted.updated_at.iter().cloned().collect::<Vec<_>>(),
			vec![DateTime::from_timestamp(4400).unwrap(), DateTime::from_timestamp(1100).unwrap()],
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

	// value_to_buffer must respect the actual `inner` type carried by a `Value::None`, not force
	// every None into a single hardcoded column type. `Value::PartialEq` now compares `inner`
	// (previously all `Value::None` compared equal regardless of type), so a wrong inner type here
	// would silently mistype every "all None" column.

	#[test]
	fn test_single_row_none_of_int4_is_int4_typed() {
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
		// ColumnBuffer::none_typed already unwraps a nested Option(inner) type to its base type
		// (there is no separate column representation for Option<Option<T>>), so a value that is
		// itself Option<Option<Duration>>::None ends up as a Duration-typed None column.
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
		// Boolean is also value_to_buffer's old hardcoded default, so this case alone would not
		// have caught the bug; kept for symmetry with the other inner types above.
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
	fn push_rows_matches_sequential_push_row_for_multiple_rows() {
		// push_rows pre-sizes the column buffers to the row count instead of growing them
		// one row at a time like push_row. The CDC producer relies on the two producing an
		// identical Columns; this pins that invariant for the multi-row case, which is the
		// only case where push_rows takes its own (pre-sizing) branch.
		let shape = RowShape::new(vec![
			RowShapeField::unconstrained("id".to_string(), ValueType::Int4),
			RowShapeField::unconstrained("label".to_string(), ValueType::Utf8),
		]);

		let make = |number: u64, id: i32, label: &str| {
			let mut encoded = shape.allocate();
			shape.set_values(&mut encoded, &[Value::Int4(id), Value::Utf8(label.to_string())]);
			Row {
				number: RowNumber::from(number),
				encoded,
				shape: shape.clone(),
			}
		};

		let rows = vec![make(1, 10, "a"), make(2, 20, "bb"), make(3, 30, "ccc")];

		let mut sequential = Columns::empty();
		for row in &rows {
			sequential.push_row(row);
		}

		let mut bulk = Columns::empty();
		bulk.push_rows(&rows);

		assert_eq!(bulk.row_count(), 3);
		assert_eq!(bulk.len(), sequential.len());
		assert_eq!(bulk.row_count(), sequential.row_count());

		for i in 0..sequential.len() {
			assert_eq!(bulk.name_at(i), sequential.name_at(i), "column {i} name diverged");
			assert_eq!(
				bulk.data_at(i).get_type(),
				sequential.data_at(i).get_type(),
				"column {i} type diverged"
			);
		}
		for r in 0..sequential.row_count() {
			assert_eq!(bulk.get_row(r), sequential.get_row(r), "row {r} values diverged");
		}

		let bulk_numbers: Vec<RowNumber> = bulk.row_numbers.iter().cloned().collect();
		let seq_numbers: Vec<RowNumber> = sequential.row_numbers.iter().cloned().collect();
		assert_eq!(bulk_numbers, seq_numbers, "row numbers diverged");
	}

	#[test]
	fn push_rows_on_empty_slice_is_a_noop() {
		let mut columns = Columns::empty();
		columns.push_rows(&[]);
		assert!(columns.is_empty());
		assert_eq!(columns.row_count(), 0);
	}

	#[test]
	fn push_rows_preserves_values_when_first_row_is_none_on_option_field() {
		// Regression: push_rows used to pre-size Option columns to length=capacity (all
		// None), then push capacity more values on top, producing a 2x-long buffer whose
		// first half (the readable half) was all None. Manifested when projecting sumtype
		// variant fields through a deferred view: the row at index 1 of an INSERT batch
		// would lose its variant payload whenever row 0's value for that field was None
		// (because row 0 carried a different variant). The bulk and sequential paths must
		// produce identical Columns even when the first row's value at an Option field is
		// None, since field.constraint.get_type() is consulted in that case and would hit
		// the Option branch.
		let shape = RowShape::new(vec![
			RowShapeField::unconstrained("id".to_string(), ValueType::Int4),
			RowShapeField::unconstrained(
				"opt_val".to_string(),
				ValueType::Option(Box::new(ValueType::Float8)),
			),
		]);

		let make = |number: u64, id: i32, opt: Value| {
			let mut encoded = shape.allocate();
			shape.set_values(&mut encoded, &[Value::Int4(id), opt]);
			Row {
				number: RowNumber::from(number),
				encoded,
				shape: shape.clone(),
			}
		};

		let v = Value::Float8(OrderedF64::try_from(3.0).unwrap());
		let rows = vec![make(1, 1, Value::none()), make(2, 2, v.clone()), make(3, 3, Value::none())];

		let mut sequential = Columns::empty();
		for row in &rows {
			sequential.push_row(row);
		}

		let mut bulk = Columns::empty();
		bulk.push_rows(&rows);

		assert_eq!(bulk.row_count(), 3);
		assert_eq!(bulk.row_count(), sequential.row_count());
		for r in 0..sequential.row_count() {
			assert_eq!(bulk.get_row(r), sequential.get_row(r), "row {r} values diverged");
		}

		// The defining assertion: the real value at row 1 must survive the bulk path.
		let opt_col = bulk.column("opt_val").unwrap();
		assert_eq!(opt_col.data().get_value(0), Value::none_of(ValueType::Float8));
		assert_eq!(opt_col.data().get_value(1), v);
		assert_eq!(opt_col.data().get_value(2), Value::none_of(ValueType::Float8));
		assert_eq!(opt_col.data().len(), 3, "Option column has more entries than rows pushed");
	}
}
