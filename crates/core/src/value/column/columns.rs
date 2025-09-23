// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	collections::HashMap,
	ops::{Deref, Index, IndexMut},
};

use reifydb_type::{Fragment, Value};

use crate::{
	interface::{
		ResolvedColumn,
		resolved::{ResolvedRingBuffer, ResolvedSource, ResolvedTable, ResolvedView},
	},
	util::CowVec,
	value::{
		column::{Column, ColumnComputed, ColumnData, ColumnResolved},
		container::UndefinedContainer,
	},
};

#[derive(Debug, Clone)]
pub struct Columns<'a>(pub CowVec<Column<'a>>);

impl<'a> Deref for Columns<'a> {
	type Target = [Column<'a>];

	fn deref(&self) -> &Self::Target {
		self.0.deref()
	}
}

impl<'a> Index<usize> for Columns<'a> {
	type Output = Column<'a>;

	fn index(&self, index: usize) -> &Self::Output {
		self.0.index(index)
	}
}

impl<'a> IndexMut<usize> for Columns<'a> {
	fn index_mut(&mut self, index: usize) -> &mut Self::Output {
		&mut self.0.make_mut()[index]
	}
}

impl<'a> Columns<'a> {
	pub fn new(columns: Vec<Column<'a>>) -> Self {
		let n = columns.first().map_or(0, |c| c.data().len());
		assert!(columns.iter().all(|c| c.data().len() == n));

		Self(CowVec::new(columns))
	}

	pub fn single_row<'b>(rows: impl IntoIterator<Item = (&'b str, Value)>) -> Columns<'a> {
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
				Value::Interval(v) => ColumnData::interval([v.clone()]),
				Value::RowNumber(v) => ColumnData::row_number([v]),
				Value::IdentityId(v) => ColumnData::identity_id([v]),
				Value::Uuid4(v) => ColumnData::uuid4([v]),
				Value::Uuid7(v) => ColumnData::uuid7([v]),
				Value::Blob(v) => ColumnData::blob([v.clone()]),
				Value::Int(v) => ColumnData::int(vec![v]),
				Value::Uint(v) => ColumnData::uint(vec![v]),
				Value::Decimal(v) => ColumnData::decimal(vec![v]),
			};

			let column = Column::Computed(ColumnComputed {
				name: Fragment::owned_internal(name.to_string()),
				data,
			});
			index.insert(column.qualified_name(), idx);
			columns.push(column);
		}

		Self::new(columns)
	}
}

impl<'a> Columns<'a> {
	pub fn shape(&self) -> (usize, usize) {
		(self.get(0).map(|c| c.data().len()).unwrap_or(0), self.len())
	}

	pub fn into_iter(self) -> impl Iterator<Item = Column<'a>> {
		self.0.into_iter()
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
		self.first().map_or(0, |col| col.data().len())
	}

	pub fn get_row(&self, index: usize) -> Vec<Value> {
		self.iter().map(|col| col.data().get_value(index)).collect()
	}
}

impl<'a> Column<'a> {
	pub fn extend(&mut self, other: Column<'a>) -> crate::Result<()> {
		self.data_mut().extend(other.data().clone())
	}
}

impl<'a> Columns<'a> {
	pub fn from_rows(names: &[&str], result_rows: &[Vec<Value>]) -> Self {
		let column_count = names.len();

		let mut columns: Vec<Column> = names
			.iter()
			.map(|name| {
				Column::Computed(ColumnComputed {
					name: Fragment::owned_internal(name.to_string()),
					data: ColumnData::Undefined(UndefinedContainer::new(0)),
				})
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

impl<'a> Columns<'a> {
	pub fn empty() -> Self {
		Self(CowVec::new(vec![]))
	}

	pub fn from_table(table: &ResolvedTable<'a>) -> Self {
		let source = ResolvedSource::Table(table.clone());

		let columns: Vec<Column> = table
			.columns()
			.iter()
			.map(|col| {
				let column_ident = Fragment::owned_internal(&col.name);
				let resolved_col = ResolvedColumn::new(column_ident, source.clone(), col.clone());
				Column::Resolved(ColumnResolved::new(
					resolved_col,
					ColumnData::with_capacity(col.constraint.get_type(), 0),
				))
			})
			.collect();

		Self::new(columns)
	}

	pub fn from_ring_buffer(ring_buffer: &ResolvedRingBuffer<'a>) -> Self {
		let source = ResolvedSource::RingBuffer(ring_buffer.clone());

		let columns: Vec<Column> = ring_buffer
			.columns()
			.iter()
			.map(|col| {
				let column_ident = Fragment::owned_internal(&col.name);
				let resolved_col = ResolvedColumn::new(column_ident, source.clone(), col.clone());
				Column::Resolved(ColumnResolved::new(
					resolved_col,
					ColumnData::with_capacity(col.constraint.get_type(), 0),
				))
			})
			.collect();

		Self::new(columns)
	}

	pub fn from_view(view: &ResolvedView<'a>) -> Self {
		let source = ResolvedSource::View(view.clone());

		let columns: Vec<Column> = view
			.columns()
			.iter()
			.map(|col| {
				let column_ident = Fragment::owned_internal(&col.name);
				let resolved_col = ResolvedColumn::new(column_ident, source.clone(), col.clone());
				Column::Resolved(ColumnResolved::new(
					resolved_col,
					ColumnData::with_capacity(col.constraint.get_type(), 0),
				))
			})
			.collect();

		Self::new(columns)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_type::{Date, DateTime, Interval, Time};

	use super::*;

	#[test]
	fn test_single_row_temporal_types() {
		let date = Date::from_ymd(2025, 1, 15).unwrap();
		let datetime = DateTime::from_timestamp(1642694400).unwrap();
		let time = Time::from_hms(14, 30, 45).unwrap();
		let interval = Interval::from_days(30);

		let columns = Columns::single_row([
			("date_col", Value::Date(date.clone())),
			("datetime_col", Value::DateTime(datetime.clone())),
			("time_col", Value::Time(time.clone())),
			("interval_col", Value::Interval(interval.clone())),
		]);

		assert_eq!(columns.len(), 4);
		assert_eq!(columns.shape(), (1, 4));

		// Check that the values are correctly stored
		assert_eq!(columns.column("date_col").unwrap().data().get_value(0), Value::Date(date));
		assert_eq!(columns.column("datetime_col").unwrap().data().get_value(0), Value::DateTime(datetime));
		assert_eq!(columns.column("time_col").unwrap().data().get_value(0), Value::Time(time));
		assert_eq!(columns.column("interval_col").unwrap().data().get_value(0), Value::Interval(interval));
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
