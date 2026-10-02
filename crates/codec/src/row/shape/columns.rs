// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	Result,
	error::{Diagnostic, Error},
	value::{
		column_view::{ColumnView, ViewData},
		container::{
			decimal_array::decimal_at,
			dictionary_array, digest_array,
			temporal_array::{dates, datetimes, durations, times},
			uuid_array::{identity_ids, uuid4s, uuid7s},
			wide_int_array::wide_at,
		},
		date::Date,
		datetime::DateTime,
		duration::Duration,
		identity::IdentityId,
		time::Time,
		uuid::{Uuid4, Uuid7},
		value_type::ValueType,
	},
};

use super::RowShape;
use crate::row::bytes::RowBuilder;

impl RowShape {
	pub fn write_columns<B: RowBuilder>(&self, rows: &mut [B], columns: &[ColumnView]) -> Result<()> {
		if columns.len() != self.fields().len() || columns.iter().any(|column| column.len() != rows.len()) {
			return Err(Error(Box::new(Diagnostic {
				code: "INTERNAL_ERROR".to_string(),
				message: format!(
					"cannot write columns of lengths {:?} into {} rows of a shape with {} fields",
					columns.iter().map(ColumnView::len).collect::<Vec<_>>(),
					rows.len(),
					self.fields().len()
				),
				..Diagnostic::default()
			})));
		}
		for (index, (field, view)) in self.fields().iter().zip(columns).enumerate() {
			let field_type = match field.constraint.get_type() {
				ValueType::Option(inner) => *inner,
				other => other,
			};
			match (field_type, &view.data) {
				(ValueType::Boolean, ViewData::Bool(a)) => typed(self, rows, index, view, |row, i| {
					self.set::<bool>(row, index, a.value(i))
				}),
				(ValueType::Float4, ViewData::Float4(a)) => typed(self, rows, index, view, |row, i| {
					let value = a.value(i);
					if value.is_nan() {
						self.set_none(row, index);
					} else {
						self.set::<f32>(
							row,
							index,
							if value == 0.0 {
								0.0
							} else {
								value
							},
						);
					}
				}),
				(ValueType::Float8, ViewData::Float8(a)) => typed(self, rows, index, view, |row, i| {
					let value = a.value(i);
					if value.is_nan() {
						self.set_none(row, index);
					} else {
						self.set::<f64>(
							row,
							index,
							if value == 0.0 {
								0.0
							} else {
								value
							},
						);
					}
				}),
				(ValueType::Int1, ViewData::Int1(a)) => {
					typed(self, rows, index, view, |row, i| self.set::<i8>(row, index, a.value(i)))
				}
				(ValueType::Int2, ViewData::Int2(a)) => {
					typed(self, rows, index, view, |row, i| self.set::<i16>(row, index, a.value(i)))
				}
				(ValueType::Int4, ViewData::Int4(a)) => {
					typed(self, rows, index, view, |row, i| self.set::<i32>(row, index, a.value(i)))
				}
				(ValueType::Int8, ViewData::Int8(a)) => {
					typed(self, rows, index, view, |row, i| self.set::<i64>(row, index, a.value(i)))
				}
				(ValueType::Int16, ViewData::Int16(a)) => {
					typed(self, rows, index, view, |row, i| match wide_at::<i128>(a, i) {
						Some(value) => self.set::<i128>(row, index, value),
						None => self.set_none(row, index),
					})
				}
				(ValueType::Uint1, ViewData::Uint1(a)) => {
					typed(self, rows, index, view, |row, i| self.set::<u8>(row, index, a.value(i)))
				}
				(ValueType::Uint2, ViewData::Uint2(a)) => {
					typed(self, rows, index, view, |row, i| self.set::<u16>(row, index, a.value(i)))
				}
				(ValueType::Uint4, ViewData::Uint4(a)) => {
					typed(self, rows, index, view, |row, i| self.set::<u32>(row, index, a.value(i)))
				}
				(ValueType::Uint8, ViewData::Uint8(a)) => {
					typed(self, rows, index, view, |row, i| self.set::<u64>(row, index, a.value(i)))
				}
				(ValueType::Uint16, ViewData::Uint16(a)) => {
					typed(self, rows, index, view, |row, i| match wide_at::<u128>(a, i) {
						Some(value) => self.set::<u128>(row, index, value),
						None => self.set_none(row, index),
					})
				}
				(
					ValueType::Utf8,
					ViewData::Utf8 {
						container,
						..
					},
				) => typed(self, rows, index, view, |row, i| {
					self.set_utf8(row, index, container.value(i))
				}),
				(
					ValueType::Blob,
					ViewData::Blob {
						container,
						..
					},
				) => typed(self, rows, index, view, |row, i| {
					self.set_blob_from_slice(row, index, container.value(i))
				}),
				(ValueType::Date, ViewData::Date(a)) => {
					let values = dates(a);
					typed(self, rows, index, view, |row, i| self.set::<Date>(row, index, values[i]))
				}
				(ValueType::DateTime, ViewData::DateTime(a)) => {
					let values = datetimes(a);
					typed(self, rows, index, view, |row, i| {
						self.set::<DateTime>(row, index, values[i])
					})
				}
				(ValueType::Time, ViewData::Time(a)) => {
					let values = times(a);
					typed(self, rows, index, view, |row, i| self.set::<Time>(row, index, values[i]))
				}
				(ValueType::Duration, ViewData::Duration(a)) => {
					let values = durations(a);
					typed(self, rows, index, view, |row, i| {
						self.set::<Duration>(row, index, values[i])
					})
				}
				(ValueType::IdentityId, ViewData::IdentityId(a)) => {
					let values = identity_ids(a);
					typed(self, rows, index, view, |row, i| {
						self.set::<IdentityId>(row, index, values[i])
					})
				}
				(ValueType::Uuid4, ViewData::Uuid4(a)) => {
					let values = uuid4s(a);
					typed(self, rows, index, view, |row, i| {
						self.set::<Uuid4>(row, index, values[i])
					})
				}
				(ValueType::Uuid7, ViewData::Uuid7(a)) => {
					let values = uuid7s(a);
					typed(self, rows, index, view, |row, i| {
						self.set::<Uuid7>(row, index, values[i])
					})
				}
				(
					ValueType::Decimal {
						..
					},
					ViewData::Decimal(d),
				) => typed(self, rows, index, view, |row, i| match decimal_at(d, i) {
					Some(value) => self.set_decimal(row, index, &value),
					None => self.set_none(row, index),
				}),
				(
					ValueType::DictionaryId,
					ViewData::DictionaryId {
						container,
						..
					},
				) => typed(self, rows, index, view, |row, i| {
					match dictionary_array::get(container, i) {
						Some(id) => self.set_dictionary_id(row, index, &id),
						None => self.set_none(row, index),
					}
				}),
				(
					ValueType::Digest {
						..
					},
					ViewData::Digest {
						container,
						..
					},
				) => typed(self, rows, index, view, |row, i| match digest_array::get(container, i) {
					Some(digest) => self.set_digest(row, index, &digest),
					None => self.set_none(row, index),
				}),
				_ => {
					for (i, row) in rows.iter_mut().enumerate() {
						self.set_value(row, index, &view.get_value(i));
					}
				}
			}
		}
		Ok(())
	}
}

fn typed<B: RowBuilder>(
	shape: &RowShape,
	rows: &mut [B],
	index: usize,
	view: &ColumnView,
	mut write: impl FnMut(&mut B, usize),
) {
	for (i, row) in rows.iter_mut().enumerate() {
		if view.none_at(i) {
			shape.set_none(row, index);
			continue;
		}
		write(row, i);
	}
}
