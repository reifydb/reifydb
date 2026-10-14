// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem::take;

use postcard::{to_extend, to_stdvec};
use reifydb_codec::{
	key::serializer::KeySerializer,
	tag::{TypeTag, ValueKind},
};
use reifydb_value::{
	Result,
	util::hash::{Hash128, xxh3_128},
	value::{
		Value,
		column_view::{ColumnView, ViewData},
		container::{
			decimal_array::decimal_get_value,
			dictionary_array,
			temporal_array::{dates, datetimes, durations, times},
			uuid_array::{identity_ids, uuid4s, uuid7s},
			varlen_array, wide_int_array,
		},
		to_value::ToValue,
		value_type::ValueType,
	},
};
use serde::Serialize;

const UTF8_VARIANT: u8 = 9;
const BLOB_VARIANT: u8 = 22;

enum Cell<'a> {
	None,
	Value(Value),
	Utf8(&'a str),
	Blob(&'a [u8]),
}

pub fn extend_key(view: &ColumnView, index: usize, serializer: &mut KeySerializer) -> Result<()> {
	if view.none_at(index) {
		serializer.try_extend_value(&Value::None {
			inner: view.base_type(),
		})?;
		return Ok(());
	}
	match &view.data {
		ViewData::Utf8 {
			container,
			..
		} => match varlen_array::get(*container, index) {
			Some(text) => {
				serializer.extend_kind(ValueKind::Utf8).extend_str(text);
			}
			None => {
				serializer.extend_value(&Value::none_of(ValueType::Utf8));
			}
		},
		ViewData::Blob {
			container,
			..
		} => match varlen_array::get(*container, index) {
			Some(bytes) => {
				serializer.extend_kind(ValueKind::Blob).extend_bytes(bytes);
			}
			None => {
				serializer.extend_value(&Value::none_of(ValueType::Blob));
			}
		},
		_ => {
			serializer.try_extend_value(&view.get_value(index))?;
		}
	}
	Ok(())
}

pub fn extend_keys(view: &ColumnView, rows: &mut [KeySerializer]) -> Result<()> {
	match &view.data {
		ViewData::Float4(array) => {
			let none = none_bytes(view);
			for (index, row) in rows.iter_mut().enumerate() {
				let value = array.value(index);
				if view.none_at(index) || value.is_nan() {
					row.extend_raw(&none);
				} else {
					row.extend_kind(ValueKind::Float4).extend_f32(if value == 0.0 {
						0.0
					} else {
						value
					});
				}
			}
		}
		ViewData::Float8(array) => {
			let none = none_bytes(view);
			for (index, row) in rows.iter_mut().enumerate() {
				let value = array.value(index);
				if view.none_at(index) || value.is_nan() {
					row.extend_raw(&none);
				} else {
					row.extend_kind(ValueKind::Float8).extend_f64(if value == 0.0 {
						0.0
					} else {
						value
					});
				}
			}
		}
		_ => {
			let none = Value::None {
				inner: view.base_type(),
			};
			for_each_cell(view, rows.len(), |index, cell| {
				let row = &mut rows[index];
				match cell {
					Cell::None => {
						row.try_extend_value(&none)?;
					}
					Cell::Value(value) => {
						row.try_extend_value(&value)?;
					}
					Cell::Utf8(text) => {
						row.extend_kind(ValueKind::Utf8).extend_str(text);
					}
					Cell::Blob(bytes) => {
						row.extend_kind(ValueKind::Blob).extend_bytes(bytes);
					}
				}
				Ok(())
			})?;
		}
	}
	Ok(())
}

pub fn extend_value_bytes(view: &ColumnView, rows: &mut [Vec<u8>]) -> Result<()> {
	let none = to_stdvec(&Value::None {
		inner: view.base_type(),
	})
	.expect("value postcard is total");
	for_each_cell(view, rows.len(), |index, cell| {
		let row = &mut rows[index];
		match cell {
			Cell::None => row.extend_from_slice(&none),
			Cell::Value(value) => postcard_into(row, &value),
			Cell::Utf8(text) => {
				row.push(UTF8_VARIANT);
				postcard_into(row, text);
			}
			Cell::Blob(bytes) => {
				row.push(BLOB_VARIANT);
				postcard_into(row, bytes);
			}
		}
		Ok(())
	})
}

pub fn value_key_hashes(views: &[ColumnView], rows: usize) -> Result<Vec<Option<Hash128>>> {
	let mut bytes = vec![Vec::new(); rows];
	let mut none = vec![false; rows];
	for view in views {
		extend_value_bytes(view, &mut bytes)?;
		for_each_cell(view, rows, |index, cell| {
			none[index] |= matches!(cell, Cell::None | Cell::Value(Value::None { .. }));
			Ok(())
		})?;
	}
	Ok(bytes.iter().zip(none).map(|(row, none)| (!none).then(|| xxh3_128(row))).collect())
}

fn postcard_into<T: Serialize + ?Sized>(row: &mut Vec<u8>, value: &T) {
	*row = to_extend(value, take(row)).expect("value postcard is total");
}

fn for_each_cell<'a>(
	view: &ColumnView<'a>,
	rows: usize,
	mut visit: impl FnMut(usize, Cell<'a>) -> Result<()>,
) -> Result<()> {
	match &view.data {
		ViewData::Bool(array) => cells(view, rows, &mut visit, |i| Cell::Value(Value::Boolean(array.value(i)))),
		ViewData::Float4(array) => cells(view, rows, &mut visit, |i| Cell::Value(array.value(i).to_value())),
		ViewData::Float8(array) => cells(view, rows, &mut visit, |i| Cell::Value(array.value(i).to_value())),
		ViewData::Int1(array) => cells(view, rows, &mut visit, |i| Cell::Value(array.value(i).to_value())),
		ViewData::Int2(array) => cells(view, rows, &mut visit, |i| Cell::Value(array.value(i).to_value())),
		ViewData::Int4(array) => cells(view, rows, &mut visit, |i| Cell::Value(array.value(i).to_value())),
		ViewData::Int8(array) => cells(view, rows, &mut visit, |i| Cell::Value(array.value(i).to_value())),
		ViewData::Int16(array) => {
			cells(view, rows, &mut visit, |i| Cell::Value(wide_int_array::get_value::<i128>(array, i)))
		}
		ViewData::Uint1(array) => cells(view, rows, &mut visit, |i| Cell::Value(array.value(i).to_value())),
		ViewData::Uint2(array) => cells(view, rows, &mut visit, |i| Cell::Value(array.value(i).to_value())),
		ViewData::Uint4(array) => cells(view, rows, &mut visit, |i| Cell::Value(array.value(i).to_value())),
		ViewData::Uint8(array) => cells(view, rows, &mut visit, |i| Cell::Value(array.value(i).to_value())),
		ViewData::Uint16(array) => {
			cells(view, rows, &mut visit, |i| Cell::Value(wide_int_array::get_value::<u128>(array, i)))
		}
		ViewData::Utf8 {
			container,
			..
		} => {
			let container = *container;
			cells(view, rows, &mut visit, move |i| match varlen_array::get(container, i) {
				Some(text) => Cell::Utf8(text),
				None => Cell::Value(Value::none_of(ValueType::Utf8)),
			})
		}
		ViewData::Blob {
			container,
			..
		} => {
			let container = *container;
			cells(view, rows, &mut visit, move |i| match varlen_array::get(container, i) {
				Some(bytes) => Cell::Blob(bytes),
				None => Cell::Value(Value::none_of(ValueType::Blob)),
			})
		}
		ViewData::Date(array) => {
			let values = dates(array);
			cells(view, rows, &mut visit, |i| Cell::Value(values[i].to_value()))
		}
		ViewData::DateTime(array) => {
			let values = datetimes(array);
			cells(view, rows, &mut visit, |i| Cell::Value(values[i].to_value()))
		}
		ViewData::Time(array) => {
			let values = times(array);
			cells(view, rows, &mut visit, |i| Cell::Value(values[i].to_value()))
		}
		ViewData::Duration(array) => {
			let values = durations(array);
			cells(view, rows, &mut visit, |i| Cell::Value(values[i].to_value()))
		}
		ViewData::IdentityId(array) => {
			let values = identity_ids(array);
			cells(view, rows, &mut visit, |i| Cell::Value(Value::IdentityId(values[i])))
		}
		ViewData::Uuid4(array) => {
			let values = uuid4s(array);
			cells(view, rows, &mut visit, |i| Cell::Value(values[i].to_value()))
		}
		ViewData::Uuid7(array) => {
			let values = uuid7s(array);
			cells(view, rows, &mut visit, |i| Cell::Value(values[i].to_value()))
		}
		ViewData::Decimal(decimals) => {
			cells(view, rows, &mut visit, |i| Cell::Value(decimal_get_value(decimals, i)))
		}
		ViewData::DictionaryId {
			container,
			..
		} => cells(view, rows, &mut visit, |i| Cell::Value(dictionary_array::get_value(container, i))),
		_ => cells(view, rows, &mut visit, |i| Cell::Value(view.get_value(i))),
	}
}

fn cells<'a>(
	view: &ColumnView<'a>,
	rows: usize,
	visit: &mut impl FnMut(usize, Cell<'a>) -> Result<()>,
	cell: impl Fn(usize) -> Cell<'a>,
) -> Result<()> {
	let nulls = view.logical_nulls();
	for index in 0..rows {
		match nulls.as_ref().is_some_and(|nulls| !(index < nulls.len() && nulls.is_valid(index))) {
			true => visit(index, Cell::None)?,
			false => visit(index, cell(index))?,
		}
	}
	Ok(())
}

fn none_bytes(view: &ColumnView) -> [u8; 2] {
	[
		ValueKind::None.byte(),
		TypeTag::of_type(&view.base_type())
			.expect("option nesting in a key none inner exceeds the supported depth")
			.byte(),
	]
}
