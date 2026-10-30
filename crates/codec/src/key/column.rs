// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	Result,
	value::{
		Value,
		column_view::{ColumnView, ViewData},
		constraint::{precision::Precision, scale::Scale},
		container::{
			decimal_array::decimal_at,
			dictionary_array,
			temporal_array::{dates, datetimes, durations, times},
			uuid_array::{identity_ids, uuid4s, uuid7s},
			wide_int_array::wide_at,
		},
	},
};

use crate::{
	key::{
		serializer::{KeySerializer, keycode_type_descending},
		sort::SortOrder,
	},
	tag::{TypeTag, ValueKind},
};

enum Cell {
	Written,
	Value(Value),
}

pub fn extend_column_with_direction(view: &ColumnView, direction: SortOrder, rows: &mut [KeySerializer]) -> Result<()> {
	let ascending = matches!(direction, SortOrder::Asc);
	match &view.data {
		ViewData::Bool(a) => typed(view, rows, ascending, |row, i, _| {
			row.extend_kind(ValueKind::Boolean).extend_bool(a.value(i));
			Ok(Cell::Written)
		}),
		ViewData::Float4(a) => typed(view, rows, ascending, |row, i, none| {
			let value = a.value(i);
			if value.is_nan() {
				row.extend_raw(none);
			} else {
				row.extend_kind(ValueKind::Float4).extend_f32(if value == 0.0 {
					0.0
				} else {
					value
				});
			}
			Ok(Cell::Written)
		}),
		ViewData::Float8(a) => typed(view, rows, ascending, |row, i, none| {
			let value = a.value(i);
			if value.is_nan() {
				row.extend_raw(none);
			} else {
				row.extend_kind(ValueKind::Float8).extend_f64(if value == 0.0 {
					0.0
				} else {
					value
				});
			}
			Ok(Cell::Written)
		}),
		ViewData::Int1(a) => typed(view, rows, ascending, |row, i, _| {
			row.extend_kind(ValueKind::Int1).extend_i8(a.value(i));
			Ok(Cell::Written)
		}),
		ViewData::Int2(a) => typed(view, rows, ascending, |row, i, _| {
			row.extend_kind(ValueKind::Int2).extend_i16(a.value(i));
			Ok(Cell::Written)
		}),
		ViewData::Int4(a) => typed(view, rows, ascending, |row, i, _| {
			row.extend_kind(ValueKind::Int4).extend_i32(a.value(i));
			Ok(Cell::Written)
		}),
		ViewData::Int8(a) => typed(view, rows, ascending, |row, i, _| {
			row.extend_kind(ValueKind::Int8).extend_i64(a.value(i));
			Ok(Cell::Written)
		}),
		ViewData::Int16(a) => typed(view, rows, ascending, |row, i, _| match wide_at::<i128>(a, i) {
			Some(value) => {
				row.extend_kind(ValueKind::Int16).extend_i128(value);
				Ok(Cell::Written)
			}
			None => Ok(Cell::Value(view.get_value(i))),
		}),
		ViewData::Uint1(a) => typed(view, rows, ascending, |row, i, _| {
			row.extend_kind(ValueKind::Uint1).extend_u8(a.value(i));
			Ok(Cell::Written)
		}),
		ViewData::Uint2(a) => typed(view, rows, ascending, |row, i, _| {
			row.extend_kind(ValueKind::Uint2).extend_u16(a.value(i));
			Ok(Cell::Written)
		}),
		ViewData::Uint4(a) => typed(view, rows, ascending, |row, i, _| {
			row.extend_kind(ValueKind::Uint4).extend_u32(a.value(i));
			Ok(Cell::Written)
		}),
		ViewData::Uint8(a) => typed(view, rows, ascending, |row, i, _| {
			row.extend_kind(ValueKind::Uint8).extend_u64(a.value(i));
			Ok(Cell::Written)
		}),
		ViewData::Uint16(a) => typed(view, rows, ascending, |row, i, _| match wide_at::<u128>(a, i) {
			Some(value) => {
				row.extend_kind(ValueKind::Uint16).extend_u128(value);
				Ok(Cell::Written)
			}
			None => Ok(Cell::Value(view.get_value(i))),
		}),
		ViewData::Utf8 {
			container,
			..
		} => typed(view, rows, ascending, |row, i, _| {
			row.extend_kind(ValueKind::Utf8).extend_str(container.value(i));
			Ok(Cell::Written)
		}),
		ViewData::Blob {
			container,
			..
		} => typed(view, rows, ascending, |row, i, _| {
			row.extend_kind(ValueKind::Blob).extend_bytes(container.value(i));
			Ok(Cell::Written)
		}),
		ViewData::Date(a) => {
			let values = dates(a);
			typed(view, rows, ascending, |row, i, _| {
				row.extend_kind(ValueKind::Date).extend_date(&values[i]);
				Ok(Cell::Written)
			})
		}
		ViewData::DateTime(a) => {
			let values = datetimes(a);
			typed(view, rows, ascending, |row, i, _| {
				row.extend_kind(ValueKind::DateTime).extend_datetime(&values[i]);
				Ok(Cell::Written)
			})
		}
		ViewData::Time(a) => {
			let values = times(a);
			typed(view, rows, ascending, |row, i, _| {
				row.extend_kind(ValueKind::Time).extend_time(&values[i]);
				Ok(Cell::Written)
			})
		}
		ViewData::Duration(a) => {
			let values = durations(a);
			typed(view, rows, ascending, |row, i, _| {
				row.extend_kind(ValueKind::Duration).extend_duration(&values[i]);
				Ok(Cell::Written)
			})
		}
		ViewData::IdentityId(a) => {
			let values = identity_ids(a);
			typed(view, rows, ascending, |row, i, _| {
				row.extend_kind(ValueKind::IdentityId).extend_identity_id(&values[i]);
				Ok(Cell::Written)
			})
		}
		ViewData::Uuid4(a) => {
			let values = uuid4s(a);
			typed(view, rows, ascending, |row, i, _| {
				row.extend_kind(ValueKind::Uuid4).extend_uuid4(&values[i]);
				Ok(Cell::Written)
			})
		}
		ViewData::Uuid7(a) => {
			let values = uuid7s(a);
			typed(view, rows, ascending, |row, i, _| {
				row.extend_kind(ValueKind::Uuid7).extend_uuid7(&values[i]);
				Ok(Cell::Written)
			})
		}
		ViewData::Decimal(d) => typed(view, rows, ascending, |row, i, _| match decimal_at(d, i) {
			Some(value) => {
				row.extend_kind(ValueKind::Decimal).extend_decimal(
					&value,
					Precision::MAX,
					Scale::new(value.scale()),
				)?;
				Ok(Cell::Written)
			}
			None => Ok(Cell::Value(view.get_value(i))),
		}),
		ViewData::DictionaryId {
			container,
			..
		} => typed(view, rows, ascending, |row, i, _| match dictionary_array::get(container, i) {
			Some(id) => {
				row.try_extend_value(&Value::DictionaryId(id))?;
				Ok(Cell::Written)
			}
			None => Ok(Cell::Value(view.get_value(i))),
		}),
		ViewData::Any {
			..
		}
		| ViewData::Digest {
			..
		}
		| ViewData::None {
			..
		} => {
			for (i, row) in rows.iter_mut().enumerate() {
				extend_cell(row, &view.get_value(i), ascending)?;
			}
			Ok(())
		}
	}
}

fn typed(
	view: &ColumnView,
	rows: &mut [KeySerializer],
	ascending: bool,
	mut write: impl FnMut(&mut KeySerializer, usize, &[u8]) -> Result<Cell>,
) -> Result<()> {
	let base = view.base_type();
	let flip = ascending == keycode_type_descending(&base);
	let none = [
		ValueKind::None.byte(),
		TypeTag::of_type(&base).expect("option nesting in a key none inner exceeds the supported depth").byte(),
	];
	for (i, row) in rows.iter_mut().enumerate() {
		if view.none_at(i) {
			let start = row.len();
			row.extend_raw(&none);
			if flip {
				row.complement_from(start);
			}
			continue;
		}
		let start = row.len();
		match write(row, i, &none)? {
			Cell::Written => {
				if flip {
					row.complement_from(start);
				}
			}
			Cell::Value(value) => extend_cell(row, &value, ascending)?,
		}
	}
	Ok(())
}

fn extend_cell(row: &mut KeySerializer, value: &Value, ascending: bool) -> Result<()> {
	let ty = match value {
		Value::None {
			inner,
		} => inner.clone(),
		present => present.get_type(),
	};
	let start = row.len();
	row.try_extend_value(value)?;
	if ascending == keycode_type_descending(&ty) {
		row.complement_from(start);
	}
	Ok(())
}
