// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	reifydb_assertions,
	value::{
		Value,
		date::Date,
		datetime::DateTime,
		duration::Duration,
		identity::IdentityId,
		ordered_f32::OrderedF32,
		ordered_f64::OrderedF64,
		time::Time,
		uuid::{Uuid4, Uuid7},
		value_type::ValueType,
	},
};

use super::RowShape;
use crate::row::bytes::EncodedRowBuilder;

impl RowShape {
	pub fn set_values(&self, row: &mut EncodedRowBuilder, values: &[Value]) {
		reifydb_assertions! {
			assert!(values.len() == self.fields().len());
		}
		for (idx, value) in values.iter().enumerate() {
			self.set_value(row, idx, value)
		}
	}

	pub fn set_value(&self, row: &mut EncodedRowBuilder, index: usize, val: &Value) {
		let field = &self.fields()[index];
		reifydb_assertions! {
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
		}

		let field_type = match field.constraint.get_type() {
			ValueType::Option(inner) => *inner,
			other => other,
		};

		match (field_type, val) {
			(ValueType::Boolean, Value::Boolean(v)) => self.set::<bool>(row, index, *v),
			(
				ValueType::Boolean,
				Value::None {
					..
				},
			) => self.set_none(row, index),

			(ValueType::Float4, Value::Float4(v)) => self.set::<f32>(row, index, v.value()),
			(
				ValueType::Float4,
				Value::None {
					..
				},
			) => self.set_none(row, index),

			(ValueType::Float8, Value::Float8(v)) => self.set::<f64>(row, index, v.value()),
			(
				ValueType::Float8,
				Value::None {
					..
				},
			) => self.set_none(row, index),

			(ValueType::Int1, Value::Int1(v)) => self.set::<i8>(row, index, *v),
			(
				ValueType::Int1,
				Value::None {
					..
				},
			) => self.set_none(row, index),

			(ValueType::Int2, Value::Int2(v)) => self.set::<i16>(row, index, *v),
			(
				ValueType::Int2,
				Value::None {
					..
				},
			) => self.set_none(row, index),

			(ValueType::Int4, Value::Int4(v)) => self.set::<i32>(row, index, *v),
			(
				ValueType::Int4,
				Value::None {
					..
				},
			) => self.set_none(row, index),

			(ValueType::Int8, Value::Int8(v)) => self.set::<i64>(row, index, *v),
			(
				ValueType::Int8,
				Value::None {
					..
				},
			) => self.set_none(row, index),

			(ValueType::Int16, Value::Int16(v)) => self.set::<i128>(row, index, *v),
			(
				ValueType::Int16,
				Value::None {
					..
				},
			) => self.set_none(row, index),

			(ValueType::Utf8, Value::Utf8(v)) => self.set_utf8(row, index, v),
			(
				ValueType::Utf8,
				Value::None {
					..
				},
			) => self.set_none(row, index),

			(ValueType::Uint1, Value::Uint1(v)) => self.set::<u8>(row, index, *v),
			(
				ValueType::Uint1,
				Value::None {
					..
				},
			) => self.set_none(row, index),

			(ValueType::Uint2, Value::Uint2(v)) => self.set::<u16>(row, index, *v),
			(
				ValueType::Uint2,
				Value::None {
					..
				},
			) => self.set_none(row, index),

			(ValueType::Uint4, Value::Uint4(v)) => self.set::<u32>(row, index, *v),
			(
				ValueType::Uint4,
				Value::None {
					..
				},
			) => self.set_none(row, index),

			(ValueType::Uint8, Value::Uint8(v)) => self.set::<u64>(row, index, *v),
			(
				ValueType::Uint8,
				Value::None {
					..
				},
			) => self.set_none(row, index),

			(ValueType::Uint16, Value::Uint16(v)) => self.set::<u128>(row, index, *v),
			(
				ValueType::Uint16,
				Value::None {
					..
				},
			) => self.set_none(row, index),

			(ValueType::Date, Value::Date(v)) => self.set::<Date>(row, index, *v),
			(
				ValueType::Date,
				Value::None {
					..
				},
			) => self.set_none(row, index),

			(ValueType::DateTime, Value::DateTime(v)) => self.set::<DateTime>(row, index, *v),
			(
				ValueType::DateTime,
				Value::None {
					..
				},
			) => self.set_none(row, index),

			(ValueType::Time, Value::Time(v)) => self.set::<Time>(row, index, *v),
			(
				ValueType::Time,
				Value::None {
					..
				},
			) => self.set_none(row, index),

			(ValueType::Duration, Value::Duration(v)) => self.set::<Duration>(row, index, *v),
			(
				ValueType::Duration,
				Value::None {
					..
				},
			) => self.set_none(row, index),

			(ValueType::Uuid4, Value::Uuid4(v)) => self.set::<Uuid4>(row, index, *v),
			(
				ValueType::Uuid4,
				Value::None {
					..
				},
			) => self.set_none(row, index),

			(ValueType::Uuid7, Value::Uuid7(v)) => self.set::<Uuid7>(row, index, *v),
			(
				ValueType::Uuid7,
				Value::None {
					..
				},
			) => self.set_none(row, index),

			(ValueType::Blob, Value::Blob(v)) => self.set_blob(row, index, v),
			(
				ValueType::Blob,
				Value::None {
					..
				},
			) => self.set_none(row, index),

			(ValueType::Int, Value::Int(v)) => self.set_int(row, index, v),
			(ValueType::Uint, Value::Uint(v)) => self.set_uint(row, index, v),
			(
				ValueType::Int,
				Value::None {
					..
				},
			) => self.set_none(row, index),
			(
				ValueType::Uint,
				Value::None {
					..
				},
			) => self.set_none(row, index),

			(ValueType::Decimal, Value::Decimal(v)) => self.set_decimal(row, index, v),
			(
				ValueType::Decimal,
				Value::None {
					..
				},
			) => self.set_none(row, index),
			(ValueType::DictionaryId, Value::DictionaryId(id)) => self.set_dictionary_id(row, index, id),

			(
				ValueType::DictionaryId,
				Value::None {
					..
				},
			) => self.set_none(row, index),

			(ValueType::IdentityId, Value::IdentityId(id)) => self.set::<IdentityId>(row, index, *id),
			(
				ValueType::IdentityId,
				Value::None {
					..
				},
			) => self.set_none(row, index),

			(
				ValueType::Any,
				Value::None {
					..
				},
			) => self.set_none(row, index),
			(ValueType::Any, Value::Any(inner)) => self.set_any(row, index, inner),
			(ty, val) => unreachable!(
				"set_value type mismatch at index {index}: column name={:?} declared_type={ty:?}, value={val:?}",
				field.name,
			),
		}
	}

	pub fn get_value(&self, row: &[u8], index: usize) -> Value {
		let field = &self.fields()[index];
		if !self.is_defined(row, index) {
			return Value::none();
		}
		let field_type = match field.constraint.get_type() {
			ValueType::Option(inner) => *inner,
			other => other,
		};

		match field_type {
			ValueType::Boolean => Value::Boolean(self.get::<bool>(row, index)),
			ValueType::Float4 => OrderedF32::try_from(self.get::<f32>(row, index))
				.map(Value::Float4)
				.unwrap_or(Value::none()),
			ValueType::Float8 => OrderedF64::try_from(self.get::<f64>(row, index))
				.map(Value::Float8)
				.unwrap_or(Value::none()),
			ValueType::Int1 => Value::Int1(self.get::<i8>(row, index)),
			ValueType::Int2 => Value::Int2(self.get::<i16>(row, index)),
			ValueType::Int4 => Value::Int4(self.get::<i32>(row, index)),
			ValueType::Int8 => Value::Int8(self.get::<i64>(row, index)),
			ValueType::Int16 => Value::Int16(self.get::<i128>(row, index)),
			ValueType::Utf8 => Value::Utf8(self.get_utf8(row, index).to_string()),
			ValueType::Uint1 => Value::Uint1(self.get::<u8>(row, index)),
			ValueType::Uint2 => Value::Uint2(self.get::<u16>(row, index)),
			ValueType::Uint4 => Value::Uint4(self.get::<u32>(row, index)),
			ValueType::Uint8 => Value::Uint8(self.get::<u64>(row, index)),
			ValueType::Uint16 => Value::Uint16(self.get::<u128>(row, index)),
			ValueType::Date => Value::Date(self.get::<Date>(row, index)),
			ValueType::DateTime => Value::DateTime(self.get::<DateTime>(row, index)),
			ValueType::Time => Value::Time(self.get::<Time>(row, index)),
			ValueType::Duration => Value::Duration(self.get::<Duration>(row, index)),
			ValueType::IdentityId => Value::IdentityId(self.get::<IdentityId>(row, index)),
			ValueType::Uuid4 => Value::Uuid4(self.get::<Uuid4>(row, index)),
			ValueType::Uuid7 => Value::Uuid7(self.get::<Uuid7>(row, index)),
			ValueType::Blob => Value::Blob(self.get_blob(row, index)),
			ValueType::Int => Value::Int(self.get_int(row, index)),
			ValueType::Uint => Value::Uint(self.get_uint(row, index)),
			ValueType::Decimal => Value::Decimal(self.get_decimal(row, index)),
			ValueType::DictionaryId => Value::DictionaryId(self.get_dictionary_id(row, index)),
			ValueType::Option(_) => unreachable!("Option type already unwrapped"),
			ValueType::Any => Value::Any(Box::new(self.get_any(row, index))),
			ValueType::List(_) => unreachable!("List type cannot be stored in database"),
			ValueType::Record(_) => unreachable!("Record type cannot be stored in database"),
			ValueType::Tuple(_) => unreachable!("Tuple type cannot be stored in database"),
		}
	}
}
