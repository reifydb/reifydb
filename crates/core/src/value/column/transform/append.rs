// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{
	return_error,
	storage::DataBitVec,
	util::bitvec::BitVec,
	value::{
		Value,
		blob::Blob,
		constraint::Constraint,
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		duration::Duration,
		int::Int,
		row_number::RowNumber,
		time::Time,
		r#type::Type,
		uint::Uint,
		uuid::{Uuid4, Uuid7},
	},
};

use crate::{
	encoded::{encoded::EncodedValues, schema::Schema},
	error::diagnostic::engine::frame_error,
	value::column::{ColumnData, columns::Columns},
};

impl Columns {
	pub fn append_columns(&mut self, other: Columns) -> reifydb_type::Result<()> {
		if self.len() != other.len() {
			return_error!(frame_error("mismatched column count".to_string()));
		}

		// Append encoded numbers from the other columns
		if !other.row_numbers.is_empty() {
			self.row_numbers.make_mut().extend(other.row_numbers.iter().copied());
		}

		let columns = self.columns.make_mut();
		for (i, (l, r)) in columns.iter_mut().zip(other.columns.into_iter()).enumerate() {
			if l.name() != r.name() {
				return_error!(frame_error(format!(
					"column name mismatch at index {}: '{}' vs '{}'",
					i,
					l.name().text(),
					r.name().text(),
				)));
			}
			l.extend(r)?;
		}
		Ok(())
	}
}

impl Columns {
	pub fn append_rows(
		&mut self,
		schema: &Schema,
		rows: impl IntoIterator<Item = EncodedValues>,
		row_numbers: Vec<RowNumber>,
	) -> reifydb_type::Result<()> {
		if self.len() != schema.field_count() {
			return_error!(frame_error(format!(
				"mismatched column count: expected {}, got {}",
				self.len(),
				schema.field_count()
			)));
		}

		let rows: Vec<EncodedValues> = rows.into_iter().collect();

		// Verify row_numbers length if provided
		if !row_numbers.is_empty() && row_numbers.len() != rows.len() {
			return_error!(frame_error(format!(
				"row_numbers length {} does not match rows length {}",
				row_numbers.len(),
				rows.len()
			)));
		}

		// Append row numbers if provided
		if !row_numbers.is_empty() {
			self.row_numbers.make_mut().extend(row_numbers);
		}

		// Handle all-none Option column conversion to properly-typed Option column
		let columns = self.columns.make_mut();
		for (index, column) in columns.iter_mut().enumerate() {
			let field = schema.get_field(index).unwrap();
			let is_all_none = if let ColumnData::Option {
				bitvec,
				..
			} = column.data()
			{
				DataBitVec::count_ones(bitvec) == 0
			} else {
				false
			};
			if is_all_none {
				let size = column.data().len();
				let new_data = match field.constraint.get_type() {
					Type::Boolean => ColumnData::bool_with_bitvec(
						vec![false; size],
						BitVec::repeat(size, false),
					),
					Type::Float4 => ColumnData::float4_with_bitvec(
						vec![0.0f32; size],
						BitVec::repeat(size, false),
					),
					Type::Float8 => ColumnData::float8_with_bitvec(
						vec![0.0f64; size],
						BitVec::repeat(size, false),
					),
					Type::Int1 => ColumnData::int1_with_bitvec(
						vec![0i8; size],
						BitVec::repeat(size, false),
					),
					Type::Int2 => ColumnData::int2_with_bitvec(
						vec![0i16; size],
						BitVec::repeat(size, false),
					),
					Type::Int4 => ColumnData::int4_with_bitvec(
						vec![0i32; size],
						BitVec::repeat(size, false),
					),
					Type::Int8 => ColumnData::int8_with_bitvec(
						vec![0i64; size],
						BitVec::repeat(size, false),
					),
					Type::Int16 => ColumnData::int16_with_bitvec(
						vec![0i128; size],
						BitVec::repeat(size, false),
					),
					Type::Utf8 => ColumnData::utf8_with_bitvec(
						vec![String::new(); size],
						BitVec::repeat(size, false),
					),
					Type::Uint1 => ColumnData::uint1_with_bitvec(
						vec![0u8; size],
						BitVec::repeat(size, false),
					),
					Type::Uint2 => ColumnData::uint2_with_bitvec(
						vec![0u16; size],
						BitVec::repeat(size, false),
					),
					Type::Uint4 => ColumnData::uint4_with_bitvec(
						vec![0u32; size],
						BitVec::repeat(size, false),
					),
					Type::Uint8 => ColumnData::uint8_with_bitvec(
						vec![0u64; size],
						BitVec::repeat(size, false),
					),
					Type::Uint16 => ColumnData::uint16_with_bitvec(
						vec![0u128; size],
						BitVec::repeat(size, false),
					),
					Type::Date => ColumnData::date_with_bitvec(
						vec![Date::default(); size],
						BitVec::repeat(size, false),
					),
					Type::DateTime => ColumnData::datetime_with_bitvec(
						vec![DateTime::default(); size],
						BitVec::repeat(size, false),
					),
					Type::Time => ColumnData::time_with_bitvec(
						vec![Time::default(); size],
						BitVec::repeat(size, false),
					),
					Type::Duration => ColumnData::duration_with_bitvec(
						vec![Duration::default(); size],
						BitVec::repeat(size, false),
					),
					Type::Option(_) => column.data().clone(),
					Type::IdentityId => ColumnData::identity_id_with_bitvec(
						vec![Default::default(); size],
						BitVec::repeat(size, false),
					),
					Type::Uuid4 => ColumnData::uuid4_with_bitvec(
						vec![Uuid4::from(uuid::Uuid::nil()); size],
						BitVec::repeat(size, false),
					),
					Type::Uuid7 => ColumnData::uuid7_with_bitvec(
						vec![Uuid7::from(uuid::Uuid::nil()); size],
						BitVec::repeat(size, false),
					),
					Type::Blob => ColumnData::blob_with_bitvec(
						vec![Blob::new(vec![]); size],
						BitVec::repeat(size, false),
					),
					Type::Int => ColumnData::int_with_bitvec(
						vec![Int::default(); size],
						BitVec::repeat(size, false),
					),
					Type::Uint => ColumnData::uint_with_bitvec(
						vec![Uint::default(); size],
						BitVec::repeat(size, false),
					),
					Type::Decimal {
						..
					} => ColumnData::decimal_with_bitvec(
						vec![Decimal::from(0); size],
						BitVec::repeat(size, false),
					),
					Type::DictionaryId => {
						let mut col_data = ColumnData::dictionary_id_with_bitvec(
							vec![Default::default(); size],
							BitVec::repeat(size, false),
						);
						if let ColumnData::DictionaryId(container) = &mut col_data {
							if let Some(Constraint::Dictionary(dict_id, _)) =
								field.constraint.constraint()
							{
								container.set_dictionary_id(*dict_id);
							}
						}
						col_data
					}
					Type::Any => ColumnData::any_with_bitvec(
						vec![Box::new(Value::none()); size],
						BitVec::repeat(size, false),
					),
				};

				*column = column.with_new_data(new_data);
			}

			// Set dictionary_id on DictionaryId containers from schema constraint
			if let ColumnData::DictionaryId(container) = column.data_mut() {
				if container.dictionary_id().is_none() {
					if let Some(Constraint::Dictionary(dict_id, _)) = field.constraint.constraint()
					{
						container.set_dictionary_id(*dict_id);
					}
				}
			}
		}

		// Append rows using Schema methods
		for row in &rows {
			// Check if all fields are defined
			let all_defined = (0..schema.field_count()).all(|i| row.is_defined(i));

			if all_defined {
				self.append_all_defined_from_schema(schema, &row)?;
			} else {
				self.append_fallback_from_schema(schema, &row)?;
			}
		}

		Ok(())
	}

	fn append_all_defined_from_schema(&mut self, schema: &Schema, row: &EncodedValues) -> reifydb_type::Result<()> {
		let columns = self.columns.make_mut();
		for (index, column) in columns.iter_mut().enumerate() {
			let field = schema.get_field(index).unwrap();
			match (column.data_mut(), field.constraint.get_type()) {
				// Handle Option-wrapped columns by unwrapping and pushing to inner + bitvec
				(
					ColumnData::Option {
						inner,
						bitvec,
					},
					_ty,
				) => {
					let value = schema.get_value(&row, index);
					if matches!(value, Value::None { .. }) {
						inner.push_none();
						DataBitVec::push(bitvec, false);
					} else {
						inner.push_value(value);
						DataBitVec::push(bitvec, true);
					}
				}
				(ColumnData::Bool(container), Type::Boolean) => {
					container.push(schema.get_bool(&row, index));
				}
				(ColumnData::Float4(container), Type::Float4) => {
					container.push(schema.get_f32(&row, index));
				}
				(ColumnData::Float8(container), Type::Float8) => {
					container.push(schema.get_f64(&row, index));
				}
				(ColumnData::Int1(container), Type::Int1) => {
					container.push(schema.get_i8(&row, index));
				}
				(ColumnData::Int2(container), Type::Int2) => {
					container.push(schema.get_i16(&row, index));
				}
				(ColumnData::Int4(container), Type::Int4) => {
					container.push(schema.get_i32(&row, index));
				}
				(ColumnData::Int8(container), Type::Int8) => {
					container.push(schema.get_i64(&row, index));
				}
				(ColumnData::Int16(container), Type::Int16) => {
					container.push(schema.get_i128(&row, index));
				}
				(
					ColumnData::Utf8 {
						container,
						..
					},
					Type::Utf8,
				) => {
					container.push(schema.get_utf8(&row, index).to_string());
				}
				(ColumnData::Uint1(container), Type::Uint1) => {
					container.push(schema.get_u8(&row, index));
				}
				(ColumnData::Uint2(container), Type::Uint2) => {
					container.push(schema.get_u16(&row, index));
				}
				(ColumnData::Uint4(container), Type::Uint4) => {
					container.push(schema.get_u32(&row, index));
				}
				(ColumnData::Uint8(container), Type::Uint8) => {
					container.push(schema.get_u64(&row, index));
				}
				(ColumnData::Uint16(container), Type::Uint16) => {
					container.push(schema.get_u128(&row, index));
				}
				(ColumnData::Date(container), Type::Date) => {
					container.push(schema.get_date(&row, index));
				}
				(ColumnData::DateTime(container), Type::DateTime) => {
					container.push(schema.get_datetime(&row, index));
				}
				(ColumnData::Time(container), Type::Time) => {
					container.push(schema.get_time(&row, index));
				}
				(ColumnData::Duration(container), Type::Duration) => {
					container.push(schema.get_duration(&row, index));
				}
				(ColumnData::Uuid4(container), Type::Uuid4) => {
					container.push(schema.get_uuid4(&row, index));
				}
				(ColumnData::Uuid7(container), Type::Uuid7) => {
					container.push(schema.get_uuid7(&row, index));
				}
				(
					ColumnData::Blob {
						container,
						..
					},
					Type::Blob,
				) => {
					container.push(schema.get_blob(&row, index));
				}
				(
					ColumnData::Int {
						container,
						..
					},
					Type::Int,
				) => {
					container.push(schema.get_int(&row, index));
				}
				(
					ColumnData::Uint {
						container,
						..
					},
					Type::Uint,
				) => {
					container.push(schema.get_uint(&row, index));
				}
				(
					ColumnData::Decimal {
						container,
						..
					},
					Type::Decimal {
						..
					},
				) => {
					container.push(schema.get_decimal(&row, index));
				}
				(ColumnData::DictionaryId(container), Type::DictionaryId) => {
					match schema.get_value(&row, index) {
						Value::DictionaryId(id) => container.push(id),
						_ => container.push_default(),
					}
				}
				(_, v) => {
					return_error!(frame_error(format!(
						"type mismatch for column '{}'({}): incompatible with value {}",
						column.name().text(),
						column.data().get_type(),
						v
					)));
				}
			}
		}
		Ok(())
	}

	fn append_fallback_from_schema(&mut self, schema: &Schema, row: &EncodedValues) -> reifydb_type::Result<()> {
		let columns = self.columns.make_mut();
		for (index, column) in columns.iter_mut().enumerate() {
			let field = schema.get_field(index).unwrap();

			// If the value is undefined, use ColumnData-level push_none
			// which correctly promotes bare containers to Option-wrapped
			if !row.is_defined(index) {
				column.data_mut().push_none();
				continue;
			}

			match (column.data_mut(), field.constraint.get_type()) {
				// Handle Option-wrapped columns
				(
					ColumnData::Option {
						inner,
						bitvec,
					},
					_ty,
				) => {
					let value = schema.get_value(row, index);
					inner.push_value(value);
					DataBitVec::push(bitvec, true);
				}
				(ColumnData::Bool(container), Type::Boolean) => {
					container.push(schema.get_bool(row, index));
				}
				(ColumnData::Float4(container), Type::Float4) => {
					container.push(schema.get_f32(row, index));
				}
				(ColumnData::Float8(container), Type::Float8) => {
					container.push(schema.get_f64(row, index));
				}
				(ColumnData::Int1(container), Type::Int1) => {
					container.push(schema.get_i8(row, index));
				}
				(ColumnData::Int2(container), Type::Int2) => {
					container.push(schema.get_i16(row, index));
				}
				(ColumnData::Int4(container), Type::Int4) => {
					container.push(schema.get_i32(row, index));
				}
				(ColumnData::Int8(container), Type::Int8) => {
					container.push(schema.get_i64(row, index));
				}
				(ColumnData::Int16(container), Type::Int16) => {
					container.push(schema.get_i128(row, index));
				}
				(
					ColumnData::Utf8 {
						container,
						..
					},
					Type::Utf8,
				) => {
					container.push(schema.get_utf8(row, index).to_string());
				}
				(ColumnData::Uint1(container), Type::Uint1) => {
					container.push(schema.get_u8(row, index));
				}
				(ColumnData::Uint2(container), Type::Uint2) => {
					container.push(schema.get_u16(row, index));
				}
				(ColumnData::Uint4(container), Type::Uint4) => {
					container.push(schema.get_u32(row, index));
				}
				(ColumnData::Uint8(container), Type::Uint8) => {
					container.push(schema.get_u64(row, index));
				}
				(ColumnData::Uint16(container), Type::Uint16) => {
					container.push(schema.get_u128(row, index));
				}
				(ColumnData::Date(container), Type::Date) => {
					container.push(schema.get_date(row, index));
				}
				(ColumnData::DateTime(container), Type::DateTime) => {
					container.push(schema.get_datetime(row, index));
				}
				(ColumnData::Time(container), Type::Time) => {
					container.push(schema.get_time(row, index));
				}
				(ColumnData::Duration(container), Type::Duration) => {
					container.push(schema.get_duration(row, index));
				}
				(ColumnData::Uuid4(container), Type::Uuid4) => {
					container.push(schema.get_uuid4(row, index));
				}
				(ColumnData::Uuid7(container), Type::Uuid7) => {
					container.push(schema.get_uuid7(row, index));
				}
				(
					ColumnData::Int {
						container,
						..
					},
					Type::Int,
				) => {
					container.push(schema.get_int(row, index));
				}
				(
					ColumnData::Uint {
						container,
						..
					},
					Type::Uint,
				) => {
					container.push(schema.get_uint(row, index));
				}
				(
					ColumnData::Decimal {
						container,
						..
					},
					Type::Decimal {
						..
					},
				) => {
					container.push(schema.get_decimal(row, index));
				}
				(ColumnData::DictionaryId(container), Type::DictionaryId) => {
					match schema.get_value(row, index) {
						Value::DictionaryId(id) => container.push(id),
						_ => container.push_default(),
					}
				}
				(l, r) => unreachable!("{:#?} {:#?}", l, r),
			}
		}
		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	mod columns {
		use reifydb_type::value::{
			r#type::Type,
			uuid::{Uuid4, Uuid7},
		};
		use uuid::{Timestamp, Uuid};

		use crate::value::column::{Column, ColumnData, columns::Columns};

		#[test]
		fn test_boolean() {
			let mut test_instance1 = Columns::new(vec![Column::bool_with_bitvec("id", [true], [false])]);

			let test_instance2 = Columns::new(vec![Column::bool_with_bitvec("id", [false], [true])]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0].data(),
				&ColumnData::bool_with_bitvec([true, false], [false, true])
			);
		}

		#[test]
		fn test_float4() {
			let mut test_instance1 = Columns::new(vec![Column::float4("id", [1.0f32, 2.0])]);

			let test_instance2 =
				Columns::new(vec![Column::float4_with_bitvec("id", [3.0f32, 4.0], [true, false])]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0].data(),
				&ColumnData::float4_with_bitvec([1.0f32, 2.0, 3.0, 4.0], [true, true, true, false])
			);
		}

		#[test]
		fn test_float8() {
			let mut test_instance1 = Columns::new(vec![Column::float8("id", [1.0f64, 2.0])]);

			let test_instance2 =
				Columns::new(vec![Column::float8_with_bitvec("id", [3.0f64, 4.0], [true, false])]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0].data(),
				&ColumnData::float8_with_bitvec([1.0f64, 2.0, 3.0, 4.0], [true, true, true, false])
			);
		}

		#[test]
		fn test_int1() {
			let mut test_instance1 = Columns::new(vec![Column::int1("id", [1, 2])]);

			let test_instance2 = Columns::new(vec![Column::int1_with_bitvec("id", [3, 4], [true, false])]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0].data(),
				&ColumnData::int1_with_bitvec([1, 2, 3, 4], [true, true, true, false])
			);
		}

		#[test]
		fn test_int2() {
			let mut test_instance1 = Columns::new(vec![Column::int2("id", [1, 2])]);

			let test_instance2 = Columns::new(vec![Column::int2_with_bitvec("id", [3, 4], [true, false])]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0].data(),
				&ColumnData::int2_with_bitvec([1, 2, 3, 4], [true, true, true, false])
			);
		}

		#[test]
		fn test_int4() {
			let mut test_instance1 = Columns::new(vec![Column::int4("id", [1, 2])]);

			let test_instance2 = Columns::new(vec![Column::int4_with_bitvec("id", [3, 4], [true, false])]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0].data(),
				&ColumnData::int4_with_bitvec([1, 2, 3, 4], [true, true, true, false])
			);
		}

		#[test]
		fn test_int8() {
			let mut test_instance1 = Columns::new(vec![Column::int8("id", [1, 2])]);

			let test_instance2 = Columns::new(vec![Column::int8_with_bitvec("id", [3, 4], [true, false])]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0].data(),
				&ColumnData::int8_with_bitvec([1, 2, 3, 4], [true, true, true, false])
			);
		}

		#[test]
		fn test_int16() {
			let mut test_instance1 = Columns::new(vec![Column::int16("id", [1, 2])]);

			let test_instance2 = Columns::new(vec![Column::int16_with_bitvec("id", [3, 4], [true, false])]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0].data(),
				&ColumnData::int16_with_bitvec([1, 2, 3, 4], [true, true, true, false])
			);
		}

		#[test]
		fn test_string() {
			let mut test_instance1 = Columns::new(vec![Column::utf8_with_bitvec(
				"id",
				vec!["a".to_string(), "b".to_string()],
				[true, true],
			)]);

			let test_instance2 = Columns::new(vec![Column::utf8_with_bitvec(
				"id",
				vec!["c".to_string(), "d".to_string()],
				[true, false],
			)]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0].data(),
				&ColumnData::utf8_with_bitvec(
					vec!["a".to_string(), "b".to_string(), "c".to_string(), "d".to_string()],
					vec![true, true, true, false]
				)
			);
		}

		#[test]
		fn test_uint1() {
			let mut test_instance1 = Columns::new(vec![Column::uint1("id", [1, 2])]);

			let test_instance2 = Columns::new(vec![Column::uint1_with_bitvec("id", [3, 4], [true, false])]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0].data(),
				&ColumnData::uint1_with_bitvec([1, 2, 3, 4], [true, true, true, false])
			);
		}

		#[test]
		fn test_uint2() {
			let mut test_instance1 = Columns::new(vec![Column::uint2("id", [1, 2])]);

			let test_instance2 = Columns::new(vec![Column::uint2_with_bitvec("id", [3, 4], [true, false])]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0].data(),
				&ColumnData::uint2_with_bitvec([1, 2, 3, 4], [true, true, true, false])
			);
		}

		#[test]
		fn test_uint4() {
			let mut test_instance1 = Columns::new(vec![Column::uint4("id", [1, 2])]);

			let test_instance2 = Columns::new(vec![Column::uint4_with_bitvec("id", [3, 4], [true, false])]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0].data(),
				&ColumnData::uint4_with_bitvec([1, 2, 3, 4], [true, true, true, false])
			);
		}

		#[test]
		fn test_uint8() {
			let mut test_instance1 = Columns::new(vec![Column::uint8("id", [1, 2])]);

			let test_instance2 = Columns::new(vec![Column::uint8_with_bitvec("id", [3, 4], [true, false])]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0].data(),
				&ColumnData::uint8_with_bitvec([1, 2, 3, 4], [true, true, true, false])
			);
		}

		#[test]
		fn test_uint16() {
			let mut test_instance1 = Columns::new(vec![Column::uint16("id", [1, 2])]);

			let test_instance2 =
				Columns::new(vec![Column::uint16_with_bitvec("id", [3, 4], [true, false])]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0].data(),
				&ColumnData::uint16_with_bitvec([1, 2, 3, 4], [true, true, true, false])
			);
		}

		#[test]
		fn test_uuid4() {
			use uuid::Uuid;

			let uuid1 = Uuid4::from(Uuid::new_v4());
			let uuid2 = Uuid4::from(Uuid::new_v4());
			let uuid3 = Uuid4::from(Uuid::new_v4());
			let uuid4 = Uuid4::from(Uuid::new_v4());

			let mut test_instance1 = Columns::new(vec![Column::uuid4("id", [uuid1, uuid2])]);

			let test_instance2 =
				Columns::new(vec![Column::uuid4_with_bitvec("id", [uuid3, uuid4], [true, false])]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0].data(),
				&ColumnData::uuid4_with_bitvec([uuid1, uuid2, uuid3, uuid4], [true, true, true, false])
			);
		}

		#[test]
		fn test_uuid7() {
			let uuid1 = Uuid7::from(Uuid::new_v7(Timestamp::from_gregorian(1, 1)));
			let uuid2 = Uuid7::from(Uuid::new_v7(Timestamp::from_gregorian(1, 2)));
			let uuid3 = Uuid7::from(Uuid::new_v7(Timestamp::from_gregorian(2, 1)));
			let uuid4 = Uuid7::from(Uuid::new_v7(Timestamp::from_gregorian(2, 2)));

			let mut test_instance1 = Columns::new(vec![Column::uuid7("id", [uuid1, uuid2])]);

			let test_instance2 =
				Columns::new(vec![Column::uuid7_with_bitvec("id", [uuid3, uuid4], [true, false])]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0].data(),
				&ColumnData::uuid7_with_bitvec([uuid1, uuid2, uuid3, uuid4], [true, true, true, false])
			);
		}

		#[test]
		fn test_with_undefined_lr_promotes_correctly() {
			let mut test_instance1 =
				Columns::new(vec![Column::int2_with_bitvec("id", [1, 2], [true, false])]);

			let test_instance2 = Columns::new(vec![Column::undefined_typed("id", Type::Boolean, 2)]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0].data(),
				&ColumnData::int2_with_bitvec([1, 2, 0, 0], [true, false, false, false])
			);
		}

		#[test]
		fn test_with_undefined_l_promotes_correctly() {
			let mut test_instance1 = Columns::new(vec![Column::undefined_typed("score", Type::Boolean, 2)]);

			let test_instance2 =
				Columns::new(vec![Column::int2_with_bitvec("score", [10, 20], [true, false])]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0].data(),
				&ColumnData::int2_with_bitvec([0, 0, 10, 20], [false, false, true, false])
			);
		}

		#[test]
		fn test_fails_on_column_count_mismatch() {
			let mut test_instance1 = Columns::new(vec![Column::int2("id", [1])]);

			let test_instance2 = Columns::new(vec![
				Column::int2("id", [2]),
				Column::utf8("name", vec!["Bob".to_string()]),
			]);

			let result = test_instance1.append_columns(test_instance2);
			assert!(result.is_err());
		}

		#[test]
		fn test_fails_on_column_name_mismatch() {
			let mut test_instance1 = Columns::new(vec![Column::int2("id", [1])]);

			let test_instance2 = Columns::new(vec![Column::int2("wrong", [2])]);

			let result = test_instance1.append_columns(test_instance2);
			assert!(result.is_err());
		}

		#[test]
		fn test_fails_on_type_mismatch() {
			let mut test_instance1 = Columns::new(vec![Column::int2("id", [1])]);

			let test_instance2 = Columns::new(vec![Column::utf8("id", vec!["A".to_string()])]);

			let result = test_instance1.append_columns(test_instance2);
			assert!(result.is_err());
		}
	}

	mod row {
		use reifydb_type::{
			fragment::Fragment,
			util::bitvec::BitVec,
			value::{Value, ordered_f32::OrderedF32, ordered_f64::OrderedF64, r#type::Type},
		};

		use crate::{
			encoded::schema::Schema,
			value::column::{Column, ColumnData, columns::Columns},
		};

		#[test]
		fn test_before_undefined_bool() {
			let mut test_instance =
				Columns::new(vec![Column::undefined_typed("test_col", Type::Boolean, 2)]);

			let schema = Schema::testing(&[Type::Boolean]);
			let mut row = schema.allocate();
			schema.set_values(&mut row, &[Value::Boolean(true)]);

			test_instance.append_rows(&schema, [row], vec![]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::bool_with_bitvec(
					[false, false, true],
					BitVec::from_slice(&[false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_float4() {
			let mut test_instance =
				Columns::new(vec![Column::undefined_typed("test_col", Type::Boolean, 2)]);
			let schema = Schema::testing(&[Type::Float4]);
			let mut row = schema.allocate();
			schema.set_values(&mut row, &[Value::Float4(OrderedF32::try_from(1.5).unwrap())]);
			test_instance.append_rows(&schema, [row], vec![]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::float4_with_bitvec(
					[0.0, 0.0, 1.5],
					BitVec::from_slice(&[false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_float8() {
			let mut test_instance =
				Columns::new(vec![Column::undefined_typed("test_col", Type::Boolean, 2)]);
			let schema = Schema::testing(&[Type::Float8]);
			let mut row = schema.allocate();
			schema.set_values(&mut row, &[Value::Float8(OrderedF64::try_from(2.25).unwrap())]);
			test_instance.append_rows(&schema, [row], vec![]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::float8_with_bitvec(
					[0.0, 0.0, 2.25],
					BitVec::from_slice(&[false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_int1() {
			let mut test_instance =
				Columns::new(vec![Column::undefined_typed("test_col", Type::Boolean, 2)]);
			let schema = Schema::testing(&[Type::Int1]);
			let mut row = schema.allocate();
			schema.set_values(&mut row, &[Value::Int1(42)]);
			test_instance.append_rows(&schema, [row], vec![]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::int1_with_bitvec([0, 0, 42], BitVec::from_slice(&[false, false, true]))
			);
		}

		#[test]
		fn test_before_undefined_int2() {
			let mut test_instance =
				Columns::new(vec![Column::undefined_typed("test_col", Type::Boolean, 2)]);
			let schema = Schema::testing(&[Type::Int2]);
			let mut row = schema.allocate();
			schema.set_values(&mut row, &[Value::Int2(-1234)]);
			test_instance.append_rows(&schema, [row], vec![]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::int2_with_bitvec([0, 0, -1234], BitVec::from_slice(&[false, false, true]))
			);
		}

		#[test]
		fn test_before_undefined_int4() {
			let mut test_instance =
				Columns::new(vec![Column::undefined_typed("test_col", Type::Boolean, 2)]);
			let schema = Schema::testing(&[Type::Int4]);
			let mut row = schema.allocate();
			schema.set_values(&mut row, &[Value::Int4(56789)]);
			test_instance.append_rows(&schema, [row], vec![]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::int4_with_bitvec([0, 0, 56789], BitVec::from_slice(&[false, false, true]))
			);
		}

		#[test]
		fn test_before_undefined_int8() {
			let mut test_instance =
				Columns::new(vec![Column::undefined_typed("test_col", Type::Boolean, 2)]);
			let schema = Schema::testing(&[Type::Int8]);
			let mut row = schema.allocate();
			schema.set_values(&mut row, &[Value::Int8(-987654321)]);
			test_instance.append_rows(&schema, [row], vec![]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::int8_with_bitvec(
					[0, 0, -987654321],
					BitVec::from_slice(&[false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_int16() {
			let mut test_instance =
				Columns::new(vec![Column::undefined_typed("test_col", Type::Boolean, 2)]);
			let schema = Schema::testing(&[Type::Int16]);
			let mut row = schema.allocate();
			schema.set_values(&mut row, &[Value::Int16(123456789012345678901234567890i128)]);
			test_instance.append_rows(&schema, [row], vec![]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::int16_with_bitvec(
					[0, 0, 123456789012345678901234567890i128],
					BitVec::from_slice(&[false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_string() {
			let mut test_instance =
				Columns::new(vec![Column::undefined_typed("test_col", Type::Boolean, 2)]);
			let schema = Schema::testing(&[Type::Utf8]);
			let mut row = schema.allocate();
			schema.set_values(&mut row, &[Value::Utf8("reifydb".into())]);
			test_instance.append_rows(&schema, [row], vec![]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::utf8_with_bitvec(
					["".to_string(), "".to_string(), "reifydb".to_string()],
					BitVec::from_slice(&[false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_uint1() {
			let mut test_instance =
				Columns::new(vec![Column::undefined_typed("test_col", Type::Boolean, 2)]);
			let schema = Schema::testing(&[Type::Uint1]);
			let mut row = schema.allocate();
			schema.set_values(&mut row, &[Value::Uint1(255)]);
			test_instance.append_rows(&schema, [row], vec![]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::uint1_with_bitvec([0, 0, 255], BitVec::from_slice(&[false, false, true]))
			);
		}

		#[test]
		fn test_before_undefined_uint2() {
			let mut test_instance =
				Columns::new(vec![Column::undefined_typed("test_col", Type::Boolean, 2)]);
			let schema = Schema::testing(&[Type::Uint2]);
			let mut row = schema.allocate();
			schema.set_values(&mut row, &[Value::Uint2(65535)]);
			test_instance.append_rows(&schema, [row], vec![]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::uint2_with_bitvec([0, 0, 65535], BitVec::from_slice(&[false, false, true]))
			);
		}

		#[test]
		fn test_before_undefined_uint4() {
			let mut test_instance =
				Columns::new(vec![Column::undefined_typed("test_col", Type::Boolean, 2)]);
			let schema = Schema::testing(&[Type::Uint4]);
			let mut row = schema.allocate();
			schema.set_values(&mut row, &[Value::Uint4(4294967295)]);
			test_instance.append_rows(&schema, [row], vec![]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::uint4_with_bitvec(
					[0, 0, 4294967295],
					BitVec::from_slice(&[false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_uint8() {
			let mut test_instance =
				Columns::new(vec![Column::undefined_typed("test_col", Type::Boolean, 2)]);
			let schema = Schema::testing(&[Type::Uint8]);
			let mut row = schema.allocate();
			schema.set_values(&mut row, &[Value::Uint8(18446744073709551615)]);
			test_instance.append_rows(&schema, [row], vec![]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::uint8_with_bitvec(
					[0, 0, 18446744073709551615],
					BitVec::from_slice(&[false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_uint16() {
			let mut test_instance =
				Columns::new(vec![Column::undefined_typed("test_col", Type::Boolean, 2)]);
			let schema = Schema::testing(&[Type::Uint16]);
			let mut row = schema.allocate();
			schema.set_values(&mut row, &[Value::Uint16(340282366920938463463374607431768211455u128)]);
			test_instance.append_rows(&schema, [row], vec![]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::uint16_with_bitvec(
					[0, 0, 340282366920938463463374607431768211455u128],
					BitVec::from_slice(&[false, false, true])
				)
			);
		}

		#[test]
		fn test_mismatched_columns() {
			let mut test_instance = Columns::new(vec![]);

			let schema = Schema::testing(&[Type::Int2]);
			let mut row = schema.allocate();
			schema.set_values(&mut row, &[Value::Int2(2)]);

			let err = test_instance.append_rows(&schema, [row], vec![]).err().unwrap();
			assert!(err.to_string().contains("mismatched column count: expected 0, got 1"));
		}

		#[test]
		fn test_ok() {
			let mut test_instance = test_instance_with_columns();

			let schema = Schema::testing(&[Type::Int2, Type::Boolean]);
			let mut row_one = schema.allocate();
			schema.set_values(&mut row_one, &[Value::Int2(2), Value::Boolean(true)]);
			let mut row_two = schema.allocate();
			schema.set_values(&mut row_two, &[Value::Int2(3), Value::Boolean(false)]);

			test_instance.append_rows(&schema, [row_one, row_two], vec![]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::int2([1, 2, 3]));
			assert_eq!(*test_instance[1].data(), ColumnData::bool([true, true, false]));
		}

		#[test]
		fn test_all_defined_bool() {
			let mut test_instance = Columns::new(vec![Column::bool("test_col", Vec::<bool>::new())]);

			let schema = Schema::testing(&[Type::Boolean]);
			let mut row_one = schema.allocate();
			schema.set_bool(&mut row_one, 0, true);
			let mut row_two = schema.allocate();
			schema.set_bool(&mut row_two, 0, false);

			test_instance.append_rows(&schema, [row_one, row_two], vec![]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::bool([true, false]));
		}

		#[test]
		fn test_all_defined_float4() {
			let mut test_instance = Columns::new(vec![Column::float4("test_col", Vec::<f32>::new())]);

			let schema = Schema::testing(&[Type::Float4]);
			let mut row_one = schema.allocate();
			schema.set_values(&mut row_one, &[Value::Float4(OrderedF32::try_from(1.0).unwrap())]);
			let mut row_two = schema.allocate();
			schema.set_values(&mut row_two, &[Value::Float4(OrderedF32::try_from(2.0).unwrap())]);

			test_instance.append_rows(&schema, [row_one, row_two], vec![]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::float4([1.0, 2.0]));
		}

		#[test]
		fn test_all_defined_float8() {
			let mut test_instance = Columns::new(vec![Column::float8("test_col", Vec::<f64>::new())]);

			let schema = Schema::testing(&[Type::Float8]);
			let mut row_one = schema.allocate();
			schema.set_values(&mut row_one, &[Value::Float8(OrderedF64::try_from(1.0).unwrap())]);
			let mut row_two = schema.allocate();
			schema.set_values(&mut row_two, &[Value::Float8(OrderedF64::try_from(2.0).unwrap())]);

			test_instance.append_rows(&schema, [row_one, row_two], vec![]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::float8([1.0, 2.0]));
		}

		#[test]
		fn test_all_defined_int1() {
			let mut test_instance = Columns::new(vec![Column::int1("test_col", Vec::<i8>::new())]);

			let schema = Schema::testing(&[Type::Int1]);
			let mut row_one = schema.allocate();
			schema.set_values(&mut row_one, &[Value::Int1(1)]);
			let mut row_two = schema.allocate();
			schema.set_values(&mut row_two, &[Value::Int1(2)]);

			test_instance.append_rows(&schema, [row_one, row_two], vec![]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::int1([1, 2]));
		}

		#[test]
		fn test_all_defined_int2() {
			let mut test_instance = Columns::new(vec![Column::int2("test_col", Vec::<i16>::new())]);

			let schema = Schema::testing(&[Type::Int2]);
			let mut row_one = schema.allocate();
			schema.set_values(&mut row_one, &[Value::Int2(100)]);
			let mut row_two = schema.allocate();
			schema.set_values(&mut row_two, &[Value::Int2(200)]);

			test_instance.append_rows(&schema, [row_one, row_two], vec![]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::int2([100, 200]));
		}

		#[test]
		fn test_all_defined_int4() {
			let mut test_instance = Columns::new(vec![Column::int4("test_col", Vec::<i32>::new())]);

			let schema = Schema::testing(&[Type::Int4]);
			let mut row_one = schema.allocate();
			schema.set_values(&mut row_one, &[Value::Int4(1000)]);
			let mut row_two = schema.allocate();
			schema.set_values(&mut row_two, &[Value::Int4(2000)]);

			test_instance.append_rows(&schema, [row_one, row_two], vec![]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::int4([1000, 2000]));
		}

		#[test]
		fn test_all_defined_int8() {
			let mut test_instance = Columns::new(vec![Column::int8("test_col", Vec::<i64>::new())]);

			let schema = Schema::testing(&[Type::Int8]);
			let mut row_one = schema.allocate();
			schema.set_values(&mut row_one, &[Value::Int8(10000)]);
			let mut row_two = schema.allocate();
			schema.set_values(&mut row_two, &[Value::Int8(20000)]);

			test_instance.append_rows(&schema, [row_one, row_two], vec![]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::int8([10000, 20000]));
		}

		#[test]
		fn test_all_defined_int16() {
			let mut test_instance = Columns::new(vec![Column::int16("test_col", Vec::<i128>::new())]);

			let schema = Schema::testing(&[Type::Int16]);
			let mut row_one = schema.allocate();
			schema.set_values(&mut row_one, &[Value::Int16(1000)]);
			let mut row_two = schema.allocate();
			schema.set_values(&mut row_two, &[Value::Int16(2000)]);

			test_instance.append_rows(&schema, [row_one, row_two], vec![]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::int16([1000, 2000]));
		}

		#[test]
		fn test_all_defined_string() {
			let mut test_instance = Columns::new(vec![Column::utf8("test_col", Vec::<String>::new())]);

			let schema = Schema::testing(&[Type::Utf8]);
			let mut row_one = schema.allocate();
			schema.set_values(&mut row_one, &[Value::Utf8("a".into())]);
			let mut row_two = schema.allocate();
			schema.set_values(&mut row_two, &[Value::Utf8("b".into())]);

			test_instance.append_rows(&schema, [row_one, row_two], vec![]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::utf8(["a".to_string(), "b".to_string()]));
		}

		#[test]
		fn test_all_defined_uint1() {
			let mut test_instance = Columns::new(vec![Column::uint1("test_col", Vec::<u8>::new())]);

			let schema = Schema::testing(&[Type::Uint1]);
			let mut row_one = schema.allocate();
			schema.set_values(&mut row_one, &[Value::Uint1(1)]);
			let mut row_two = schema.allocate();
			schema.set_values(&mut row_two, &[Value::Uint1(2)]);

			test_instance.append_rows(&schema, [row_one, row_two], vec![]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::uint1([1, 2]));
		}

		#[test]
		fn test_all_defined_uint2() {
			let mut test_instance = Columns::new(vec![Column::uint2("test_col", Vec::<u16>::new())]);

			let schema = Schema::testing(&[Type::Uint2]);
			let mut row_one = schema.allocate();
			schema.set_values(&mut row_one, &[Value::Uint2(100)]);
			let mut row_two = schema.allocate();
			schema.set_values(&mut row_two, &[Value::Uint2(200)]);

			test_instance.append_rows(&schema, [row_one, row_two], vec![]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::uint2([100, 200]));
		}

		#[test]
		fn test_all_defined_uint4() {
			let mut test_instance = Columns::new(vec![Column::uint4("test_col", Vec::<u32>::new())]);

			let schema = Schema::testing(&[Type::Uint4]);
			let mut row_one = schema.allocate();
			schema.set_values(&mut row_one, &[Value::Uint4(1000)]);
			let mut row_two = schema.allocate();
			schema.set_values(&mut row_two, &[Value::Uint4(2000)]);

			test_instance.append_rows(&schema, [row_one, row_two], vec![]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::uint4([1000, 2000]));
		}

		#[test]
		fn test_all_defined_uint8() {
			let mut test_instance = Columns::new(vec![Column::uint8("test_col", Vec::<u64>::new())]);

			let schema = Schema::testing(&[Type::Uint8]);
			let mut row_one = schema.allocate();
			schema.set_values(&mut row_one, &[Value::Uint8(10000)]);
			let mut row_two = schema.allocate();
			schema.set_values(&mut row_two, &[Value::Uint8(20000)]);

			test_instance.append_rows(&schema, [row_one, row_two], vec![]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::uint8([10000, 20000]));
		}

		#[test]
		fn test_all_defined_uint16() {
			let mut test_instance = Columns::new(vec![Column::uint16("test_col", Vec::<u128>::new())]);

			let schema = Schema::testing(&[Type::Uint16]);
			let mut row_one = schema.allocate();
			schema.set_values(&mut row_one, &[Value::Uint16(1000)]);
			let mut row_two = schema.allocate();
			schema.set_values(&mut row_two, &[Value::Uint16(2000)]);

			test_instance.append_rows(&schema, [row_one, row_two], vec![]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::uint16([1000, 2000]));
		}

		#[test]
		fn test_row_with_undefined() {
			let mut test_instance = test_instance_with_columns();

			let schema = Schema::testing(&[Type::Int2, Type::Boolean]);
			let mut row = schema.allocate();
			schema.set_values(&mut row, &[Value::none(), Value::Boolean(false)]);

			test_instance.append_rows(&schema, [row], vec![]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::int2_with_bitvec(vec![1, 0], vec![true, false])
			);
			assert_eq!(*test_instance[1].data(), ColumnData::bool_with_bitvec([true, false], [true, true]));
		}

		#[test]
		fn test_row_with_type_mismatch_fails() {
			let mut test_instance = test_instance_with_columns();

			let schema = Schema::testing(&[Type::Boolean, Type::Boolean]);
			let mut row = schema.allocate();
			schema.set_values(&mut row, &[Value::Boolean(true), Value::Boolean(true)]);

			let result = test_instance.append_rows(&schema, [row], vec![]);
			assert!(result.is_err());
			assert!(result.unwrap_err().to_string().contains("type mismatch"));
		}

		#[test]
		fn test_row_wrong_length_fails() {
			let mut test_instance = test_instance_with_columns();

			let schema = Schema::testing(&[Type::Int2]);
			let mut row = schema.allocate();
			schema.set_values(&mut row, &[Value::Int2(2)]);

			let result = test_instance.append_rows(&schema, [row], vec![]);
			assert!(result.is_err());
			assert!(result.unwrap_err().to_string().contains("mismatched column count"));
		}

		#[test]
		fn test_fallback_bool() {
			let mut test_instance = Columns::new(vec![
				Column::bool("test_col", Vec::<bool>::new()),
				Column::bool("none", Vec::<bool>::new()),
			]);

			let schema = Schema::testing(&[Type::Boolean, Type::Boolean]);
			let mut row_one = schema.allocate();
			schema.set_bool(&mut row_one, 0, true);
			schema.set_undefined(&mut row_one, 1);

			test_instance.append_rows(&schema, [row_one], vec![]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::bool_with_bitvec([true], [true]));

			assert_eq!(*test_instance[1].data(), ColumnData::bool_with_bitvec([false], [false]));
		}

		#[test]
		fn test_fallback_float4() {
			let mut test_instance = Columns::new(vec![
				Column::float4("test_col", Vec::<f32>::new()),
				Column::float4("none", Vec::<f32>::new()),
			]);

			let schema = Schema::testing(&[Type::Float4, Type::Float4]);
			let mut row = schema.allocate();
			schema.set_f32(&mut row, 0, 1.5);
			schema.set_undefined(&mut row, 1);

			test_instance.append_rows(&schema, [row], vec![]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::float4_with_bitvec([1.5], [true]));
			assert_eq!(*test_instance[1].data(), ColumnData::float4_with_bitvec([0.0], [false]));
		}

		#[test]
		fn test_fallback_float8() {
			let mut test_instance = Columns::new(vec![
				Column::float8("test_col", Vec::<f64>::new()),
				Column::float8("none", Vec::<f64>::new()),
			]);

			let schema = Schema::testing(&[Type::Float8, Type::Float8]);
			let mut row = schema.allocate();
			schema.set_f64(&mut row, 0, 2.5);
			schema.set_undefined(&mut row, 1);

			test_instance.append_rows(&schema, [row], vec![]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::float8_with_bitvec([2.5], [true]));
			assert_eq!(*test_instance[1].data(), ColumnData::float8_with_bitvec([0.0], [false]));
		}

		#[test]
		fn test_fallback_int1() {
			let mut test_instance = Columns::new(vec![
				Column::int1("test_col", Vec::<i8>::new()),
				Column::int1("none", Vec::<i8>::new()),
			]);

			let schema = Schema::testing(&[Type::Int1, Type::Int1]);
			let mut row = schema.allocate();
			schema.set_i8(&mut row, 0, 42);
			schema.set_undefined(&mut row, 1);

			test_instance.append_rows(&schema, [row], vec![]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::int1_with_bitvec([42], [true]));
			assert_eq!(*test_instance[1].data(), ColumnData::int1_with_bitvec([0], [false]));
		}

		#[test]
		fn test_fallback_int2() {
			let mut test_instance = Columns::new(vec![
				Column::int2("test_col", Vec::<i16>::new()),
				Column::int2("none", Vec::<i16>::new()),
			]);

			let schema = Schema::testing(&[Type::Int2, Type::Int2]);
			let mut row = schema.allocate();
			schema.set_i16(&mut row, 0, -1234i16);
			schema.set_undefined(&mut row, 1);

			test_instance.append_rows(&schema, [row], vec![]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::int2_with_bitvec([-1234], [true]));
			assert_eq!(*test_instance[1].data(), ColumnData::int2_with_bitvec([0], [false]));
		}

		#[test]
		fn test_fallback_int4() {
			let mut test_instance = Columns::new(vec![
				Column::int4("test_col", Vec::<i32>::new()),
				Column::int4("none", Vec::<i32>::new()),
			]);

			let schema = Schema::testing(&[Type::Int4, Type::Int4]);
			let mut row = schema.allocate();
			schema.set_i32(&mut row, 0, 56789);
			schema.set_undefined(&mut row, 1);

			test_instance.append_rows(&schema, [row], vec![]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::int4_with_bitvec([56789], [true]));
			assert_eq!(*test_instance[1].data(), ColumnData::int4_with_bitvec([0], [false]));
		}

		#[test]
		fn test_fallback_int8() {
			let mut test_instance = Columns::new(vec![
				Column::int8("test_col", Vec::<i64>::new()),
				Column::int8("none", Vec::<i64>::new()),
			]);

			let schema = Schema::testing(&[Type::Int8, Type::Int8]);
			let mut row = schema.allocate();
			schema.set_i64(&mut row, 0, -987654321);
			schema.set_undefined(&mut row, 1);

			test_instance.append_rows(&schema, [row], vec![]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::int8_with_bitvec([-987654321], [true]));
			assert_eq!(*test_instance[1].data(), ColumnData::int8_with_bitvec([0], [false]));
		}

		#[test]
		fn test_fallback_int16() {
			let mut test_instance = Columns::new(vec![
				Column::int16("test_col", Vec::<i128>::new()),
				Column::int16("none", Vec::<i128>::new()),
			]);

			let schema = Schema::testing(&[Type::Int16, Type::Int16]);
			let mut row = schema.allocate();
			schema.set_i128(&mut row, 0, 123456789012345678901234567890i128);
			schema.set_undefined(&mut row, 1);

			test_instance.append_rows(&schema, [row], vec![]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::int16_with_bitvec([123456789012345678901234567890i128], [true])
			);
			assert_eq!(*test_instance[1].data(), ColumnData::int16_with_bitvec([0], [false]));
		}

		#[test]
		fn test_fallback_string() {
			let mut test_instance = Columns::new(vec![
				Column::utf8("test_col", Vec::<String>::new()),
				Column::utf8("none", Vec::<String>::new()),
			]);

			let schema = Schema::testing(&[Type::Utf8, Type::Utf8]);
			let mut row = schema.allocate();
			schema.set_utf8(&mut row, 0, "reifydb");
			schema.set_undefined(&mut row, 1);

			test_instance.append_rows(&schema, [row], vec![]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::utf8_with_bitvec(["reifydb".to_string()], [true])
			);
			assert_eq!(*test_instance[1].data(), ColumnData::utf8_with_bitvec(["".to_string()], [false]));
		}

		#[test]
		fn test_fallback_uint1() {
			let mut test_instance = Columns::new(vec![
				Column::uint1("test_col", Vec::<u8>::new()),
				Column::uint1("none", Vec::<u8>::new()),
			]);

			let schema = Schema::testing(&[Type::Uint1, Type::Uint1]);
			let mut row = schema.allocate();
			schema.set_u8(&mut row, 0, 255);
			schema.set_undefined(&mut row, 1);

			test_instance.append_rows(&schema, [row], vec![]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::uint1_with_bitvec([255], [true]));
			assert_eq!(*test_instance[1].data(), ColumnData::uint1_with_bitvec([0], [false]));
		}

		#[test]
		fn test_fallback_uint2() {
			let mut test_instance = Columns::new(vec![
				Column::uint2("test_col", Vec::<u16>::new()),
				Column::uint2("none", Vec::<u16>::new()),
			]);

			let schema = Schema::testing(&[Type::Uint2, Type::Uint2]);
			let mut row = schema.allocate();
			schema.set_u16(&mut row, 0, 65535u16);
			schema.set_undefined(&mut row, 1);

			test_instance.append_rows(&schema, [row], vec![]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::uint2_with_bitvec([65535], [true]));
			assert_eq!(*test_instance[1].data(), ColumnData::uint2_with_bitvec([0], [false]));
		}

		#[test]
		fn test_fallback_uint4() {
			let mut test_instance = Columns::new(vec![
				Column::uint4("test_col", Vec::<u32>::new()),
				Column::uint4("none", Vec::<u32>::new()),
			]);

			let schema = Schema::testing(&[Type::Uint4, Type::Uint4]);
			let mut row = schema.allocate();
			schema.set_u32(&mut row, 0, 4294967295u32);
			schema.set_undefined(&mut row, 1);

			test_instance.append_rows(&schema, [row], vec![]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::uint4_with_bitvec([4294967295], [true]));
			assert_eq!(*test_instance[1].data(), ColumnData::uint4_with_bitvec([0], [false]));
		}

		#[test]
		fn test_fallback_uint8() {
			let mut test_instance = Columns::new(vec![
				Column::uint8("test_col", Vec::<u64>::new()),
				Column::uint8("none", Vec::<u64>::new()),
			]);

			let schema = Schema::testing(&[Type::Uint8, Type::Uint8]);
			let mut row = schema.allocate();
			schema.set_u64(&mut row, 0, 18446744073709551615u64);
			schema.set_undefined(&mut row, 1);

			test_instance.append_rows(&schema, [row], vec![]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::uint8_with_bitvec([18446744073709551615], [true])
			);
			assert_eq!(*test_instance[1].data(), ColumnData::uint8_with_bitvec([0], [false]));
		}

		#[test]
		fn test_fallback_uint16() {
			let mut test_instance = Columns::new(vec![
				Column::uint16("test_col", Vec::<u128>::new()),
				Column::uint16("none", Vec::<u128>::new()),
			]);

			let schema = Schema::testing(&[Type::Uint16, Type::Uint16]);
			let mut row = schema.allocate();
			schema.set_u128(&mut row, 0, 340282366920938463463374607431768211455u128);
			schema.set_undefined(&mut row, 1);

			test_instance.append_rows(&schema, [row], vec![]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::uint16_with_bitvec([340282366920938463463374607431768211455u128], [true])
			);
			assert_eq!(*test_instance[1].data(), ColumnData::uint16_with_bitvec([0], [false]));
		}

		#[test]
		fn test_all_defined_dictionary_id() {
			use reifydb_type::value::{
				constraint::TypeConstraint,
				dictionary::{DictionaryEntryId, DictionaryId},
			};

			use crate::encoded::schema::SchemaField;

			let constraint = TypeConstraint::dictionary(DictionaryId::from(1u64), Type::Uint4);
			let schema = Schema::new(vec![SchemaField::new("status", constraint)]);

			let mut test_instance =
				Columns::new(vec![Column::dictionary_id("status", Vec::<DictionaryEntryId>::new())]);

			let mut row_one = schema.allocate();
			schema.set_values(&mut row_one, &[Value::DictionaryId(DictionaryEntryId::U4(10))]);
			let mut row_two = schema.allocate();
			schema.set_values(&mut row_two, &[Value::DictionaryId(DictionaryEntryId::U4(20))]);

			test_instance.append_rows(&schema, [row_one, row_two], vec![]).unwrap();

			assert_eq!(
				test_instance[0].data().get_value(0),
				Value::DictionaryId(DictionaryEntryId::U4(10))
			);
			assert_eq!(
				test_instance[0].data().get_value(1),
				Value::DictionaryId(DictionaryEntryId::U4(20))
			);
		}

		#[test]
		fn test_fallback_dictionary_id() {
			use reifydb_type::value::{
				constraint::TypeConstraint,
				dictionary::{DictionaryEntryId, DictionaryId},
			};

			use crate::encoded::schema::SchemaField;

			let dict_constraint = TypeConstraint::dictionary(DictionaryId::from(1u64), Type::Uint4);
			let schema = Schema::new(vec![
				SchemaField::new("dict_col", dict_constraint),
				SchemaField::unconstrained("bool_col", Type::Boolean),
			]);

			let mut test_instance = Columns::new(vec![
				Column::dictionary_id("dict_col", Vec::<DictionaryEntryId>::new()),
				Column::bool("bool_col", Vec::<bool>::new()),
			]);

			let mut row = schema.allocate();
			schema.set_values(&mut row, &[Value::none(), Value::Boolean(true)]);

			test_instance.append_rows(&schema, [row], vec![]).unwrap();

			// Dictionary column should be undefined
			assert!(!test_instance[0].data().is_defined(0));
			// Bool column should be defined
			assert_eq!(test_instance[1].data().get_value(0), Value::Boolean(true));
		}

		#[test]
		fn test_before_undefined_dictionary_id() {
			use reifydb_type::value::{
				constraint::TypeConstraint,
				dictionary::{DictionaryEntryId, DictionaryId},
			};

			use crate::encoded::schema::SchemaField;

			let constraint = TypeConstraint::dictionary(DictionaryId::from(2u64), Type::Uint4);
			let schema = Schema::new(vec![SchemaField::new("tag", constraint)]);

			let mut test_instance = Columns::new(vec![Column::undefined_typed("tag", Type::Boolean, 2)]);

			let mut row = schema.allocate();
			schema.set_values(&mut row, &[Value::DictionaryId(DictionaryEntryId::U4(5))]);

			test_instance.append_rows(&schema, [row], vec![]).unwrap();

			// First two are undefined (promoted from Undefined column), third is defined
			assert!(!test_instance[0].data().is_defined(0));
			assert!(!test_instance[0].data().is_defined(1));
			assert!(test_instance[0].data().is_defined(2));
			assert_eq!(test_instance[0].data().get_value(2), Value::DictionaryId(DictionaryEntryId::U4(5)));
		}

		fn test_instance_with_columns() -> Columns {
			Columns::new(vec![
				Column {
					name: Fragment::internal("int2"),
					data: ColumnData::int2(vec![1]),
				},
				Column {
					name: Fragment::internal("bool"),
					data: ColumnData::bool(vec![true]),
				},
			])
		}
	}
}
