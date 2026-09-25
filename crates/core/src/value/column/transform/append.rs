// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem;

use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder};
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
		blob::Blob,
		constraint::Constraint,
		container::{
			decimal_array::uint16_to_native,
			dictionary_array::push_entry,
			digest_array::push_digest,
			temporal_array::{date_to_native, datetime_to_native, duration_to_native, time_to_native},
		},
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		dictionary::DictionaryEntryId,
		duration::Duration,
		identity::IdentityId,
		int::Int,
		row_number::RowNumber,
		system_columns::RowStamps,
		time::Time,
		uint::Uint,
		uuid::{Uuid4, Uuid7},
		value_type::ValueType,
	},
};
use uuid::Uuid;

use crate::{
	error::CoreError,
	value::column::{ColumnBuffer, builder::ColumnBuilder, columns::Columns},
};

impl Columns {
	pub fn append_columns(&mut self, other: Columns) -> Result<()> {
		if self.len() != other.len() {
			return Err(CoreError::FrameError {
				message: "mismatched column count".to_string(),
			}
			.into());
		}

		self.system.extend(&other.system)?;

		for i in 0..self.columns.len() {
			let self_name = self.names[i].text().to_string();
			let other_name = other.names[i].text().to_string();
			if self_name != other_name {
				return Err(CoreError::FrameError {
					message: format!(
						"column name mismatch at index {}: '{}' vs '{}'",
						i, self_name, other_name,
					),
				}
				.into());
			}
			let other_data = other.columns[i].clone();
			self.columns[i].extend(other_data)?;
		}
		Ok(())
	}
}

impl Columns {
	pub fn append_rows(
		&mut self,
		shape: &RowShape,
		bytes_vec: impl IntoIterator<Item = impl Into<EncodedBytes>>,
		row_numbers: Vec<RowNumber>,
	) -> Result<()> {
		self.validate_append_shape(shape)?;

		let bytes_vec: Vec<EncodedBytes> = bytes_vec.into_iter().map(Into::into).collect();
		Self::validate_row_numbers(&row_numbers, bytes_vec.len())?;

		reifydb_assertions! {
			let columns = self.len();
			let fields = shape.field_count();
			assert!(
				columns == fields,
				"append_rows retypes and dispatches per column by indexing shape.get_field(index); a \
				 column/field count divergence makes get_field(index).unwrap() panic on a valid call or \
				 route a row value into the wrong column (columns={columns}, shape fields={fields})"
			);
		}

		self.push_system_columns(shape, &bytes_vec, &row_numbers);
		self.retype_all_none_columns(shape);
		self.append_each_bytes(shape, &bytes_vec)
	}

	#[inline]
	fn validate_append_shape(&self, shape: &RowShape) -> Result<()> {
		if self.len() != shape.field_count() {
			return Err(CoreError::FrameError {
				message: format!(
					"mismatched column count: expected {}, got {}",
					self.len(),
					shape.field_count()
				),
			}
			.into());
		}
		Ok(())
	}

	#[inline]
	fn validate_row_numbers(row_numbers: &[RowNumber], rows_len: usize) -> Result<()> {
		if !row_numbers.is_empty() && row_numbers.len() != rows_len {
			return Err(CoreError::FrameError {
				message: format!(
					"row_numbers length {} does not match rows length {}",
					row_numbers.len(),
					rows_len
				),
			}
			.into());
		}
		Ok(())
	}

	#[inline]
	fn push_system_columns(&mut self, shape: &RowShape, bytes_slice: &[EncodedBytes], row_numbers: &[RowNumber]) {
		for (index, row) in bytes_slice.iter().enumerate() {
			let (created_at, updated_at) = match shape.family() {
				RowFamily::Pod => (None, None),
				_ => (Some(shape.created_at(row)), Some(shape.updated_at(row))),
			};

			self.system.push(RowStamps {
				row_number: row_numbers.get(index).copied(),
				partition: None,
				created_at,
				updated_at,
				time: shape.time(row),
				commit_version: None,
			});
		}
	}

	#[inline]
	fn retype_all_none_columns(&mut self, shape: &RowShape) {
		let columns = &mut self.columns;
		for (index, column) in columns.iter_mut().enumerate() {
			let field = shape.get_field(index).unwrap();
			let is_all_none = column.nulls().is_some_and(|nulls| nulls.null_count() == nulls.len());
			if is_all_none {
				let size = column.len();
				let new_data = match field.constraint.get_type() {
					ValueType::Boolean => ColumnBuffer::bool_with_bitvec(
						vec![false; size],
						BooleanBuffer::new_unset(size),
					),
					ValueType::Float4 => ColumnBuffer::float4_with_bitvec(
						vec![0.0f32; size],
						BooleanBuffer::new_unset(size),
					),
					ValueType::Float8 => ColumnBuffer::float8_with_bitvec(
						vec![0.0f64; size],
						BooleanBuffer::new_unset(size),
					),
					ValueType::Int1 => ColumnBuffer::int1_with_bitvec(
						vec![0i8; size],
						BooleanBuffer::new_unset(size),
					),
					ValueType::Int2 => ColumnBuffer::int2_with_bitvec(
						vec![0i16; size],
						BooleanBuffer::new_unset(size),
					),
					ValueType::Int4 => ColumnBuffer::int4_with_bitvec(
						vec![0i32; size],
						BooleanBuffer::new_unset(size),
					),
					ValueType::Int8 => ColumnBuffer::int8_with_bitvec(
						vec![0i64; size],
						BooleanBuffer::new_unset(size),
					),
					ValueType::Int16 => ColumnBuffer::int16_with_bitvec(
						vec![0i128; size],
						BooleanBuffer::new_unset(size),
					),
					ValueType::Utf8 => ColumnBuffer::utf8_with_bitvec(
						vec![String::new(); size],
						BooleanBuffer::new_unset(size),
					),
					ValueType::Uint1 => ColumnBuffer::uint1_with_bitvec(
						vec![0u8; size],
						BooleanBuffer::new_unset(size),
					),
					ValueType::Uint2 => ColumnBuffer::uint2_with_bitvec(
						vec![0u16; size],
						BooleanBuffer::new_unset(size),
					),
					ValueType::Uint4 => ColumnBuffer::uint4_with_bitvec(
						vec![0u32; size],
						BooleanBuffer::new_unset(size),
					),
					ValueType::Uint8 => ColumnBuffer::uint8_with_bitvec(
						vec![0u64; size],
						BooleanBuffer::new_unset(size),
					),
					ValueType::Uint16 => ColumnBuffer::uint16_with_bitvec(
						vec![0u128; size],
						BooleanBuffer::new_unset(size),
					),
					ValueType::Date => ColumnBuffer::date_with_bitvec(
						vec![Date::default(); size],
						BooleanBuffer::new_unset(size),
					),
					ValueType::DateTime => ColumnBuffer::datetime_with_bitvec(
						vec![DateTime::default(); size],
						BooleanBuffer::new_unset(size),
					),
					ValueType::Time => ColumnBuffer::time_with_bitvec(
						vec![Time::default(); size],
						BooleanBuffer::new_unset(size),
					),
					ValueType::Duration => ColumnBuffer::duration_with_bitvec(
						vec![Duration::default(); size],
						BooleanBuffer::new_unset(size),
					),
					ValueType::Option(_) => column.clone(),
					ValueType::IdentityId => ColumnBuffer::identity_id_with_bitvec(
						vec![Default::default(); size],
						BooleanBuffer::new_unset(size),
					),
					ValueType::Uuid4 => ColumnBuffer::uuid4_with_bitvec(
						vec![Uuid4::from(Uuid::nil()); size],
						BooleanBuffer::new_unset(size),
					),
					ValueType::Uuid7 => ColumnBuffer::uuid7_with_bitvec(
						vec![Uuid7::from(Uuid::nil()); size],
						BooleanBuffer::new_unset(size),
					),
					ValueType::Blob => ColumnBuffer::blob_with_bitvec(
						vec![Blob::new(vec![]); size],
						BooleanBuffer::new_unset(size),
					),
					ValueType::Int {
						precision,
					} => ColumnBuffer::int_with_bitvec(
						precision,
						vec![Int::default(); size],
						BooleanBuffer::new_unset(size),
					),
					ValueType::Uint {
						precision,
					} => ColumnBuffer::uint_with_bitvec(
						precision,
						vec![Uint::default(); size],
						BooleanBuffer::new_unset(size),
					),
					ValueType::Decimal {
						precision,
						scale,
					} => ColumnBuffer::decimal_with_bitvec(
						precision,
						scale,
						vec![Decimal::default(); size],
						BooleanBuffer::new_unset(size),
					),
					ValueType::DictionaryId => {
						let mut col_data = ColumnBuffer::dictionary_id_with_bitvec(
							vec![Default::default(); size],
							BooleanBuffer::new_unset(size),
						);
						if let ColumnBuffer::DictionaryId {
							dictionary_id,
							..
						} = &mut col_data
							&& let Some(Constraint::Dictionary(dict_id, _)) =
								field.constraint.constraint()
						{
							*dictionary_id = Some(*dict_id);
						}
						col_data
					}
					ValueType::Any | ValueType::Tuple(_) => ColumnBuffer::any_with_bitvec(
						vec![Value::none(); size],
						BooleanBuffer::new_unset(size),
					),
					declared @ (ValueType::List(_) | ValueType::Record(_)) => {
						ColumnBuffer::any_with_bitvec_typed(
							vec![Value::none(); size],
							BooleanBuffer::new_unset(size),
							declared,
						)
					}
					digest @ ValueType::Digest {
						..
					} => ColumnBuffer::none_typed(digest, size),
				};

				*column = new_data;
			}

			if let ColumnBuffer::DictionaryId {
				dictionary_id,
				..
			} = &mut *column
				&& dictionary_id.is_none()
				&& let Some(Constraint::Dictionary(dict_id, _)) = field.constraint.constraint()
			{
				*dictionary_id = Some(*dict_id);
			}
		}
	}

	#[inline]
	fn append_each_bytes(&mut self, shape: &RowShape, bytes_slice: &[EncodedBytes]) -> Result<()> {
		let mut builders: Vec<ColumnBuilder> = self
			.columns
			.iter_mut()
			.map(|column| mem::replace(column, ColumnBuffer::bool(vec![])).into_builder())
			.collect();
		let appended = Self::append_rows_into(&self.names, &mut builders, shape, bytes_slice);
		for (column, builder) in self.columns.iter_mut().zip(builders) {
			*column = builder.finish();
		}
		appended
	}

	fn append_rows_into(
		names: &[Fragment],
		builders: &mut [ColumnBuilder],
		shape: &RowShape,
		bytes_slice: &[EncodedBytes],
	) -> Result<()> {
		for row in bytes_slice {
			let all_defined = (0..shape.field_count()).all(|i| shape.is_defined(row, i));

			if all_defined {
				Self::append_all_defined_from_shape(names, builders, shape, row)?;
			} else {
				Self::append_fallback_from_shape(builders, shape, row)?;
			}
		}

		Ok(())
	}

	fn append_all_defined_from_shape(
		names: &[Fragment],
		columns: &mut [ColumnBuilder],
		shape: &RowShape,
		bytes: &EncodedBytes,
	) -> Result<()> {
		for (index, column) in columns.iter_mut().enumerate() {
			let field = shape.get_field(index).unwrap();
			match (&mut *column, field.constraint.get_type()) {
				(
					ColumnBuilder::Option {
						inner,
						bitvec,
					},
					_ty,
				) => {
					let value = shape.get_value(bytes, index);
					if matches!(value, Value::None { .. }) {
						inner.push_none();
						bitvec.append(false);
					} else {
						inner.push_value(value);
						bitvec.append(true);
					}
				}
				(ColumnBuilder::Bool(builder), ValueType::Boolean) => {
					builder.append(shape.get::<bool>(bytes, index));
				}
				(ColumnBuilder::Float4(builder), ValueType::Float4) => {
					builder.append_value(shape.get::<f32>(bytes, index));
				}
				(ColumnBuilder::Float8(builder), ValueType::Float8) => {
					builder.append_value(shape.get::<f64>(bytes, index));
				}
				(ColumnBuilder::Int1(builder), ValueType::Int1) => {
					builder.append_value(shape.get::<i8>(bytes, index));
				}
				(ColumnBuilder::Int2(builder), ValueType::Int2) => {
					builder.append_value(shape.get::<i16>(bytes, index));
				}
				(ColumnBuilder::Int4(builder), ValueType::Int4) => {
					builder.append_value(shape.get::<i32>(bytes, index));
				}
				(ColumnBuilder::Int8(builder), ValueType::Int8) => {
					builder.append_value(shape.get::<i64>(bytes, index));
				}
				(ColumnBuilder::Int16(builder), ValueType::Int16) => {
					builder.append_value(shape.get::<i128>(bytes, index));
				}
				(
					ColumnBuilder::Utf8 {
						builder,
						..
					},
					ValueType::Utf8,
				) => {
					builder.append_value(shape.get_utf8(bytes, index));
				}
				(ColumnBuilder::Uint1(builder), ValueType::Uint1) => {
					builder.append_value(shape.get::<u8>(bytes, index));
				}
				(ColumnBuilder::Uint2(builder), ValueType::Uint2) => {
					builder.append_value(shape.get::<u16>(bytes, index));
				}
				(ColumnBuilder::Uint4(builder), ValueType::Uint4) => {
					builder.append_value(shape.get::<u32>(bytes, index));
				}
				(ColumnBuilder::Uint8(builder), ValueType::Uint8) => {
					builder.append_value(shape.get::<u64>(bytes, index));
				}
				(ColumnBuilder::Uint16(builder), ValueType::Uint16) => {
					builder.append_value(uint16_to_native(shape.get::<u128>(bytes, index)));
				}
				(ColumnBuilder::Date(builder), ValueType::Date) => {
					builder.append_value(date_to_native(shape.get::<Date>(bytes, index)));
				}
				(ColumnBuilder::DateTime(builder), ValueType::DateTime) => {
					builder.append_value(datetime_to_native(shape.get::<DateTime>(bytes, index)));
				}
				(ColumnBuilder::Time(builder), ValueType::Time) => {
					builder.append_value(time_to_native(shape.get::<Time>(bytes, index)));
				}
				(ColumnBuilder::Duration(builder), ValueType::Duration) => {
					builder.append_value(duration_to_native(shape.get::<Duration>(bytes, index)));
				}
				(ColumnBuilder::Uuid4(buffer), ValueType::Uuid4) => {
					buffer.extend_from_slice(shape.get::<Uuid4>(bytes, index).as_bytes());
				}
				(ColumnBuilder::Uuid7(buffer), ValueType::Uuid7) => {
					buffer.extend_from_slice(shape.get::<Uuid7>(bytes, index).as_bytes());
				}
				(ColumnBuilder::IdentityId(buffer), ValueType::IdentityId) => {
					buffer.extend_from_slice(shape.get::<IdentityId>(bytes, index).as_bytes());
				}
				(
					ColumnBuilder::Blob {
						builder,
						..
					},
					ValueType::Blob,
				) => {
					builder.append_value(shape.get_blob_slice(bytes, index));
				}
				(
					ColumnBuilder::Int(builder),
					ValueType::Int {
						..
					},
				) => {
					builder.push(&Decimal::from(shape.get_int(bytes, index)));
				}
				(
					ColumnBuilder::Uint(builder),
					ValueType::Uint {
						..
					},
				) => {
					builder.push(&Decimal::from(shape.get_uint(bytes, index)));
				}
				(
					ColumnBuilder::Decimal(builder),
					ValueType::Decimal {
						..
					},
				) => {
					builder.push(&shape.get_decimal(bytes, index));
				}
				(
					ColumnBuilder::DictionaryId {
						buffer,
						..
					},
					ValueType::DictionaryId,
				) => match shape.get_value(bytes, index) {
					Value::DictionaryId(id) => push_entry(buffer, id),
					_ => push_entry(buffer, DictionaryEntryId::default()),
				},
				(
					ColumnBuilder::Digest {
						builder,
						inner,
						accuracy,
					},
					ValueType::Digest {
						inner: field_inner,
						accuracy: field_accuracy,
					},
				) if *inner == *field_inner && *accuracy == field_accuracy => {
					push_digest(builder, &shape.get_digest(bytes, index));
				}
				(_, v) => {
					return Err(CoreError::FrameError {
						message: format!(
							"type mismatch for column '{}'({}): incompatible with value {}",
							names[index].text(),
							column.get_type(),
							v
						),
					}
					.into());
				}
			}
		}
		Ok(())
	}

	fn append_fallback_from_shape(
		columns: &mut [ColumnBuilder],
		shape: &RowShape,
		bytes: &EncodedBytes,
	) -> Result<()> {
		for (index, column) in columns.iter_mut().enumerate() {
			let field = shape.get_field(index).unwrap();

			if !shape.is_defined(bytes, index) {
				column.push_none();
				continue;
			}

			match (&mut *column, field.constraint.get_type()) {
				(
					ColumnBuilder::Option {
						inner,
						bitvec,
					},
					_ty,
				) => {
					let value = shape.get_value(bytes, index);
					inner.push_value(value);
					bitvec.append(true);
				}
				(ColumnBuilder::Bool(builder), ValueType::Boolean) => {
					builder.append(shape.get::<bool>(bytes, index));
				}
				(ColumnBuilder::Float4(builder), ValueType::Float4) => {
					builder.append_value(shape.get::<f32>(bytes, index));
				}
				(ColumnBuilder::Float8(builder), ValueType::Float8) => {
					builder.append_value(shape.get::<f64>(bytes, index));
				}
				(ColumnBuilder::Int1(builder), ValueType::Int1) => {
					builder.append_value(shape.get::<i8>(bytes, index));
				}
				(ColumnBuilder::Int2(builder), ValueType::Int2) => {
					builder.append_value(shape.get::<i16>(bytes, index));
				}
				(ColumnBuilder::Int4(builder), ValueType::Int4) => {
					builder.append_value(shape.get::<i32>(bytes, index));
				}
				(ColumnBuilder::Int8(builder), ValueType::Int8) => {
					builder.append_value(shape.get::<i64>(bytes, index));
				}
				(ColumnBuilder::Int16(builder), ValueType::Int16) => {
					builder.append_value(shape.get::<i128>(bytes, index));
				}
				(
					ColumnBuilder::Utf8 {
						builder,
						..
					},
					ValueType::Utf8,
				) => {
					builder.append_value(shape.get_utf8(bytes, index));
				}
				(ColumnBuilder::Uint1(builder), ValueType::Uint1) => {
					builder.append_value(shape.get::<u8>(bytes, index));
				}
				(ColumnBuilder::Uint2(builder), ValueType::Uint2) => {
					builder.append_value(shape.get::<u16>(bytes, index));
				}
				(ColumnBuilder::Uint4(builder), ValueType::Uint4) => {
					builder.append_value(shape.get::<u32>(bytes, index));
				}
				(ColumnBuilder::Uint8(builder), ValueType::Uint8) => {
					builder.append_value(shape.get::<u64>(bytes, index));
				}
				(ColumnBuilder::Uint16(builder), ValueType::Uint16) => {
					builder.append_value(uint16_to_native(shape.get::<u128>(bytes, index)));
				}
				(ColumnBuilder::Date(builder), ValueType::Date) => {
					builder.append_value(date_to_native(shape.get::<Date>(bytes, index)));
				}
				(ColumnBuilder::DateTime(builder), ValueType::DateTime) => {
					builder.append_value(datetime_to_native(shape.get::<DateTime>(bytes, index)));
				}
				(ColumnBuilder::Time(builder), ValueType::Time) => {
					builder.append_value(time_to_native(shape.get::<Time>(bytes, index)));
				}
				(ColumnBuilder::Duration(builder), ValueType::Duration) => {
					builder.append_value(duration_to_native(shape.get::<Duration>(bytes, index)));
				}
				(ColumnBuilder::Uuid4(buffer), ValueType::Uuid4) => {
					buffer.extend_from_slice(shape.get::<Uuid4>(bytes, index).as_bytes());
				}
				(ColumnBuilder::Uuid7(buffer), ValueType::Uuid7) => {
					buffer.extend_from_slice(shape.get::<Uuid7>(bytes, index).as_bytes());
				}
				(ColumnBuilder::IdentityId(buffer), ValueType::IdentityId) => {
					buffer.extend_from_slice(shape.get::<IdentityId>(bytes, index).as_bytes());
				}
				(
					ColumnBuilder::Blob {
						builder,
						..
					},
					ValueType::Blob,
				) => {
					builder.append_value(shape.get_blob_slice(bytes, index));
				}
				(
					ColumnBuilder::Int(builder),
					ValueType::Int {
						..
					},
				) => {
					builder.push(&Decimal::from(shape.get_int(bytes, index)));
				}
				(
					ColumnBuilder::Uint(builder),
					ValueType::Uint {
						..
					},
				) => {
					builder.push(&Decimal::from(shape.get_uint(bytes, index)));
				}
				(
					ColumnBuilder::Decimal(builder),
					ValueType::Decimal {
						..
					},
				) => {
					builder.push(&shape.get_decimal(bytes, index));
				}
				(
					ColumnBuilder::DictionaryId {
						buffer,
						..
					},
					ValueType::DictionaryId,
				) => match shape.get_value(bytes, index) {
					Value::DictionaryId(id) => push_entry(buffer, id),
					_ => push_entry(buffer, DictionaryEntryId::default()),
				},
				(
					ColumnBuilder::Digest {
						builder,
						inner,
						accuracy,
					},
					ValueType::Digest {
						inner: field_inner,
						accuracy: field_accuracy,
					},
				) if *inner == *field_inner && *accuracy == field_accuracy => {
					push_digest(builder, &shape.get_digest(bytes, index));
				}
				(l, r) => {
					let l = mem::replace(l, ColumnBuilder::Bool(BooleanBufferBuilder::new(0)))
						.finish();
					unreachable!("{:#?} {:#?}", l, r)
				}
			}
		}
		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	mod columns {
		use reifydb_value::value::{
			uuid::{Uuid4, Uuid7},
			value_type::ValueType,
		};
		use uuid::{Timestamp, Uuid};

		use crate::value::column::{ColumnBuffer, ColumnWithName, columns::Columns};

		#[test]
		fn test_boolean() {
			let mut test_instance1 = Columns::new(vec![ColumnWithName::new(
				"id",
				ColumnBuffer::bool_with_bitvec([true], vec![false]),
			)]);

			let test_instance2 = Columns::new(vec![ColumnWithName::new(
				"id",
				ColumnBuffer::bool_with_bitvec([false], vec![true]),
			)]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(test_instance1[0], ColumnBuffer::bool_with_bitvec([true, false], vec![false, true]));
		}

		#[test]
		fn test_float4() {
			let mut test_instance1 =
				Columns::new(vec![ColumnWithName::new("id", ColumnBuffer::float4([1.0f32, 2.0]))]);

			let test_instance2 = Columns::new(vec![ColumnWithName::new(
				"id",
				ColumnBuffer::float4_with_bitvec([3.0f32, 4.0], vec![true, false]),
			)]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnBuffer::float4_with_bitvec(
					[1.0f32, 2.0, 3.0, 4.0],
					vec![true, true, true, false]
				)
			);
		}

		#[test]
		fn test_float8() {
			let mut test_instance1 =
				Columns::new(vec![ColumnWithName::new("id", ColumnBuffer::float8([1.0f64, 2.0]))]);

			let test_instance2 = Columns::new(vec![ColumnWithName::new(
				"id",
				ColumnBuffer::float8_with_bitvec([3.0f64, 4.0], vec![true, false]),
			)]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnBuffer::float8_with_bitvec(
					[1.0f64, 2.0, 3.0, 4.0],
					vec![true, true, true, false]
				)
			);
		}

		#[test]
		fn test_int1() {
			let mut test_instance1 = Columns::new(vec![ColumnWithName::int1("id", [1, 2])]);

			let test_instance2 = Columns::new(vec![ColumnWithName::new(
				"id",
				ColumnBuffer::int1_with_bitvec([3, 4], vec![true, false]),
			)]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnBuffer::int1_with_bitvec([1, 2, 3, 4], vec![true, true, true, false])
			);
		}

		#[test]
		fn test_int2() {
			let mut test_instance1 =
				Columns::new(vec![ColumnWithName::new("id", ColumnBuffer::int2([1, 2]))]);

			let test_instance2 = Columns::new(vec![ColumnWithName::new(
				"id",
				ColumnBuffer::int2_with_bitvec([3, 4], vec![true, false]),
			)]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnBuffer::int2_with_bitvec([1, 2, 3, 4], vec![true, true, true, false])
			);
		}

		#[test]
		fn test_int4() {
			let mut test_instance1 = Columns::new(vec![ColumnWithName::int4("id", [1, 2])]);

			let test_instance2 = Columns::new(vec![ColumnWithName::new(
				"id",
				ColumnBuffer::int4_with_bitvec([3, 4], vec![true, false]),
			)]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnBuffer::int4_with_bitvec([1, 2, 3, 4], vec![true, true, true, false])
			);
		}

		#[test]
		fn test_int8() {
			let mut test_instance1 =
				Columns::new(vec![ColumnWithName::new("id", ColumnBuffer::int8([1, 2]))]);

			let test_instance2 = Columns::new(vec![ColumnWithName::new(
				"id",
				ColumnBuffer::int8_with_bitvec([3, 4], vec![true, false]),
			)]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnBuffer::int8_with_bitvec([1, 2, 3, 4], vec![true, true, true, false])
			);
		}

		#[test]
		fn test_int16() {
			let mut test_instance1 =
				Columns::new(vec![ColumnWithName::new("id", ColumnBuffer::int16([1, 2]))]);

			let test_instance2 = Columns::new(vec![ColumnWithName::new(
				"id",
				ColumnBuffer::int16_with_bitvec([3, 4], vec![true, false]),
			)]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnBuffer::int16_with_bitvec([1, 2, 3, 4], vec![true, true, true, false])
			);
		}

		#[test]
		fn test_string() {
			let mut test_instance1 = Columns::new(vec![ColumnWithName::new(
				"id",
				ColumnBuffer::utf8_with_bitvec(
					vec!["a".to_string(), "b".to_string()],
					vec![true, true],
				),
			)]);

			let test_instance2 = Columns::new(vec![ColumnWithName::new(
				"id",
				ColumnBuffer::utf8_with_bitvec(
					vec!["c".to_string(), "d".to_string()],
					vec![true, false],
				),
			)]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnBuffer::utf8_with_bitvec(
					vec!["a".to_string(), "b".to_string(), "c".to_string(), "d".to_string()],
					vec![true, true, true, false]
				)
			);
		}

		#[test]
		fn test_uint1() {
			let mut test_instance1 =
				Columns::new(vec![ColumnWithName::new("id", ColumnBuffer::uint1([1, 2]))]);

			let test_instance2 = Columns::new(vec![ColumnWithName::new(
				"id",
				ColumnBuffer::uint1_with_bitvec([3, 4], vec![true, false]),
			)]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnBuffer::uint1_with_bitvec([1, 2, 3, 4], vec![true, true, true, false])
			);
		}

		#[test]
		fn test_uint2() {
			let mut test_instance1 =
				Columns::new(vec![ColumnWithName::new("id", ColumnBuffer::uint2([1, 2]))]);

			let test_instance2 = Columns::new(vec![ColumnWithName::new(
				"id",
				ColumnBuffer::uint2_with_bitvec([3, 4], vec![true, false]),
			)]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnBuffer::uint2_with_bitvec([1, 2, 3, 4], vec![true, true, true, false])
			);
		}

		#[test]
		fn test_uint4() {
			let mut test_instance1 =
				Columns::new(vec![ColumnWithName::new("id", ColumnBuffer::uint4([1, 2]))]);

			let test_instance2 = Columns::new(vec![ColumnWithName::new(
				"id",
				ColumnBuffer::uint4_with_bitvec([3, 4], vec![true, false]),
			)]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnBuffer::uint4_with_bitvec([1, 2, 3, 4], vec![true, true, true, false])
			);
		}

		#[test]
		fn test_uint8() {
			let mut test_instance1 =
				Columns::new(vec![ColumnWithName::new("id", ColumnBuffer::uint8([1, 2]))]);

			let test_instance2 = Columns::new(vec![ColumnWithName::new(
				"id",
				ColumnBuffer::uint8_with_bitvec([3, 4], vec![true, false]),
			)]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnBuffer::uint8_with_bitvec([1, 2, 3, 4], vec![true, true, true, false])
			);
		}

		#[test]
		fn test_uint16() {
			let mut test_instance1 =
				Columns::new(vec![ColumnWithName::new("id", ColumnBuffer::uint16([1, 2]))]);

			let test_instance2 = Columns::new(vec![ColumnWithName::new(
				"id",
				ColumnBuffer::uint16_with_bitvec([3, 4], vec![true, false]),
			)]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnBuffer::uint16_with_bitvec([1, 2, 3, 4], vec![true, true, true, false])
			);
		}

		#[test]
		fn test_uuid4() {
			let uuid1 = Uuid4::from(Uuid::new_v4());
			let uuid2 = Uuid4::from(Uuid::new_v4());
			let uuid3 = Uuid4::from(Uuid::new_v4());
			let uuid4 = Uuid4::from(Uuid::new_v4());

			let mut test_instance1 =
				Columns::new(vec![ColumnWithName::new("id", ColumnBuffer::uuid4([uuid1, uuid2]))]);

			let test_instance2 = Columns::new(vec![ColumnWithName::new(
				"id",
				ColumnBuffer::uuid4_with_bitvec([uuid3, uuid4], vec![true, false]),
			)]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnBuffer::uuid4_with_bitvec(
					[uuid1, uuid2, uuid3, uuid4],
					vec![true, true, true, false]
				)
			);
		}

		#[test]
		fn test_uuid7() {
			let uuid1 = Uuid7::from(Uuid::new_v7(Timestamp::from_gregorian_time(1, 1)));
			let uuid2 = Uuid7::from(Uuid::new_v7(Timestamp::from_gregorian_time(1, 2)));
			let uuid3 = Uuid7::from(Uuid::new_v7(Timestamp::from_gregorian_time(2, 1)));
			let uuid4 = Uuid7::from(Uuid::new_v7(Timestamp::from_gregorian_time(2, 2)));

			let mut test_instance1 =
				Columns::new(vec![ColumnWithName::new("id", ColumnBuffer::uuid7([uuid1, uuid2]))]);

			let test_instance2 = Columns::new(vec![ColumnWithName::new(
				"id",
				ColumnBuffer::uuid7_with_bitvec([uuid3, uuid4], vec![true, false]),
			)]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnBuffer::uuid7_with_bitvec(
					[uuid1, uuid2, uuid3, uuid4],
					vec![true, true, true, false]
				)
			);
		}

		#[test]
		fn test_with_undefined_lr_promotes_correctly() {
			let mut test_instance1 = Columns::new(vec![ColumnWithName::new(
				"id",
				ColumnBuffer::int2_with_bitvec([1, 2], vec![true, false]),
			)]);

			let test_instance2 =
				Columns::new(vec![ColumnWithName::undefined_typed("id", ValueType::Boolean, 2)]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnBuffer::int2_with_bitvec([1, 2, 0, 0], vec![true, false, false, false])
			);
		}

		#[test]
		fn test_with_undefined_l_promotes_correctly() {
			let mut test_instance1 =
				Columns::new(vec![ColumnWithName::undefined_typed("score", ValueType::Boolean, 2)]);

			let test_instance2 = Columns::new(vec![ColumnWithName::new(
				"score",
				ColumnBuffer::int2_with_bitvec([10, 20], vec![true, false]),
			)]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnBuffer::int2_with_bitvec([0, 0, 10, 20], vec![false, false, true, false])
			);
		}

		#[test]
		fn test_fails_on_column_count_mismatch() {
			let mut test_instance1 = Columns::new(vec![ColumnWithName::new("id", ColumnBuffer::int2([1]))]);

			let test_instance2 = Columns::new(vec![
				ColumnWithName::new("id", ColumnBuffer::int2([2])),
				ColumnWithName::utf8("name", vec!["Bob".to_string()]),
			]);

			let result = test_instance1.append_columns(test_instance2);
			assert!(result.is_err());
		}

		#[test]
		fn test_fails_on_column_name_mismatch() {
			let mut test_instance1 = Columns::new(vec![ColumnWithName::new("id", ColumnBuffer::int2([1]))]);

			let test_instance2 = Columns::new(vec![ColumnWithName::new("wrong", ColumnBuffer::int2([2]))]);

			let result = test_instance1.append_columns(test_instance2);
			assert!(result.is_err());
		}

		#[test]
		fn test_fails_on_type_mismatch() {
			let mut test_instance1 = Columns::new(vec![ColumnWithName::new("id", ColumnBuffer::int2([1]))]);

			let test_instance2 = Columns::new(vec![ColumnWithName::utf8("id", vec!["A".to_string()])]);

			let result = test_instance1.append_columns(test_instance2);
			assert!(result.is_err());
		}
	}

	mod row {
		use arrow_buffer::BooleanBuffer;
		use reifydb_codec::row::shape::{RowFamily, RowShape, RowShapeField};
		use reifydb_value::{
			fragment::Fragment,
			value::{
				Value,
				blob::Blob,
				constraint::TypeConstraint,
				dictionary::{DictionaryEntryId, DictionaryId},
				identity::IdentityId,
				ordered_f32::OrderedF32,
				ordered_f64::OrderedF64,
				value_type::ValueType,
			},
		};

		use crate::value::column::{ColumnBuffer, ColumnWithName, columns::Columns};

		#[test]
		fn test_before_undefined_bool() {
			let mut test_instance =
				Columns::new(vec![ColumnWithName::undefined_typed("test_col", ValueType::Boolean, 2)]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Boolean]);
			let mut row = shape.allocate_table();
			shape.set_values(&mut row, &[Value::Boolean(true)]);

			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				test_instance[0],
				ColumnBuffer::bool_with_bitvec(
					[false, false, true],
					BooleanBuffer::from(vec![false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_float4() {
			let mut test_instance =
				Columns::new(vec![ColumnWithName::undefined_typed("test_col", ValueType::Boolean, 2)]);
			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Float4]);
			let mut row = shape.allocate_table();
			shape.set_values(&mut row, &[Value::Float4(OrderedF32::try_from(1.5).unwrap())]);
			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				test_instance[0],
				ColumnBuffer::float4_with_bitvec(
					[0.0, 0.0, 1.5],
					BooleanBuffer::from(vec![false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_float8() {
			let mut test_instance =
				Columns::new(vec![ColumnWithName::undefined_typed("test_col", ValueType::Boolean, 2)]);
			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Float8]);
			let mut row = shape.allocate_table();
			shape.set_values(&mut row, &[Value::Float8(OrderedF64::try_from(2.25).unwrap())]);
			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				test_instance[0],
				ColumnBuffer::float8_with_bitvec(
					[0.0, 0.0, 2.25],
					BooleanBuffer::from(vec![false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_int1() {
			let mut test_instance =
				Columns::new(vec![ColumnWithName::undefined_typed("test_col", ValueType::Boolean, 2)]);
			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Int1]);
			let mut row = shape.allocate_table();
			shape.set_values(&mut row, &[Value::Int1(42)]);
			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				test_instance[0],
				ColumnBuffer::int1_with_bitvec(
					[0, 0, 42],
					BooleanBuffer::from(vec![false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_int2() {
			let mut test_instance =
				Columns::new(vec![ColumnWithName::undefined_typed("test_col", ValueType::Boolean, 2)]);
			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Int2]);
			let mut row = shape.allocate_table();
			shape.set_values(&mut row, &[Value::Int2(-1234)]);
			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				test_instance[0],
				ColumnBuffer::int2_with_bitvec(
					[0, 0, -1234],
					BooleanBuffer::from(vec![false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_int4() {
			let mut test_instance =
				Columns::new(vec![ColumnWithName::undefined_typed("test_col", ValueType::Boolean, 2)]);
			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Int4]);
			let mut row = shape.allocate_table();
			shape.set_values(&mut row, &[Value::Int4(56789)]);
			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				test_instance[0],
				ColumnBuffer::int4_with_bitvec(
					[0, 0, 56789],
					BooleanBuffer::from(vec![false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_int8() {
			let mut test_instance =
				Columns::new(vec![ColumnWithName::undefined_typed("test_col", ValueType::Boolean, 2)]);
			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Int8]);
			let mut row = shape.allocate_table();
			shape.set_values(&mut row, &[Value::Int8(-987654321)]);
			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				test_instance[0],
				ColumnBuffer::int8_with_bitvec(
					[0, 0, -987654321],
					BooleanBuffer::from(vec![false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_int16() {
			let mut test_instance =
				Columns::new(vec![ColumnWithName::undefined_typed("test_col", ValueType::Boolean, 2)]);
			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Int16]);
			let mut row = shape.allocate_table();
			shape.set_values(&mut row, &[Value::Int16(123456789012345678901234567890i128)]);
			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				test_instance[0],
				ColumnBuffer::int16_with_bitvec(
					[0, 0, 123456789012345678901234567890i128],
					BooleanBuffer::from(vec![false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_string() {
			let mut test_instance =
				Columns::new(vec![ColumnWithName::undefined_typed("test_col", ValueType::Boolean, 2)]);
			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Utf8]);
			let mut row = shape.allocate_table();
			shape.set_values(&mut row, &[Value::Utf8("reifydb".into())]);
			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				test_instance[0],
				ColumnBuffer::utf8_with_bitvec(
					["".to_string(), "".to_string(), "reifydb".to_string()],
					BooleanBuffer::from(vec![false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_uint1() {
			let mut test_instance =
				Columns::new(vec![ColumnWithName::undefined_typed("test_col", ValueType::Boolean, 2)]);
			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Uint1]);
			let mut row = shape.allocate_table();
			shape.set_values(&mut row, &[Value::Uint1(255)]);
			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				test_instance[0],
				ColumnBuffer::uint1_with_bitvec(
					[0, 0, 255],
					BooleanBuffer::from(vec![false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_uint2() {
			let mut test_instance =
				Columns::new(vec![ColumnWithName::undefined_typed("test_col", ValueType::Boolean, 2)]);
			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Uint2]);
			let mut row = shape.allocate_table();
			shape.set_values(&mut row, &[Value::Uint2(65535)]);
			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				test_instance[0],
				ColumnBuffer::uint2_with_bitvec(
					[0, 0, 65535],
					BooleanBuffer::from(vec![false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_uint4() {
			let mut test_instance =
				Columns::new(vec![ColumnWithName::undefined_typed("test_col", ValueType::Boolean, 2)]);
			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Uint4]);
			let mut row = shape.allocate_table();
			shape.set_values(&mut row, &[Value::Uint4(4294967295)]);
			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				test_instance[0],
				ColumnBuffer::uint4_with_bitvec(
					[0, 0, 4294967295],
					BooleanBuffer::from(vec![false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_uint8() {
			let mut test_instance =
				Columns::new(vec![ColumnWithName::undefined_typed("test_col", ValueType::Boolean, 2)]);
			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Uint8]);
			let mut row = shape.allocate_table();
			shape.set_values(&mut row, &[Value::Uint8(18446744073709551615)]);
			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				test_instance[0],
				ColumnBuffer::uint8_with_bitvec(
					[0, 0, 18446744073709551615],
					BooleanBuffer::from(vec![false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_uint16() {
			let mut test_instance =
				Columns::new(vec![ColumnWithName::undefined_typed("test_col", ValueType::Boolean, 2)]);
			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Uint16]);
			let mut row = shape.allocate_table();
			shape.set_values(&mut row, &[Value::Uint16(340282366920938463463374607431768211455u128)]);
			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				test_instance[0],
				ColumnBuffer::uint16_with_bitvec(
					[0, 0, 340282366920938463463374607431768211455u128],
					BooleanBuffer::from(vec![false, false, true])
				)
			);
		}

		#[test]
		fn test_mismatched_columns() {
			let mut test_instance = Columns::new(vec![]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Int2]);
			let mut row = shape.allocate_table();
			shape.set_values(&mut row, &[Value::Int2(2)]);

			let err = test_instance.append_rows(&shape, [row.freeze()], vec![]).err().unwrap();
			assert!(err.to_string().contains("mismatched column count: expected 0, got 1"));
		}

		#[test]
		fn test_ok() {
			let mut test_instance = test_instance_with_columns();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Int2, ValueType::Boolean]);
			let mut row_one = shape.allocate_table();
			shape.set_values(&mut row_one, &[Value::Int2(2), Value::Boolean(true)]);
			let mut row_two = shape.allocate_table();
			shape.set_values(&mut row_two, &[Value::Int2(3), Value::Boolean(false)]);

			test_instance.append_rows(&shape, [row_one.freeze(), row_two.freeze()], vec![]).unwrap();

			assert_eq!(test_instance[0], ColumnBuffer::int2([1, 2, 3]));
			assert_eq!(test_instance[1], ColumnBuffer::bool([true, true, false]));
		}

		#[test]
		fn test_all_defined_bool() {
			let mut test_instance =
				Columns::new(vec![ColumnWithName::bool("test_col", Vec::<bool>::new())]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Boolean]);
			let mut row_one = shape.allocate_table();
			shape.set::<bool>(&mut row_one, 0, true);
			let mut row_two = shape.allocate_table();
			shape.set::<bool>(&mut row_two, 0, false);

			test_instance.append_rows(&shape, [row_one.freeze(), row_two.freeze()], vec![]).unwrap();

			assert_eq!(test_instance[0], ColumnBuffer::bool([true, false]));
		}

		#[test]
		fn test_all_defined_float4() {
			let mut test_instance = Columns::new(vec![ColumnWithName::new(
				"test_col",
				ColumnBuffer::float4(Vec::<f32>::new()),
			)]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Float4]);
			let mut row_one = shape.allocate_table();
			shape.set_values(&mut row_one, &[Value::Float4(OrderedF32::try_from(1.0).unwrap())]);
			let mut row_two = shape.allocate_table();
			shape.set_values(&mut row_two, &[Value::Float4(OrderedF32::try_from(2.0).unwrap())]);

			test_instance.append_rows(&shape, [row_one.freeze(), row_two.freeze()], vec![]).unwrap();

			assert_eq!(test_instance[0], ColumnBuffer::float4([1.0, 2.0]));
		}

		#[test]
		fn test_all_defined_float8() {
			let mut test_instance = Columns::new(vec![ColumnWithName::new(
				"test_col",
				ColumnBuffer::float8(Vec::<f64>::new()),
			)]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Float8]);
			let mut row_one = shape.allocate_table();
			shape.set_values(&mut row_one, &[Value::Float8(OrderedF64::try_from(1.0).unwrap())]);
			let mut row_two = shape.allocate_table();
			shape.set_values(&mut row_two, &[Value::Float8(OrderedF64::try_from(2.0).unwrap())]);

			test_instance.append_rows(&shape, [row_one.freeze(), row_two.freeze()], vec![]).unwrap();

			assert_eq!(test_instance[0], ColumnBuffer::float8([1.0, 2.0]));
		}

		#[test]
		fn test_all_defined_int1() {
			let mut test_instance = Columns::new(vec![ColumnWithName::int1("test_col", Vec::<i8>::new())]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Int1]);
			let mut row_one = shape.allocate_table();
			shape.set_values(&mut row_one, &[Value::Int1(1)]);
			let mut row_two = shape.allocate_table();
			shape.set_values(&mut row_two, &[Value::Int1(2)]);

			test_instance.append_rows(&shape, [row_one.freeze(), row_two.freeze()], vec![]).unwrap();

			assert_eq!(test_instance[0], ColumnBuffer::int1([1, 2]));
		}

		#[test]
		fn test_all_defined_int2() {
			let mut test_instance = Columns::new(vec![ColumnWithName::new(
				"test_col",
				ColumnBuffer::int2(Vec::<i16>::new()),
			)]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Int2]);
			let mut row_one = shape.allocate_table();
			shape.set_values(&mut row_one, &[Value::Int2(100)]);
			let mut row_two = shape.allocate_table();
			shape.set_values(&mut row_two, &[Value::Int2(200)]);

			test_instance.append_rows(&shape, [row_one.freeze(), row_two.freeze()], vec![]).unwrap();

			assert_eq!(test_instance[0], ColumnBuffer::int2([100, 200]));
		}

		#[test]
		fn test_all_defined_int4() {
			let mut test_instance = Columns::new(vec![ColumnWithName::int4("test_col", Vec::<i32>::new())]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Int4]);
			let mut row_one = shape.allocate_table();
			shape.set_values(&mut row_one, &[Value::Int4(1000)]);
			let mut row_two = shape.allocate_table();
			shape.set_values(&mut row_two, &[Value::Int4(2000)]);

			test_instance.append_rows(&shape, [row_one.freeze(), row_two.freeze()], vec![]).unwrap();

			assert_eq!(test_instance[0], ColumnBuffer::int4([1000, 2000]));
		}

		#[test]
		fn test_all_defined_int8() {
			let mut test_instance = Columns::new(vec![ColumnWithName::new(
				"test_col",
				ColumnBuffer::int8(Vec::<i64>::new()),
			)]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Int8]);
			let mut row_one = shape.allocate_table();
			shape.set_values(&mut row_one, &[Value::Int8(10000)]);
			let mut row_two = shape.allocate_table();
			shape.set_values(&mut row_two, &[Value::Int8(20000)]);

			test_instance.append_rows(&shape, [row_one.freeze(), row_two.freeze()], vec![]).unwrap();

			assert_eq!(test_instance[0], ColumnBuffer::int8([10000, 20000]));
		}

		#[test]
		fn test_all_defined_int16() {
			let mut test_instance = Columns::new(vec![ColumnWithName::new(
				"test_col",
				ColumnBuffer::int16(Vec::<i128>::new()),
			)]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Int16]);
			let mut row_one = shape.allocate_table();
			shape.set_values(&mut row_one, &[Value::Int16(1000)]);
			let mut row_two = shape.allocate_table();
			shape.set_values(&mut row_two, &[Value::Int16(2000)]);

			test_instance.append_rows(&shape, [row_one.freeze(), row_two.freeze()], vec![]).unwrap();

			assert_eq!(test_instance[0], ColumnBuffer::int16([1000, 2000]));
		}

		#[test]
		fn test_all_defined_string() {
			let mut test_instance =
				Columns::new(vec![ColumnWithName::utf8("test_col", Vec::<String>::new())]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Utf8]);
			let mut row_one = shape.allocate_table();
			shape.set_values(&mut row_one, &[Value::Utf8("a".into())]);
			let mut row_two = shape.allocate_table();
			shape.set_values(&mut row_two, &[Value::Utf8("b".into())]);

			test_instance.append_rows(&shape, [row_one.freeze(), row_two.freeze()], vec![]).unwrap();

			assert_eq!(test_instance[0], ColumnBuffer::utf8(["a".to_string(), "b".to_string()]));
		}

		#[test]
		fn test_all_defined_uint1() {
			let mut test_instance = Columns::new(vec![ColumnWithName::new(
				"test_col",
				ColumnBuffer::uint1(Vec::<u8>::new()),
			)]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Uint1]);
			let mut row_one = shape.allocate_table();
			shape.set_values(&mut row_one, &[Value::Uint1(1)]);
			let mut row_two = shape.allocate_table();
			shape.set_values(&mut row_two, &[Value::Uint1(2)]);

			test_instance.append_rows(&shape, [row_one.freeze(), row_two.freeze()], vec![]).unwrap();

			assert_eq!(test_instance[0], ColumnBuffer::uint1([1, 2]));
		}

		#[test]
		fn test_all_defined_uint2() {
			let mut test_instance = Columns::new(vec![ColumnWithName::new(
				"test_col",
				ColumnBuffer::uint2(Vec::<u16>::new()),
			)]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Uint2]);
			let mut row_one = shape.allocate_table();
			shape.set_values(&mut row_one, &[Value::Uint2(100)]);
			let mut row_two = shape.allocate_table();
			shape.set_values(&mut row_two, &[Value::Uint2(200)]);

			test_instance.append_rows(&shape, [row_one.freeze(), row_two.freeze()], vec![]).unwrap();

			assert_eq!(test_instance[0], ColumnBuffer::uint2([100, 200]));
		}

		#[test]
		fn test_all_defined_uint4() {
			let mut test_instance = Columns::new(vec![ColumnWithName::new(
				"test_col",
				ColumnBuffer::uint4(Vec::<u32>::new()),
			)]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Uint4]);
			let mut row_one = shape.allocate_table();
			shape.set_values(&mut row_one, &[Value::Uint4(1000)]);
			let mut row_two = shape.allocate_table();
			shape.set_values(&mut row_two, &[Value::Uint4(2000)]);

			test_instance.append_rows(&shape, [row_one.freeze(), row_two.freeze()], vec![]).unwrap();

			assert_eq!(test_instance[0], ColumnBuffer::uint4([1000, 2000]));
		}

		#[test]
		fn test_all_defined_uint8() {
			let mut test_instance = Columns::new(vec![ColumnWithName::new(
				"test_col",
				ColumnBuffer::uint8(Vec::<u64>::new()),
			)]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Uint8]);
			let mut row_one = shape.allocate_table();
			shape.set_values(&mut row_one, &[Value::Uint8(10000)]);
			let mut row_two = shape.allocate_table();
			shape.set_values(&mut row_two, &[Value::Uint8(20000)]);

			test_instance.append_rows(&shape, [row_one.freeze(), row_two.freeze()], vec![]).unwrap();

			assert_eq!(test_instance[0], ColumnBuffer::uint8([10000, 20000]));
		}

		#[test]
		fn test_all_defined_uint16() {
			let mut test_instance = Columns::new(vec![ColumnWithName::new(
				"test_col",
				ColumnBuffer::uint16(Vec::<u128>::new()),
			)]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Uint16]);
			let mut row_one = shape.allocate_table();
			shape.set_values(&mut row_one, &[Value::Uint16(1000)]);
			let mut row_two = shape.allocate_table();
			shape.set_values(&mut row_two, &[Value::Uint16(2000)]);

			test_instance.append_rows(&shape, [row_one.freeze(), row_two.freeze()], vec![]).unwrap();

			assert_eq!(test_instance[0], ColumnBuffer::uint16([1000, 2000]));
		}

		#[test]
		fn test_row_with_undefined() {
			let mut test_instance = test_instance_with_columns();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Int2, ValueType::Boolean]);
			let mut row = shape.allocate_table();
			shape.set_values(&mut row, &[Value::none(), Value::Boolean(false)]);

			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(test_instance[0], ColumnBuffer::int2_with_bitvec(vec![1, 0], vec![true, false]));
			assert_eq!(test_instance[1], ColumnBuffer::bool_with_bitvec([true, false], vec![true, true]));
		}

		#[test]
		fn test_row_with_type_mismatch_fails() {
			let mut test_instance = test_instance_with_columns();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Boolean, ValueType::Boolean]);
			let mut row = shape.allocate_table();
			shape.set_values(&mut row, &[Value::Boolean(true), Value::Boolean(true)]);

			let result = test_instance.append_rows(&shape, [row.freeze()], vec![]);
			assert!(result.is_err());
			assert!(result.unwrap_err().to_string().contains("type mismatch"));
		}

		#[test]
		fn test_row_wrong_length_fails() {
			let mut test_instance = test_instance_with_columns();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Int2]);
			let mut row = shape.allocate_table();
			shape.set_values(&mut row, &[Value::Int2(2)]);

			let result = test_instance.append_rows(&shape, [row.freeze()], vec![]);
			assert!(result.is_err());
			assert!(result.unwrap_err().to_string().contains("mismatched column count"));
		}

		#[test]
		fn test_fallback_bool() {
			let mut test_instance = Columns::new(vec![
				ColumnWithName::bool("test_col", Vec::<bool>::new()),
				ColumnWithName::bool("none", Vec::<bool>::new()),
			]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Boolean, ValueType::Boolean]);
			let mut row_one = shape.allocate_table();
			shape.set::<bool>(&mut row_one, 0, true);
			shape.set_none(&mut row_one, 1);

			test_instance.append_rows(&shape, [row_one.freeze()], vec![]).unwrap();

			assert_eq!(test_instance[0], ColumnBuffer::bool_with_bitvec([true], vec![true]));

			assert_eq!(test_instance[1], ColumnBuffer::bool_with_bitvec([false], vec![false]));
		}

		#[test]
		fn test_fallback_float4() {
			let mut test_instance = Columns::new(vec![
				ColumnWithName::new("test_col", ColumnBuffer::float4(Vec::<f32>::new())),
				ColumnWithName::new("none", ColumnBuffer::float4(Vec::<f32>::new())),
			]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Float4, ValueType::Float4]);
			let mut row = shape.allocate_table();
			shape.set::<f32>(&mut row, 0, 1.5f32);
			shape.set_none(&mut row, 1);

			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(test_instance[0], ColumnBuffer::float4_with_bitvec([1.5], vec![true]));
			assert_eq!(test_instance[1], ColumnBuffer::float4_with_bitvec([0.0], vec![false]));
		}

		#[test]
		fn test_fallback_float8() {
			let mut test_instance = Columns::new(vec![
				ColumnWithName::new("test_col", ColumnBuffer::float8(Vec::<f64>::new())),
				ColumnWithName::new("none", ColumnBuffer::float8(Vec::<f64>::new())),
			]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Float8, ValueType::Float8]);
			let mut row = shape.allocate_table();
			shape.set::<f64>(&mut row, 0, 2.5f64);
			shape.set_none(&mut row, 1);

			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(test_instance[0], ColumnBuffer::float8_with_bitvec([2.5], vec![true]));
			assert_eq!(test_instance[1], ColumnBuffer::float8_with_bitvec([0.0], vec![false]));
		}

		#[test]
		fn test_fallback_int1() {
			let mut test_instance = Columns::new(vec![
				ColumnWithName::int1("test_col", Vec::<i8>::new()),
				ColumnWithName::int1("none", Vec::<i8>::new()),
			]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Int1, ValueType::Int1]);
			let mut row = shape.allocate_table();
			shape.set::<i8>(&mut row, 0, 42i8);
			shape.set_none(&mut row, 1);

			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(test_instance[0], ColumnBuffer::int1_with_bitvec([42], vec![true]));
			assert_eq!(test_instance[1], ColumnBuffer::int1_with_bitvec([0], vec![false]));
		}

		#[test]
		fn test_fallback_int2() {
			let mut test_instance = Columns::new(vec![
				ColumnWithName::new("test_col", ColumnBuffer::int2(Vec::<i16>::new())),
				ColumnWithName::new("none", ColumnBuffer::int2(Vec::<i16>::new())),
			]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Int2, ValueType::Int2]);
			let mut row = shape.allocate_table();
			shape.set::<i16>(&mut row, 0, -1234i16);
			shape.set_none(&mut row, 1);

			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(test_instance[0], ColumnBuffer::int2_with_bitvec([-1234], vec![true]));
			assert_eq!(test_instance[1], ColumnBuffer::int2_with_bitvec([0], vec![false]));
		}

		#[test]
		fn test_fallback_int4() {
			let mut test_instance = Columns::new(vec![
				ColumnWithName::int4("test_col", Vec::<i32>::new()),
				ColumnWithName::int4("none", Vec::<i32>::new()),
			]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Int4, ValueType::Int4]);
			let mut row = shape.allocate_table();
			shape.set::<i32>(&mut row, 0, 56789i32);
			shape.set_none(&mut row, 1);

			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(test_instance[0], ColumnBuffer::int4_with_bitvec([56789], vec![true]));
			assert_eq!(test_instance[1], ColumnBuffer::int4_with_bitvec([0], vec![false]));
		}

		#[test]
		fn test_fallback_int8() {
			let mut test_instance = Columns::new(vec![
				ColumnWithName::new("test_col", ColumnBuffer::int8(Vec::<i64>::new())),
				ColumnWithName::new("none", ColumnBuffer::int8(Vec::<i64>::new())),
			]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Int8, ValueType::Int8]);
			let mut row = shape.allocate_table();
			shape.set::<i64>(&mut row, 0, -987654321i64);
			shape.set_none(&mut row, 1);

			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(test_instance[0], ColumnBuffer::int8_with_bitvec([-987654321], vec![true]));
			assert_eq!(test_instance[1], ColumnBuffer::int8_with_bitvec([0], vec![false]));
		}

		#[test]
		fn test_fallback_int16() {
			let mut test_instance = Columns::new(vec![
				ColumnWithName::new("test_col", ColumnBuffer::int16(Vec::<i128>::new())),
				ColumnWithName::new("none", ColumnBuffer::int16(Vec::<i128>::new())),
			]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Int16, ValueType::Int16]);
			let mut row = shape.allocate_table();
			shape.set::<i128>(&mut row, 0, 123456789012345678901234567890i128);
			shape.set_none(&mut row, 1);

			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				test_instance[0],
				ColumnBuffer::int16_with_bitvec([123456789012345678901234567890i128], vec![true])
			);
			assert_eq!(test_instance[1], ColumnBuffer::int16_with_bitvec([0], vec![false]));
		}

		#[test]
		fn test_fallback_string() {
			let mut test_instance = Columns::new(vec![
				ColumnWithName::utf8("test_col", Vec::<String>::new()),
				ColumnWithName::utf8("none", Vec::<String>::new()),
			]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Utf8, ValueType::Utf8]);
			let mut row = shape.allocate_table();
			shape.set_utf8(&mut row, 0, "reifydb");
			shape.set_none(&mut row, 1);

			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				test_instance[0],
				ColumnBuffer::utf8_with_bitvec(["reifydb".to_string()], vec![true])
			);
			assert_eq!(test_instance[1], ColumnBuffer::utf8_with_bitvec(["".to_string()], vec![false]));
		}

		#[test]
		fn test_fallback_uint1() {
			let mut test_instance = Columns::new(vec![
				ColumnWithName::new("test_col", ColumnBuffer::uint1(Vec::<u8>::new())),
				ColumnWithName::new("none", ColumnBuffer::uint1(Vec::<u8>::new())),
			]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Uint1, ValueType::Uint1]);
			let mut row = shape.allocate_table();
			shape.set::<u8>(&mut row, 0, 255u8);
			shape.set_none(&mut row, 1);

			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(test_instance[0], ColumnBuffer::uint1_with_bitvec([255], vec![true]));
			assert_eq!(test_instance[1], ColumnBuffer::uint1_with_bitvec([0], vec![false]));
		}

		#[test]
		fn test_fallback_uint2() {
			let mut test_instance = Columns::new(vec![
				ColumnWithName::new("test_col", ColumnBuffer::uint2(Vec::<u16>::new())),
				ColumnWithName::new("none", ColumnBuffer::uint2(Vec::<u16>::new())),
			]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Uint2, ValueType::Uint2]);
			let mut row = shape.allocate_table();
			shape.set::<u16>(&mut row, 0, 65535u16);
			shape.set_none(&mut row, 1);

			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(test_instance[0], ColumnBuffer::uint2_with_bitvec([65535], vec![true]));
			assert_eq!(test_instance[1], ColumnBuffer::uint2_with_bitvec([0], vec![false]));
		}

		#[test]
		fn test_fallback_uint4() {
			let mut test_instance = Columns::new(vec![
				ColumnWithName::new("test_col", ColumnBuffer::uint4(Vec::<u32>::new())),
				ColumnWithName::new("none", ColumnBuffer::uint4(Vec::<u32>::new())),
			]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Uint4, ValueType::Uint4]);
			let mut row = shape.allocate_table();
			shape.set::<u32>(&mut row, 0, 4294967295u32);
			shape.set_none(&mut row, 1);

			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(test_instance[0], ColumnBuffer::uint4_with_bitvec([4294967295], vec![true]));
			assert_eq!(test_instance[1], ColumnBuffer::uint4_with_bitvec([0], vec![false]));
		}

		#[test]
		fn test_fallback_uint8() {
			let mut test_instance = Columns::new(vec![
				ColumnWithName::new("test_col", ColumnBuffer::uint8(Vec::<u64>::new())),
				ColumnWithName::new("none", ColumnBuffer::uint8(Vec::<u64>::new())),
			]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Uint8, ValueType::Uint8]);
			let mut row = shape.allocate_table();
			shape.set::<u64>(&mut row, 0, 18446744073709551615u64);
			shape.set_none(&mut row, 1);

			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				test_instance[0],
				ColumnBuffer::uint8_with_bitvec([18446744073709551615], vec![true])
			);
			assert_eq!(test_instance[1], ColumnBuffer::uint8_with_bitvec([0], vec![false]));
		}

		#[test]
		fn test_fallback_uint16() {
			let mut test_instance = Columns::new(vec![
				ColumnWithName::new("test_col", ColumnBuffer::uint16(Vec::<u128>::new())),
				ColumnWithName::new("none", ColumnBuffer::uint16(Vec::<u128>::new())),
			]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Uint16, ValueType::Uint16]);
			let mut row = shape.allocate_table();
			shape.set::<u128>(&mut row, 0, 340282366920938463463374607431768211455u128);
			shape.set_none(&mut row, 1);

			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				test_instance[0],
				ColumnBuffer::uint16_with_bitvec(
					[340282366920938463463374607431768211455u128],
					vec![true]
				)
			);
			assert_eq!(test_instance[1], ColumnBuffer::uint16_with_bitvec([0], vec![false]));
		}

		#[test]
		fn test_all_defined_dictionary_id() {
			let constraint = TypeConstraint::dictionary(DictionaryId::from(1u64), ValueType::Uint4);
			let shape = RowShape::new(RowFamily::Table, vec![RowShapeField::new("status", constraint)]);

			let mut test_instance = Columns::new(vec![ColumnWithName::new(
				"status",
				ColumnBuffer::dictionary_id(Vec::<DictionaryEntryId>::new()),
			)]);

			let mut row_one = shape.allocate_table();
			shape.set_values(&mut row_one, &[Value::DictionaryId(DictionaryEntryId::U4(10))]);
			let mut row_two = shape.allocate_table();
			shape.set_values(&mut row_two, &[Value::DictionaryId(DictionaryEntryId::U4(20))]);

			test_instance.append_rows(&shape, [row_one.freeze(), row_two.freeze()], vec![]).unwrap();

			assert_eq!(test_instance[0].get_value(0), Value::DictionaryId(DictionaryEntryId::U4(10)));
			assert_eq!(test_instance[0].get_value(1), Value::DictionaryId(DictionaryEntryId::U4(20)));
		}

		#[test]
		fn test_fallback_dictionary_id() {
			let dict_constraint = TypeConstraint::dictionary(DictionaryId::from(1u64), ValueType::Uint4);
			let shape = RowShape::new(
				RowFamily::Table,
				vec![
					RowShapeField::new("dict_col", dict_constraint),
					RowShapeField::unconstrained("bool_col", ValueType::Boolean),
				],
			);

			let mut test_instance = Columns::new(vec![
				ColumnWithName::new(
					"dict_col",
					ColumnBuffer::dictionary_id(Vec::<DictionaryEntryId>::new()),
				),
				ColumnWithName::bool("bool_col", Vec::<bool>::new()),
			]);

			let mut row = shape.allocate_table();
			shape.set_values(&mut row, &[Value::none(), Value::Boolean(true)]);

			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			assert!(!test_instance[0].is_defined(0));
			assert_eq!(test_instance[1].get_value(0), Value::Boolean(true));
		}

		#[test]
		fn test_before_undefined_dictionary_id() {
			let constraint = TypeConstraint::dictionary(DictionaryId::from(2u64), ValueType::Uint4);
			let shape = RowShape::new(RowFamily::Table, vec![RowShapeField::new("tag", constraint)]);

			let mut test_instance =
				Columns::new(vec![ColumnWithName::undefined_typed("tag", ValueType::Boolean, 2)]);

			let mut row = shape.allocate_table();
			shape.set_values(&mut row, &[Value::DictionaryId(DictionaryEntryId::U4(5))]);

			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			// The first two rows carry over from the undefined column the append promoted.
			assert!(!test_instance[0].is_defined(0));
			assert!(!test_instance[0].is_defined(1));
			assert!(test_instance[0].is_defined(2));
			assert_eq!(test_instance[0].get_value(2), Value::DictionaryId(DictionaryEntryId::U4(5)));
		}

		#[test]
		fn test_all_defined_identity_id() {
			let id1 = IdentityId::anonymous();
			let id2 = IdentityId::root();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::IdentityId]);
			let mut test_instance = Columns::new(vec![ColumnWithName::new(
				Fragment::internal("id_col"),
				ColumnBuffer::identity_id(Vec::<IdentityId>::new()),
			)]);

			let mut row_one = shape.allocate_table();
			shape.set::<IdentityId>(&mut row_one, 0, id1);
			let mut row_two = shape.allocate_table();
			shape.set::<IdentityId>(&mut row_two, 0, id2);

			test_instance.append_rows(&shape, [row_one.freeze(), row_two.freeze()], vec![]).unwrap();

			assert_eq!(test_instance[0].get_value(0), Value::IdentityId(id1));
			assert_eq!(test_instance[0].get_value(1), Value::IdentityId(id2));
		}

		#[test]
		fn test_fallback_identity_id() {
			let id = IdentityId::anonymous();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::IdentityId, ValueType::Boolean]);
			let mut test_instance = Columns::new(vec![
				ColumnWithName::new(
					Fragment::internal("id_col"),
					ColumnBuffer::identity_id(Vec::<IdentityId>::new()),
				),
				ColumnWithName::bool("bool_col", Vec::<bool>::new()),
			]);

			let mut row = shape.allocate_table();
			shape.set::<IdentityId>(&mut row, 0, id);
			shape.set_none(&mut row, 1);

			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(test_instance[0].get_value(0), Value::IdentityId(id));
			assert!(test_instance[0].is_defined(0));
			assert!(!test_instance[1].is_defined(0));
		}

		#[test]
		fn test_all_defined_blob() {
			let blob1 = Blob::new(vec![1, 2, 3]);
			let blob2 = Blob::new(vec![4, 5]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Blob]);
			let mut test_instance = Columns::new(vec![ColumnWithName::new(
				Fragment::internal("blob_col"),
				ColumnBuffer::blob(Vec::<Blob>::new()),
			)]);

			let mut row_one = shape.allocate_table();
			shape.set_blob(&mut row_one, 0, &blob1);
			let mut row_two = shape.allocate_table();
			shape.set_blob(&mut row_two, 0, &blob2);

			test_instance.append_rows(&shape, [row_one.freeze(), row_two.freeze()], vec![]).unwrap();

			assert_eq!(test_instance[0].get_value(0), Value::Blob(blob1));
			assert_eq!(test_instance[0].get_value(1), Value::Blob(blob2));
		}

		#[test]
		fn test_fallback_blob() {
			let blob = Blob::new(vec![10, 20, 30]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Blob, ValueType::Boolean]);
			let mut test_instance = Columns::new(vec![
				ColumnWithName::new(
					Fragment::internal("blob_col"),
					ColumnBuffer::blob(Vec::<Blob>::new()),
				),
				ColumnWithName::bool("bool_col", Vec::<bool>::new()),
			]);

			let mut row = shape.allocate_table();
			shape.set_blob(&mut row, 0, &blob);
			shape.set_none(&mut row, 1);

			test_instance.append_rows(&shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(test_instance[0].get_value(0), Value::Blob(blob));
			assert!(test_instance[0].is_defined(0));
			assert!(!test_instance[1].is_defined(0));
		}

		fn test_instance_with_columns() -> Columns {
			Columns::new(vec![
				ColumnWithName::new(Fragment::internal("int2"), ColumnBuffer::int2(vec![1])),
				ColumnWithName::new(Fragment::internal("bool"), ColumnBuffer::bool(vec![true])),
			])
		}
	}
}
