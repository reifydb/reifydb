// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Display;

use arrow_array::{Array, BooleanArray, LargeBinaryArray};
use reifydb_value::{
	Result,
	error::TypeError,
	fragment::LazyFragment,
	value::{
		blob::Blob,
		container::{
			decimal_array::u128s,
			temporal_array::{dates, datetimes, durations, times},
			uuid_array::{identity_ids, uuid4s, uuid7s},
		},
		identity::IdentityId,
		is::{IsNumber, IsTemporal, IsUuid},
		value_type::ValueType,
	},
};

use super::error::CastError;
use crate::value::column::{buffer::ColumnBuffer, builder::ColumnBuilder};

pub fn to_text(data: &ColumnBuffer, lazy_fragment: impl LazyFragment) -> Result<ColumnBuffer> {
	match data {
		ColumnBuffer::Blob {
			container,
			..
		} => from_blob(container, lazy_fragment),
		ColumnBuffer::Bool(container) => from_bool(container),
		ColumnBuffer::Int1(container) => from_number(container.values()),
		ColumnBuffer::Int2(container) => from_number(container.values()),
		ColumnBuffer::Int4(container) => from_number(container.values()),
		ColumnBuffer::Int8(container) => from_number(container.values()),
		ColumnBuffer::Int16(container) => from_number(container.values()),
		ColumnBuffer::Uint1(container) => from_number(container.values()),
		ColumnBuffer::Uint2(container) => from_number(container.values()),
		ColumnBuffer::Uint4(container) => from_number(container.values()),
		ColumnBuffer::Uint8(container) => from_number(container.values()),
		ColumnBuffer::Uint16(container) => from_number(&u128s(container)),
		ColumnBuffer::Float4(container) => from_number(container.values()),
		ColumnBuffer::Float8(container) => from_number(container.values()),
		ColumnBuffer::Int {
			container,
			..
		} => from_number(container),
		ColumnBuffer::Uint {
			container,
			..
		} => from_number(container),
		ColumnBuffer::Decimal {
			container,
			..
		} => from_number(container),
		ColumnBuffer::Date(container) => from_temporal(dates(container)),
		ColumnBuffer::DateTime(container) => from_temporal(datetimes(container)),
		ColumnBuffer::Time(container) => from_temporal(times(container)),
		ColumnBuffer::Duration(container) => from_temporal(durations(container)),
		ColumnBuffer::Uuid4(container) => from_uuid(uuid4s(container)),
		ColumnBuffer::Uuid7(container) => from_uuid(uuid7s(container)),
		ColumnBuffer::IdentityId(container) => from_identity_id(identity_ids(container)),
		_ => {
			let from = data.get_type();
			Err(TypeError::UnsupportedCast {
				from,
				to: ValueType::Utf8,
				fragment: lazy_fragment.fragment(),
			}
			.into())
		}
	}
}

#[inline]
pub fn from_blob(container: &LargeBinaryArray, lazy_fragment: impl LazyFragment) -> Result<ColumnBuffer> {
	let mut out = ColumnBuilder::with_capacity(ValueType::Utf8, container.len());
	for idx in 0..container.len() {
		let blob = Blob::new(container.value(idx).to_vec());
		match blob.to_utf8() {
			Ok(s) => out.push(s),
			Err(e) => {
				return Err(CastError::InvalidBlobToUtf8 {
					fragment: lazy_fragment.fragment(),
					cause: e.diagnostic(),
				}
				.into());
			}
		}
	}
	Ok(out.finish())
}

#[inline]
fn from_bool(container: &BooleanArray) -> Result<ColumnBuffer> {
	let mut out = ColumnBuilder::with_capacity(ValueType::Utf8, container.len());
	for idx in 0..container.len() {
		if container.is_valid(idx) {
			out.push::<String>(container.value(idx).to_string());
		} else {
			out.push_none();
		}
	}
	Ok(out.finish())
}

#[inline]
fn from_number<T>(container: &[T]) -> Result<ColumnBuffer>
where
	T: Display + IsNumber,
{
	let mut out = ColumnBuilder::with_capacity(ValueType::Utf8, container.len());
	for value in container {
		out.push::<String>(value.to_string());
	}
	Ok(out.finish())
}

#[inline]
fn from_temporal<T>(container: &[T]) -> Result<ColumnBuffer>
where
	T: Display + IsTemporal,
{
	let mut out = ColumnBuilder::with_capacity(ValueType::Utf8, container.len());
	for value in container {
		out.push::<String>(value.to_string());
	}
	Ok(out.finish())
}

#[inline]
fn from_uuid<T>(container: &[T]) -> Result<ColumnBuffer>
where
	T: Display + IsUuid,
{
	let mut out = ColumnBuilder::with_capacity(ValueType::Utf8, container.len());
	for value in container {
		out.push::<String>(value.to_string());
	}
	Ok(out.finish())
}

#[inline]
fn from_identity_id(container: &[IdentityId]) -> Result<ColumnBuffer> {
	let mut out = ColumnBuilder::with_capacity(ValueType::Utf8, container.len());
	for value in container {
		out.push::<String>(value.to_string());
	}
	Ok(out.finish())
}

#[cfg(test)]
pub mod tests {
	use arrow_array::LargeBinaryArray;
	use reifydb_value::{
		fragment::Fragment,
		value::{blob::Blob, container::varlen_array::get},
	};

	use crate::value::column::{buffer::ColumnBuffer, cast::text::from_blob};

	#[test]
	fn test_from_blob() {
		let blobs = vec![
			Blob::from_utf8(Fragment::internal("Hello")),
			Blob::from_utf8(Fragment::internal("World")),
		];
		let container = LargeBinaryArray::from_iter_values(blobs.iter().map(|blob| blob.as_bytes()));

		let result = from_blob(&container, || Fragment::testing_empty()).unwrap();

		match result {
			ColumnBuffer::Utf8 {
				container,
				..
			} => {
				assert_eq!(get(&container, 0), Some("Hello"));
				assert_eq!(get(&container, 1), Some("World"));
			}
			_ => panic!("Expected UTF8 column data"),
		}
	}

	#[test]
	fn test_from_blob_invalid() {
		let blobs = vec![
			Blob::new(vec![0xFF, 0xFE]), // Invalid UTF-8
		];
		let container = LargeBinaryArray::from_iter_values(blobs.iter().map(|blob| blob.as_bytes()));

		let result = from_blob(&container, || Fragment::testing_empty());
		assert!(result.is_err());
	}
}
