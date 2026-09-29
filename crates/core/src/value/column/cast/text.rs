// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Display;

use arrow_array::{Array, ArrayRef, BooleanArray, LargeBinaryArray};
use arrow_schema::FieldRef;
use reifydb_value::{
	Result,
	error::TypeError,
	fragment::LazyFragment,
	value::{
		blob::Blob,
		column_view::{ColumnView, ViewData},
		container::{
			decimal_array::decimals,
			temporal_array::{dates, datetimes, durations, times},
			uuid_array::{identity_ids, uuid4s, uuid7s},
			wide_int_array::wides,
		},
		identity::IdentityId,
		is::{IsNumber, IsTemporal, IsUuid},
		value_type::ValueType,
	},
};

use super::error::CastError;
use crate::value::column::builder::ColumnBuilder;

pub fn to_text(data: &ColumnView, lazy_fragment: impl LazyFragment) -> Result<(FieldRef, ArrayRef)> {
	let name = data.field.name();
	match &data.data {
		ViewData::Blob {
			container,
			..
		} => from_blob(container, name, lazy_fragment),
		ViewData::Bool(container) => from_bool(container, name),
		ViewData::Int1(container) => from_number(container.values(), name),
		ViewData::Int2(container) => from_number(container.values(), name),
		ViewData::Int4(container) => from_number(container.values(), name),
		ViewData::Int8(container) => from_number(container.values(), name),
		ViewData::Int16(container) => from_number(&wides::<i128>(container), name),
		ViewData::Uint1(container) => from_number(container.values(), name),
		ViewData::Uint2(container) => from_number(container.values(), name),
		ViewData::Uint4(container) => from_number(container.values(), name),
		ViewData::Uint8(container) => from_number(container.values(), name),
		ViewData::Uint16(container) => from_number(&wides::<u128>(container), name),
		ViewData::Float4(container) => from_number(container.values(), name),
		ViewData::Float8(container) => from_number(container.values(), name),
		ViewData::Decimal(container) => from_number(&decimals(container), name),
		ViewData::Date(container) => from_temporal(dates(container), name),
		ViewData::DateTime(container) => from_temporal(datetimes(container), name),
		ViewData::Time(container) => from_temporal(times(container), name),
		ViewData::Duration(container) => from_temporal(durations(container), name),
		ViewData::Uuid4(container) => from_uuid(uuid4s(container), name),
		ViewData::Uuid7(container) => from_uuid(uuid7s(container), name),
		ViewData::IdentityId(container) => from_identity_id(identity_ids(container), name),
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
pub fn from_blob(
	container: &LargeBinaryArray,
	name: &str,
	lazy_fragment: impl LazyFragment,
) -> Result<(FieldRef, ArrayRef)> {
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
	Ok(out.finish(name))
}

#[inline]
fn from_bool(container: &BooleanArray, name: &str) -> Result<(FieldRef, ArrayRef)> {
	let mut out = ColumnBuilder::with_capacity(ValueType::Utf8, container.len());
	for idx in 0..container.len() {
		if container.is_valid(idx) {
			out.push::<String>(container.value(idx).to_string());
		} else {
			out.push_none();
		}
	}
	Ok(out.finish(name))
}

#[inline]
fn from_number<T>(container: &[T], name: &str) -> Result<(FieldRef, ArrayRef)>
where
	T: Display + IsNumber,
{
	let mut out = ColumnBuilder::with_capacity(ValueType::Utf8, container.len());
	for value in container {
		out.push::<String>(value.to_string());
	}
	Ok(out.finish(name))
}

#[inline]
fn from_temporal<T>(container: &[T], name: &str) -> Result<(FieldRef, ArrayRef)>
where
	T: Display + IsTemporal,
{
	let mut out = ColumnBuilder::with_capacity(ValueType::Utf8, container.len());
	for value in container {
		out.push::<String>(value.to_string());
	}
	Ok(out.finish(name))
}

#[inline]
fn from_uuid<T>(container: &[T], name: &str) -> Result<(FieldRef, ArrayRef)>
where
	T: Display + IsUuid,
{
	let mut out = ColumnBuilder::with_capacity(ValueType::Utf8, container.len());
	for value in container {
		out.push::<String>(value.to_string());
	}
	Ok(out.finish(name))
}

#[inline]
fn from_identity_id(container: &[IdentityId], name: &str) -> Result<(FieldRef, ArrayRef)> {
	let mut out = ColumnBuilder::with_capacity(ValueType::Utf8, container.len());
	for value in container {
		out.push::<String>(value.to_string());
	}
	Ok(out.finish(name))
}

#[cfg(test)]
pub mod tests {
	use arrow_array::LargeBinaryArray;
	use reifydb_value::{
		fragment::Fragment,
		value::{
			blob::Blob,
			column_view::{ColumnView, ViewData},
			container::varlen_array::get,
		},
	};

	use crate::value::column::cast::text::from_blob;

	#[test]
	fn test_from_blob() {
		let blobs =
			[Blob::from_utf8(Fragment::internal("Hello")), Blob::from_utf8(Fragment::internal("World"))];
		let container = LargeBinaryArray::from_iter_values(blobs.iter().map(|blob| blob.as_bytes()));

		let result = from_blob(&container, "x", Fragment::testing_empty).unwrap();

		match ColumnView::try_from(&result).unwrap().data {
			ViewData::Utf8 {
				container,
				..
			} => {
				assert_eq!(get(container, 0), Some("Hello"));
				assert_eq!(get(container, 1), Some("World"));
			}
			_ => panic!("Expected UTF8 column data"),
		}
	}

	#[test]
	fn test_from_blob_invalid() {
		let blobs = [
			Blob::new(vec![0xFF, 0xFE]), // Invalid UTF-8
		];
		let container = LargeBinaryArray::from_iter_values(blobs.iter().map(|blob| blob.as_bytes()));

		let result = from_blob(&container, "x", Fragment::testing_empty);
		assert!(result.is_err());
	}
}
