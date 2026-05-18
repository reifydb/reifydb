// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::fmt::Display;

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_type::{
	error::TypeError,
	fragment::LazyFragment,
	value::{
		blob::Blob,
		container::{
			blob::BlobContainer, bool::BoolContainer, identity_id::IdentityIdContainer,
			number::NumberContainer, temporal::TemporalContainer, uuid::UuidContainer,
		},
		is::{IsNumber, IsTemporal, IsUuid},
		r#type::Type,
	},
};

use crate::{Result, error::CastError};

pub fn to_text(data: &ColumnBuffer, lazy_fragment: impl LazyFragment) -> Result<ColumnBuffer> {
	match data {
		ColumnBuffer::Blob {
			container,
			..
		} => from_blob(container, lazy_fragment),
		ColumnBuffer::Bool(container) => from_bool(container),
		ColumnBuffer::Int1(container) => from_number(container),
		ColumnBuffer::Int2(container) => from_number(container),
		ColumnBuffer::Int4(container) => from_number(container),
		ColumnBuffer::Int8(container) => from_number(container),
		ColumnBuffer::Int16(container) => from_number(container),
		ColumnBuffer::Uint1(container) => from_number(container),
		ColumnBuffer::Uint2(container) => from_number(container),
		ColumnBuffer::Uint4(container) => from_number(container),
		ColumnBuffer::Uint8(container) => from_number(container),
		ColumnBuffer::Uint16(container) => from_number(container),
		ColumnBuffer::Float4(container) => from_number(container),
		ColumnBuffer::Float8(container) => from_number(container),
		ColumnBuffer::Date(container) => from_temporal(container),
		ColumnBuffer::DateTime(container) => from_temporal(container),
		ColumnBuffer::Time(container) => from_temporal(container),
		ColumnBuffer::Duration(container) => from_temporal(container),
		ColumnBuffer::Uuid4(container) => from_uuid(container),
		ColumnBuffer::Uuid7(container) => from_uuid(container),
		ColumnBuffer::IdentityId(container) => from_identity_id(container),
		_ => {
			let from = data.get_type();
			Err(TypeError::UnsupportedCast {
				from,
				to: Type::Utf8,
				fragment: lazy_fragment.fragment(),
			}
			.into())
		}
	}
}

#[inline]
pub fn from_blob(container: &BlobContainer, lazy_fragment: impl LazyFragment) -> Result<ColumnBuffer> {
	let mut out = ColumnBuffer::with_capacity(Type::Utf8, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			let blob = Blob::new(container.get(idx).unwrap_or(&[]).to_vec());
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
		} else {
			out.push_none()
		}
	}
	Ok(out)
}

#[inline]
fn from_bool(container: &BoolContainer) -> Result<ColumnBuffer> {
	let mut out = ColumnBuffer::with_capacity(Type::Utf8, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			out.push::<String>(container.data().get(idx).to_string());
		} else {
			out.push_none();
		}
	}
	Ok(out)
}

#[inline]
fn from_number<T>(container: &NumberContainer<T>) -> Result<ColumnBuffer>
where
	T: Copy + Display + IsNumber + Default,
{
	let mut out = ColumnBuffer::with_capacity(Type::Utf8, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			out.push::<String>(container[idx].to_string());
		} else {
			out.push_none();
		}
	}
	Ok(out)
}

#[inline]
fn from_temporal<T>(container: &TemporalContainer<T>) -> Result<ColumnBuffer>
where
	T: Copy + Display + IsTemporal + Default,
{
	let mut out = ColumnBuffer::with_capacity(Type::Utf8, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			out.push::<String>(container[idx].to_string());
		} else {
			out.push_none();
		}
	}
	Ok(out)
}

#[inline]
fn from_uuid<T>(container: &UuidContainer<T>) -> Result<ColumnBuffer>
where
	T: Copy + Display + IsUuid + Default,
{
	let mut out = ColumnBuffer::with_capacity(Type::Utf8, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			out.push::<String>(container[idx].to_string());
		} else {
			out.push_none();
		}
	}
	Ok(out)
}

#[inline]
fn from_identity_id(container: &IdentityIdContainer) -> Result<ColumnBuffer> {
	let mut out = ColumnBuffer::with_capacity(Type::Utf8, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			out.push::<String>(container[idx].to_string());
		} else {
			out.push_none();
		}
	}
	Ok(out)
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::value::column::buffer::ColumnBuffer;
	use reifydb_type::{
		fragment::Fragment,
		value::{blob::Blob, container::blob::BlobContainer},
	};

	use crate::expression::cast::text::from_blob;

	#[test]
	fn test_from_blob() {
		let blobs = vec![
			Blob::from_utf8(Fragment::internal("Hello")),
			Blob::from_utf8(Fragment::internal("World")),
		];
		let container = BlobContainer::new(blobs);

		let result = from_blob(&container, || Fragment::testing_empty()).unwrap();

		match result {
			ColumnBuffer::Utf8 {
				container,
				..
			} => {
				assert_eq!(container.get(0), Some("Hello"));
				assert_eq!(container.get(1), Some("World"));
			}
			_ => panic!("Expected UTF8 column data"),
		}
	}

	#[test]
	fn test_from_blob_invalid() {
		let blobs = vec![
			Blob::new(vec![0xFF, 0xFE]), // Invalid UTF-8
		];
		let container = BlobContainer::new(blobs);

		let result = from_blob(&container, || Fragment::testing_empty());
		assert!(result.is_err());
	}
}
