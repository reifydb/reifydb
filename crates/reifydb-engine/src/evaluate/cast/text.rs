// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use std::fmt::{Debug, Display};

use reifydb_core::{
	Error, Type, err,
	interface::fragment::LazyFragment,
	result::error::diagnostic::cast,
	value::{
		IsNumber, IsTemporal, IsUuid,
		container::{
			BlobContainer, BoolContainer, NumberContainer,
			TemporalContainer, UuidContainer,
		},
	},
};

use crate::columnar::ColumnData;

pub fn to_text<'a>(
	data: &ColumnData,
	lazy_fragment: impl LazyFragment<'a>,
) -> crate::Result<ColumnData> {
	match data {
		ColumnData::Blob(container) => {
			from_blob(container, lazy_fragment)
		}
		ColumnData::Bool(container) => from_bool(container),
		ColumnData::Int1(container) => from_number(container),
		ColumnData::Int2(container) => from_number(container),
		ColumnData::Int4(container) => from_number(container),
		ColumnData::Int8(container) => from_number(container),
		ColumnData::Int16(container) => from_number(container),
		ColumnData::Uint1(container) => from_number(container),
		ColumnData::Uint2(container) => from_number(container),
		ColumnData::Uint4(container) => from_number(container),
		ColumnData::Uint8(container) => from_number(container),
		ColumnData::Uint16(container) => from_number(container),
		ColumnData::Float4(container) => from_number(container),
		ColumnData::Float8(container) => from_number(container),
		ColumnData::Date(container) => from_temporal(container),
		ColumnData::DateTime(container) => from_temporal(container),
		ColumnData::Time(container) => from_temporal(container),
		ColumnData::Interval(container) => from_temporal(container),
		ColumnData::Uuid4(container) => from_uuid(container),
		ColumnData::Uuid7(container) => from_uuid(container),
		_ => {
			let source_type = data.get_type();
			err!(cast::unsupported_cast(
				lazy_fragment.fragment(),
				source_type,
				Type::Utf8
			))
		}
	}
}

#[inline]
pub fn from_blob<'a>(
	container: &BlobContainer,
	lazy_fragment: impl LazyFragment<'a>,
) -> crate::Result<ColumnData> {
	let mut out = ColumnData::with_capacity(Type::Utf8, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			match container[idx].to_utf8() {
				Ok(s) => out.push(s),
				Err(e) => {
					return Err(Error(
						cast::invalid_blob_to_utf8(
							lazy_fragment
								.fragment(),
							e.diagnostic(),
						),
					));
				}
			}
		} else {
			out.push_undefined()
		}
	}
	Ok(out)
}

#[inline]
fn from_bool(container: &BoolContainer) -> crate::Result<ColumnData> {
	let mut out = ColumnData::with_capacity(Type::Utf8, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			out.push::<String>(
				container.data().get(idx).to_string(),
			);
		} else {
			out.push_undefined();
		}
	}
	Ok(out)
}

#[inline]
fn from_number<T>(container: &NumberContainer<T>) -> crate::Result<ColumnData>
where
	T: Copy + Display + Clone + Debug + Default + IsNumber,
{
	let mut out = ColumnData::with_capacity(Type::Utf8, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			out.push::<String>(container[idx].to_string());
		} else {
			out.push_undefined();
		}
	}
	Ok(out)
}

#[inline]
fn from_temporal<T>(
	container: &TemporalContainer<T>,
) -> crate::Result<ColumnData>
where
	T: Copy + Display + Clone + Debug + Default + IsTemporal,
{
	let mut out = ColumnData::with_capacity(Type::Utf8, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			out.push::<String>(container[idx].to_string());
		} else {
			out.push_undefined();
		}
	}
	Ok(out)
}

#[inline]
fn from_uuid<T>(container: &UuidContainer<T>) -> crate::Result<ColumnData>
where
	T: Copy + Display + Clone + Debug + Default + IsUuid,
{
	let mut out = ColumnData::with_capacity(Type::Utf8, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			out.push::<String>(container[idx].to_string());
		} else {
			out.push_undefined();
		}
	}
	Ok(out)
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		BitVec, Blob, Fragment, value::container::BlobContainer,
	};

	use crate::{columnar::ColumnData, evaluate::cast::text::from_blob};

	#[test]
	fn test_from_blob() {
		use reifydb_core::interface::fragment::OwnedFragment;
		let blobs = vec![
			Blob::from_utf8(OwnedFragment::internal("Hello")),
			Blob::from_utf8(OwnedFragment::internal("World")),
		];
		let bitvec = BitVec::repeat(2, true);
		let container = BlobContainer::new(blobs, bitvec);

		let result =
			from_blob(&container, || Fragment::testing_empty())
				.unwrap();

		match result {
			ColumnData::Utf8(container) => {
				assert_eq!(container[0], "Hello");
				assert_eq!(container[1], "World");
			}
			_ => panic!("Expected UTF8 column data"),
		}
	}

	#[test]
	fn test_from_blob_invalid() {
		let blobs = vec![
			Blob::new(vec![0xFF, 0xFE]), // Invalid UTF-8
		];
		let bitvec = BitVec::repeat(1, true);
		let container = BlobContainer::new(blobs, bitvec);

		let result =
			from_blob(&container, || Fragment::testing_empty());
		assert!(result.is_err());
	}
}
