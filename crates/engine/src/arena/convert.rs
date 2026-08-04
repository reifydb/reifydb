// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Debug;

use bumpalo::Bump as BumpAlloc;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer};
use reifydb_value::{
	storage::{DataBitVec, DataVec, Plain, Storage},
	util::bitvec::BitVec,
	value::{
		Value,
		container::{
			any::AnyContainer, blob::BlobContainer, bool::BoolContainer, dictionary::DictionaryContainer,
			identity_id::IdentityIdContainer, number::NumberContainer, temporal::TemporalContainer,
			utf8::Utf8Container, uuid::UuidContainer,
		},
		dictionary::DictionaryEntryId,
		identity::IdentityId,
		is::{IsNumber, IsTemporal, IsUuid},
	},
};

use super::{Bump, BumpBitVec, BumpVec};

fn bitvec_to_plain<S: Storage>(src: &S::BitVec) -> BitVec {
	let len = DataBitVec::len(src);
	let mut dst = BitVec::with_capacity(len);
	for i in 0..len {
		dst.push(DataBitVec::get(src, i));
	}
	dst
}

fn bitvec_to_bump<'bump, S: Storage>(src: &S::BitVec, bump: &'bump BumpAlloc) -> BumpBitVec<'bump> {
	let len = DataBitVec::len(src);
	let mut dst = BumpBitVec::with_capacity_in(len, bump);
	for i in 0..len {
		DataBitVec::push(&mut dst, DataBitVec::get(src, i));
	}
	dst
}

fn vec_to_plain<T: Clone + PartialEq + 'static, S: Storage>(src: &S::Vec<T>) -> Vec<T> {
	let mut dst = Vec::with_capacity(DataVec::len(src));
	dst.extend_from_slice(DataVec::as_slice(src));
	dst
}

fn vec_to_bump<'bump, T: Clone + PartialEq + 'static, S: Storage>(
	src: &S::Vec<T>,
	bump: &'bump BumpAlloc,
) -> BumpVec<'bump, T> {
	let mut dst = BumpVec::with_capacity_in(DataVec::len(src), bump);
	DataVec::extend_from_slice(&mut dst, DataVec::as_slice(src));
	dst
}

fn number_to_plain<T: IsNumber + Clone + Debug + Default, S: Storage>(
	src: &NumberContainer<T, S>,
) -> NumberContainer<T, Plain> {
	NumberContainer::from_parts(vec_to_plain::<T, S>(src.data()))
}

fn number_to_bump<'bump, T: IsNumber + Clone + Debug + Default, S: Storage>(
	src: &NumberContainer<T, S>,
	bump: &'bump BumpAlloc,
) -> NumberContainer<T, Bump<'bump>> {
	NumberContainer::from_parts(vec_to_bump::<T, S>(src.data(), bump))
}

fn bool_to_plain<S: Storage>(src: &BoolContainer<S>) -> BoolContainer<Plain> {
	BoolContainer::from_parts(bitvec_to_plain::<S>(src.data()))
}

fn bool_to_bump<'bump, S: Storage>(src: &BoolContainer<S>, bump: &'bump BumpAlloc) -> BoolContainer<Bump<'bump>> {
	BoolContainer::from_parts(bitvec_to_bump::<S>(src.data(), bump))
}

fn temporal_to_plain<T: IsTemporal + Clone + Debug + Default, S: Storage>(
	src: &TemporalContainer<T, S>,
) -> TemporalContainer<T, Plain> {
	TemporalContainer::from_parts(vec_to_plain::<T, S>(src.data()))
}

fn temporal_to_bump<'bump, T: IsTemporal + Clone + Debug + Default, S: Storage>(
	src: &TemporalContainer<T, S>,
	bump: &'bump BumpAlloc,
) -> TemporalContainer<T, Bump<'bump>> {
	TemporalContainer::from_parts(vec_to_bump::<T, S>(src.data(), bump))
}

fn uuid_to_plain<T: IsUuid + Clone + Debug + Default, S: Storage>(
	src: &UuidContainer<T, S>,
) -> UuidContainer<T, Plain> {
	UuidContainer::from_parts(vec_to_plain::<T, S>(src.data()))
}

fn uuid_to_bump<'bump, T: IsUuid + Clone + Debug + Default, S: Storage>(
	src: &UuidContainer<T, S>,
	bump: &'bump BumpAlloc,
) -> UuidContainer<T, Bump<'bump>> {
	UuidContainer::from_parts(vec_to_bump::<T, S>(src.data(), bump))
}

fn utf8_to_plain<S: Storage>(src: &Utf8Container<S>) -> Utf8Container<Plain> {
	let data = vec_to_plain::<u8, S>(src.data_storage());
	let offsets = vec_to_plain::<u64, S>(src.offsets_storage());
	Utf8Container::from_storage_parts(data, offsets)
}

fn utf8_to_bump<'bump, S: Storage>(src: &Utf8Container<S>, bump: &'bump BumpAlloc) -> Utf8Container<Bump<'bump>> {
	let data = vec_to_bump::<u8, S>(src.data_storage(), bump);
	let offsets = vec_to_bump::<u64, S>(src.offsets_storage(), bump);
	Utf8Container::from_storage_parts(data, offsets)
}

fn blob_to_plain<S: Storage>(src: &BlobContainer<S>) -> BlobContainer<Plain> {
	let data = vec_to_plain::<u8, S>(src.data_storage());
	let offsets = vec_to_plain::<u64, S>(src.offsets_storage());
	BlobContainer::from_storage_parts(data, offsets)
}

fn blob_to_bump<'bump, S: Storage>(src: &BlobContainer<S>, bump: &'bump BumpAlloc) -> BlobContainer<Bump<'bump>> {
	let data = vec_to_bump::<u8, S>(src.data_storage(), bump);
	let offsets = vec_to_bump::<u64, S>(src.offsets_storage(), bump);
	BlobContainer::from_storage_parts(data, offsets)
}

fn identity_id_to_plain<S: Storage>(src: &IdentityIdContainer<S>) -> IdentityIdContainer<Plain> {
	IdentityIdContainer::from_parts(vec_to_plain::<IdentityId, S>(src.data()))
}

fn identity_id_to_bump<'bump, S: Storage>(
	src: &IdentityIdContainer<S>,
	bump: &'bump BumpAlloc,
) -> IdentityIdContainer<Bump<'bump>> {
	IdentityIdContainer::from_parts(vec_to_bump::<IdentityId, S>(src.data(), bump))
}

fn any_to_plain<S: Storage>(src: &AnyContainer<S>) -> AnyContainer<Plain> {
	AnyContainer::from_parts(vec_to_plain::<Box<Value>, S>(src.data()))
}

fn any_to_bump<'bump, S: Storage>(src: &AnyContainer<S>, bump: &'bump BumpAlloc) -> AnyContainer<Bump<'bump>> {
	AnyContainer::from_parts(vec_to_bump::<Box<Value>, S>(src.data(), bump))
}

fn dictionary_to_plain<S: Storage>(src: &DictionaryContainer<S>) -> DictionaryContainer<Plain> {
	DictionaryContainer::from_parts(vec_to_plain::<DictionaryEntryId, S>(src.data()), src.dictionary_id())
}

fn dictionary_to_bump<'bump, S: Storage>(
	src: &DictionaryContainer<S>,
	bump: &'bump BumpAlloc,
) -> DictionaryContainer<Bump<'bump>> {
	DictionaryContainer::from_parts(vec_to_bump::<DictionaryEntryId, S>(src.data(), bump), src.dictionary_id())
}

pub fn column_data_to_plain<S: Storage>(src: &ColumnBuffer<S>) -> ColumnBuffer<Plain> {
	match src {
		ColumnBuffer::Bool(c) => ColumnBuffer::Bool(bool_to_plain(c)),
		ColumnBuffer::Float4(c) => ColumnBuffer::Float4(number_to_plain(c)),
		ColumnBuffer::Float8(c) => ColumnBuffer::Float8(number_to_plain(c)),
		ColumnBuffer::Int1(c) => ColumnBuffer::Int1(number_to_plain(c)),
		ColumnBuffer::Int2(c) => ColumnBuffer::Int2(number_to_plain(c)),
		ColumnBuffer::Int4(c) => ColumnBuffer::Int4(number_to_plain(c)),
		ColumnBuffer::Int8(c) => ColumnBuffer::Int8(number_to_plain(c)),
		ColumnBuffer::Int16(c) => ColumnBuffer::Int16(number_to_plain(c)),
		ColumnBuffer::Uint1(c) => ColumnBuffer::Uint1(number_to_plain(c)),
		ColumnBuffer::Uint2(c) => ColumnBuffer::Uint2(number_to_plain(c)),
		ColumnBuffer::Uint4(c) => ColumnBuffer::Uint4(number_to_plain(c)),
		ColumnBuffer::Uint8(c) => ColumnBuffer::Uint8(number_to_plain(c)),
		ColumnBuffer::Uint16(c) => ColumnBuffer::Uint16(number_to_plain(c)),
		ColumnBuffer::Utf8 {
			container,
			max_bytes,
		} => ColumnBuffer::Utf8 {
			container: utf8_to_plain(container),
			max_bytes: *max_bytes,
		},
		ColumnBuffer::Date(c) => ColumnBuffer::Date(temporal_to_plain(c)),
		ColumnBuffer::DateTime(c) => ColumnBuffer::DateTime(temporal_to_plain(c)),
		ColumnBuffer::Time(c) => ColumnBuffer::Time(temporal_to_plain(c)),
		ColumnBuffer::Duration(c) => ColumnBuffer::Duration(temporal_to_plain(c)),
		ColumnBuffer::IdentityId(c) => ColumnBuffer::IdentityId(identity_id_to_plain(c)),
		ColumnBuffer::Uuid4(c) => ColumnBuffer::Uuid4(uuid_to_plain(c)),
		ColumnBuffer::Uuid7(c) => ColumnBuffer::Uuid7(uuid_to_plain(c)),
		ColumnBuffer::Blob {
			container,
			max_bytes,
		} => ColumnBuffer::Blob {
			container: blob_to_plain(container),
			max_bytes: *max_bytes,
		},
		ColumnBuffer::Int {
			container,
			max_bytes,
		} => ColumnBuffer::Int {
			container: number_to_plain(container),
			max_bytes: *max_bytes,
		},
		ColumnBuffer::Uint {
			container,
			max_bytes,
		} => ColumnBuffer::Uint {
			container: number_to_plain(container),
			max_bytes: *max_bytes,
		},
		ColumnBuffer::Decimal {
			container,
			precision,
			scale,
		} => ColumnBuffer::Decimal {
			container: number_to_plain(container),
			precision: *precision,
			scale: *scale,
		},
		ColumnBuffer::Any(c) => ColumnBuffer::Any(any_to_plain(c)),
		ColumnBuffer::DictionaryId(c) => ColumnBuffer::DictionaryId(dictionary_to_plain(c)),
		ColumnBuffer::Option {
			inner,
			bitvec,
		} => ColumnBuffer::Option {
			inner: Box::new(column_data_to_plain(inner)),
			bitvec: bitvec_to_plain::<S>(bitvec),
		},
	}
}

pub fn column_data_to_bump<'bump, S: Storage>(
	src: &ColumnBuffer<S>,
	bump: &'bump BumpAlloc,
) -> ColumnBuffer<Bump<'bump>> {
	match src {
		ColumnBuffer::Bool(c) => ColumnBuffer::Bool(bool_to_bump(c, bump)),
		ColumnBuffer::Float4(c) => ColumnBuffer::Float4(number_to_bump(c, bump)),
		ColumnBuffer::Float8(c) => ColumnBuffer::Float8(number_to_bump(c, bump)),
		ColumnBuffer::Int1(c) => ColumnBuffer::Int1(number_to_bump(c, bump)),
		ColumnBuffer::Int2(c) => ColumnBuffer::Int2(number_to_bump(c, bump)),
		ColumnBuffer::Int4(c) => ColumnBuffer::Int4(number_to_bump(c, bump)),
		ColumnBuffer::Int8(c) => ColumnBuffer::Int8(number_to_bump(c, bump)),
		ColumnBuffer::Int16(c) => ColumnBuffer::Int16(number_to_bump(c, bump)),
		ColumnBuffer::Uint1(c) => ColumnBuffer::Uint1(number_to_bump(c, bump)),
		ColumnBuffer::Uint2(c) => ColumnBuffer::Uint2(number_to_bump(c, bump)),
		ColumnBuffer::Uint4(c) => ColumnBuffer::Uint4(number_to_bump(c, bump)),
		ColumnBuffer::Uint8(c) => ColumnBuffer::Uint8(number_to_bump(c, bump)),
		ColumnBuffer::Uint16(c) => ColumnBuffer::Uint16(number_to_bump(c, bump)),
		ColumnBuffer::Utf8 {
			container,
			max_bytes,
		} => ColumnBuffer::Utf8 {
			container: utf8_to_bump(container, bump),
			max_bytes: *max_bytes,
		},
		ColumnBuffer::Date(c) => ColumnBuffer::Date(temporal_to_bump(c, bump)),
		ColumnBuffer::DateTime(c) => ColumnBuffer::DateTime(temporal_to_bump(c, bump)),
		ColumnBuffer::Time(c) => ColumnBuffer::Time(temporal_to_bump(c, bump)),
		ColumnBuffer::Duration(c) => ColumnBuffer::Duration(temporal_to_bump(c, bump)),
		ColumnBuffer::IdentityId(c) => ColumnBuffer::IdentityId(identity_id_to_bump(c, bump)),
		ColumnBuffer::Uuid4(c) => ColumnBuffer::Uuid4(uuid_to_bump(c, bump)),
		ColumnBuffer::Uuid7(c) => ColumnBuffer::Uuid7(uuid_to_bump(c, bump)),
		ColumnBuffer::Blob {
			container,
			max_bytes,
		} => ColumnBuffer::Blob {
			container: blob_to_bump(container, bump),
			max_bytes: *max_bytes,
		},
		ColumnBuffer::Int {
			container,
			max_bytes,
		} => ColumnBuffer::Int {
			container: number_to_bump(container, bump),
			max_bytes: *max_bytes,
		},
		ColumnBuffer::Uint {
			container,
			max_bytes,
		} => ColumnBuffer::Uint {
			container: number_to_bump(container, bump),
			max_bytes: *max_bytes,
		},
		ColumnBuffer::Decimal {
			container,
			precision,
			scale,
		} => ColumnBuffer::Decimal {
			container: number_to_bump(container, bump),
			precision: *precision,
			scale: *scale,
		},
		ColumnBuffer::Any(c) => ColumnBuffer::Any(any_to_bump(c, bump)),
		ColumnBuffer::DictionaryId(c) => ColumnBuffer::DictionaryId(dictionary_to_bump(c, bump)),
		ColumnBuffer::Option {
			inner,
			bitvec,
		} => ColumnBuffer::Option {
			inner: Box::new(column_data_to_bump(inner, bump)),
			bitvec: bitvec_to_bump::<S>(bitvec, bump),
		},
	}
}

pub fn column_to_plain(src: &ColumnWithName) -> ColumnWithName {
	ColumnWithName::new(src.name().clone(), column_data_to_plain::<Plain>(src.data()))
}

pub fn column_to_bump(src: &ColumnWithName, _bump: &BumpAlloc) -> ColumnWithName {
	ColumnWithName::new(src.name().clone(), column_data_to_plain::<Plain>(src.data()))
}

#[cfg(test)]
mod tests {
	use reifydb_core::value::column::ColumnWithName;
	use reifydb_value::value::value_type::ValueType;

	use super::*;

	#[test]
	fn test_column_data_cow_roundtrip() {
		let original = ColumnBuffer::int4(vec![10, 20, 30]);
		let bump_alloc = BumpAlloc::new();

		let bump_data = column_data_to_bump::<Plain>(&original, &bump_alloc);
		assert_eq!(bump_data.len(), 3);

		let cow_data = column_data_to_plain::<Bump>(&bump_data);
		assert_eq!(cow_data, original);
	}

	#[test]
	fn test_column_data_bool_roundtrip() {
		let original = ColumnBuffer::bool(vec![true, false, true]);
		let bump_alloc = BumpAlloc::new();

		let bump_data = column_data_to_bump::<Plain>(&original, &bump_alloc);
		let cow_data = column_data_to_plain::<Bump>(&bump_data);
		assert_eq!(cow_data, original);
	}

	#[test]
	fn test_column_data_utf8_roundtrip() {
		let original = ColumnBuffer::utf8(vec![String::from("hello"), String::from("world")]);
		let bump_alloc = BumpAlloc::new();

		let bump_data = column_data_to_bump::<Plain>(&original, &bump_alloc);
		let cow_data = column_data_to_plain::<Bump>(&bump_data);
		assert_eq!(cow_data, original);
	}

	#[test]
	fn test_column_data_float8_roundtrip() {
		let original = ColumnBuffer::float8(vec![1.5, 2.7, 3.9]);
		let bump_alloc = BumpAlloc::new();

		let bump_data = column_data_to_bump::<Plain>(&original, &bump_alloc);
		let cow_data = column_data_to_plain::<Bump>(&bump_data);
		assert_eq!(cow_data, original);
	}

	#[test]
	fn test_column_data_none_roundtrip() {
		let original = ColumnBuffer::none_typed(ValueType::Boolean, 5);
		let bump_alloc = BumpAlloc::new();

		let bump_data = column_data_to_bump::<Plain>(&original, &bump_alloc);
		let cow_data = column_data_to_plain::<Bump>(&bump_data);
		assert_eq!(cow_data, original);
	}

	#[test]
	fn test_column_roundtrip() {
		let original = ColumnWithName::int4("age", vec![25, 30, 35]);
		let bump_alloc = BumpAlloc::new();

		let bump_col = column_to_bump(&original, &bump_alloc);
		let cow_col = column_to_plain(&bump_col);
		assert_eq!(cow_col, original);
	}
}
