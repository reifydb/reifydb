// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::fmt::Debug;

use bumpalo::Bump as BumpAlloc;
use reifydb_core::value::column::{Column, data::ColumnData};
use reifydb_type::{
	storage::{Cow, DataBitVec, DataVec, Storage},
	util::{bitvec::BitVec, cowvec::CowVec},
	value::{
		Value,
		blob::Blob,
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

fn bitvec_to_cow<S: Storage>(src: &S::BitVec) -> BitVec {
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

fn vec_to_cow<T: Clone + PartialEq + 'static, S: Storage>(src: &S::Vec<T>) -> CowVec<T> {
	let mut dst = CowVec::with_capacity(DataVec::len(src));
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

fn number_to_cow<T: IsNumber + Clone + Debug + Default, S: Storage>(
	src: &NumberContainer<T, S>,
) -> NumberContainer<T, Cow> {
	NumberContainer::from_parts(vec_to_cow::<T, S>(src.data()))
}

fn number_to_bump<'bump, T: IsNumber + Clone + Debug + Default, S: Storage>(
	src: &NumberContainer<T, S>,
	bump: &'bump BumpAlloc,
) -> NumberContainer<T, Bump<'bump>> {
	NumberContainer::from_parts(vec_to_bump::<T, S>(src.data(), bump))
}

fn bool_to_cow<S: Storage>(src: &BoolContainer<S>) -> BoolContainer<Cow> {
	BoolContainer::from_parts(bitvec_to_cow::<S>(src.data()))
}

fn bool_to_bump<'bump, S: Storage>(src: &BoolContainer<S>, bump: &'bump BumpAlloc) -> BoolContainer<Bump<'bump>> {
	BoolContainer::from_parts(bitvec_to_bump::<S>(src.data(), bump))
}

fn temporal_to_cow<T: IsTemporal + Clone + Debug + Default, S: Storage>(
	src: &TemporalContainer<T, S>,
) -> TemporalContainer<T, Cow> {
	TemporalContainer::from_parts(vec_to_cow::<T, S>(src.data()))
}

fn temporal_to_bump<'bump, T: IsTemporal + Clone + Debug + Default, S: Storage>(
	src: &TemporalContainer<T, S>,
	bump: &'bump BumpAlloc,
) -> TemporalContainer<T, Bump<'bump>> {
	TemporalContainer::from_parts(vec_to_bump::<T, S>(src.data(), bump))
}

fn uuid_to_cow<T: IsUuid + Clone + Debug + Default, S: Storage>(src: &UuidContainer<T, S>) -> UuidContainer<T, Cow> {
	UuidContainer::from_parts(vec_to_cow::<T, S>(src.data()))
}

fn uuid_to_bump<'bump, T: IsUuid + Clone + Debug + Default, S: Storage>(
	src: &UuidContainer<T, S>,
	bump: &'bump BumpAlloc,
) -> UuidContainer<T, Bump<'bump>> {
	UuidContainer::from_parts(vec_to_bump::<T, S>(src.data(), bump))
}

fn utf8_to_cow<S: Storage>(src: &Utf8Container<S>) -> Utf8Container<Cow> {
	Utf8Container::from_parts(vec_to_cow::<String, S>(src.data()))
}

fn utf8_to_bump<'bump, S: Storage>(src: &Utf8Container<S>, bump: &'bump BumpAlloc) -> Utf8Container<Bump<'bump>> {
	Utf8Container::from_parts(vec_to_bump::<String, S>(src.data(), bump))
}

fn blob_to_cow<S: Storage>(src: &BlobContainer<S>) -> BlobContainer<Cow> {
	BlobContainer::from_parts(vec_to_cow::<Blob, S>(src.data()))
}

fn blob_to_bump<'bump, S: Storage>(src: &BlobContainer<S>, bump: &'bump BumpAlloc) -> BlobContainer<Bump<'bump>> {
	BlobContainer::from_parts(vec_to_bump::<Blob, S>(src.data(), bump))
}

fn identity_id_to_cow<S: Storage>(src: &IdentityIdContainer<S>) -> IdentityIdContainer<Cow> {
	IdentityIdContainer::from_parts(vec_to_cow::<IdentityId, S>(src.data()))
}

fn identity_id_to_bump<'bump, S: Storage>(
	src: &IdentityIdContainer<S>,
	bump: &'bump BumpAlloc,
) -> IdentityIdContainer<Bump<'bump>> {
	IdentityIdContainer::from_parts(vec_to_bump::<IdentityId, S>(src.data(), bump))
}

fn any_to_cow<S: Storage>(src: &AnyContainer<S>) -> AnyContainer<Cow> {
	AnyContainer::from_parts(vec_to_cow::<Box<Value>, S>(src.data()))
}

fn any_to_bump<'bump, S: Storage>(src: &AnyContainer<S>, bump: &'bump BumpAlloc) -> AnyContainer<Bump<'bump>> {
	AnyContainer::from_parts(vec_to_bump::<Box<Value>, S>(src.data(), bump))
}

fn dictionary_to_cow<S: Storage>(src: &DictionaryContainer<S>) -> DictionaryContainer<Cow> {
	DictionaryContainer::from_parts(vec_to_cow::<DictionaryEntryId, S>(src.data()), src.dictionary_id())
}

fn dictionary_to_bump<'bump, S: Storage>(
	src: &DictionaryContainer<S>,
	bump: &'bump BumpAlloc,
) -> DictionaryContainer<Bump<'bump>> {
	DictionaryContainer::from_parts(vec_to_bump::<DictionaryEntryId, S>(src.data(), bump), src.dictionary_id())
}

pub fn column_data_to_cow<S: Storage>(src: &ColumnData<S>) -> ColumnData<Cow> {
	match src {
		ColumnData::Bool(c) => ColumnData::Bool(bool_to_cow(c)),
		ColumnData::Float4(c) => ColumnData::Float4(number_to_cow(c)),
		ColumnData::Float8(c) => ColumnData::Float8(number_to_cow(c)),
		ColumnData::Int1(c) => ColumnData::Int1(number_to_cow(c)),
		ColumnData::Int2(c) => ColumnData::Int2(number_to_cow(c)),
		ColumnData::Int4(c) => ColumnData::Int4(number_to_cow(c)),
		ColumnData::Int8(c) => ColumnData::Int8(number_to_cow(c)),
		ColumnData::Int16(c) => ColumnData::Int16(number_to_cow(c)),
		ColumnData::Uint1(c) => ColumnData::Uint1(number_to_cow(c)),
		ColumnData::Uint2(c) => ColumnData::Uint2(number_to_cow(c)),
		ColumnData::Uint4(c) => ColumnData::Uint4(number_to_cow(c)),
		ColumnData::Uint8(c) => ColumnData::Uint8(number_to_cow(c)),
		ColumnData::Uint16(c) => ColumnData::Uint16(number_to_cow(c)),
		ColumnData::Utf8 {
			container,
			max_bytes,
		} => ColumnData::Utf8 {
			container: utf8_to_cow(container),
			max_bytes: *max_bytes,
		},
		ColumnData::Date(c) => ColumnData::Date(temporal_to_cow(c)),
		ColumnData::DateTime(c) => ColumnData::DateTime(temporal_to_cow(c)),
		ColumnData::Time(c) => ColumnData::Time(temporal_to_cow(c)),
		ColumnData::Duration(c) => ColumnData::Duration(temporal_to_cow(c)),
		ColumnData::IdentityId(c) => ColumnData::IdentityId(identity_id_to_cow(c)),
		ColumnData::Uuid4(c) => ColumnData::Uuid4(uuid_to_cow(c)),
		ColumnData::Uuid7(c) => ColumnData::Uuid7(uuid_to_cow(c)),
		ColumnData::Blob {
			container,
			max_bytes,
		} => ColumnData::Blob {
			container: blob_to_cow(container),
			max_bytes: *max_bytes,
		},
		ColumnData::Int {
			container,
			max_bytes,
		} => ColumnData::Int {
			container: number_to_cow(container),
			max_bytes: *max_bytes,
		},
		ColumnData::Uint {
			container,
			max_bytes,
		} => ColumnData::Uint {
			container: number_to_cow(container),
			max_bytes: *max_bytes,
		},
		ColumnData::Decimal {
			container,
			precision,
			scale,
		} => ColumnData::Decimal {
			container: number_to_cow(container),
			precision: *precision,
			scale: *scale,
		},
		ColumnData::Any(c) => ColumnData::Any(any_to_cow(c)),
		ColumnData::DictionaryId(c) => ColumnData::DictionaryId(dictionary_to_cow(c)),
		ColumnData::Option {
			inner,
			bitvec,
		} => ColumnData::Option {
			inner: Box::new(column_data_to_cow(inner)),
			bitvec: bitvec_to_cow::<S>(bitvec),
		},
	}
}

pub fn column_data_to_bump<'bump, S: Storage>(src: &ColumnData<S>, bump: &'bump BumpAlloc) -> ColumnData<Bump<'bump>> {
	match src {
		ColumnData::Bool(c) => ColumnData::Bool(bool_to_bump(c, bump)),
		ColumnData::Float4(c) => ColumnData::Float4(number_to_bump(c, bump)),
		ColumnData::Float8(c) => ColumnData::Float8(number_to_bump(c, bump)),
		ColumnData::Int1(c) => ColumnData::Int1(number_to_bump(c, bump)),
		ColumnData::Int2(c) => ColumnData::Int2(number_to_bump(c, bump)),
		ColumnData::Int4(c) => ColumnData::Int4(number_to_bump(c, bump)),
		ColumnData::Int8(c) => ColumnData::Int8(number_to_bump(c, bump)),
		ColumnData::Int16(c) => ColumnData::Int16(number_to_bump(c, bump)),
		ColumnData::Uint1(c) => ColumnData::Uint1(number_to_bump(c, bump)),
		ColumnData::Uint2(c) => ColumnData::Uint2(number_to_bump(c, bump)),
		ColumnData::Uint4(c) => ColumnData::Uint4(number_to_bump(c, bump)),
		ColumnData::Uint8(c) => ColumnData::Uint8(number_to_bump(c, bump)),
		ColumnData::Uint16(c) => ColumnData::Uint16(number_to_bump(c, bump)),
		ColumnData::Utf8 {
			container,
			max_bytes,
		} => ColumnData::Utf8 {
			container: utf8_to_bump(container, bump),
			max_bytes: *max_bytes,
		},
		ColumnData::Date(c) => ColumnData::Date(temporal_to_bump(c, bump)),
		ColumnData::DateTime(c) => ColumnData::DateTime(temporal_to_bump(c, bump)),
		ColumnData::Time(c) => ColumnData::Time(temporal_to_bump(c, bump)),
		ColumnData::Duration(c) => ColumnData::Duration(temporal_to_bump(c, bump)),
		ColumnData::IdentityId(c) => ColumnData::IdentityId(identity_id_to_bump(c, bump)),
		ColumnData::Uuid4(c) => ColumnData::Uuid4(uuid_to_bump(c, bump)),
		ColumnData::Uuid7(c) => ColumnData::Uuid7(uuid_to_bump(c, bump)),
		ColumnData::Blob {
			container,
			max_bytes,
		} => ColumnData::Blob {
			container: blob_to_bump(container, bump),
			max_bytes: *max_bytes,
		},
		ColumnData::Int {
			container,
			max_bytes,
		} => ColumnData::Int {
			container: number_to_bump(container, bump),
			max_bytes: *max_bytes,
		},
		ColumnData::Uint {
			container,
			max_bytes,
		} => ColumnData::Uint {
			container: number_to_bump(container, bump),
			max_bytes: *max_bytes,
		},
		ColumnData::Decimal {
			container,
			precision,
			scale,
		} => ColumnData::Decimal {
			container: number_to_bump(container, bump),
			precision: *precision,
			scale: *scale,
		},
		ColumnData::Any(c) => ColumnData::Any(any_to_bump(c, bump)),
		ColumnData::DictionaryId(c) => ColumnData::DictionaryId(dictionary_to_bump(c, bump)),
		ColumnData::Option {
			inner,
			bitvec,
		} => ColumnData::Option {
			inner: Box::new(column_data_to_bump(inner, bump)),
			bitvec: bitvec_to_bump::<S>(bitvec, bump),
		},
	}
}

pub fn column_to_cow<S: Storage>(src: &Column<S>) -> Column<Cow> {
	Column::new(src.name().clone(), column_data_to_cow(src.data()))
}

pub fn column_to_bump<'bump, S: Storage>(src: &Column<S>, bump: &'bump BumpAlloc) -> Column<Bump<'bump>> {
	Column::new(src.name().clone(), column_data_to_bump(src.data(), bump))
}

#[cfg(test)]
mod tests {
	use reifydb_core::value::column::Column;
	use reifydb_type::value::r#type::Type;

	use super::*;

	#[test]
	fn test_column_data_cow_roundtrip() {
		let original = ColumnData::int4(vec![10, 20, 30]);
		let bump_alloc = BumpAlloc::new();

		// Cow -> Bump
		let bump_data = column_data_to_bump::<Cow>(&original, &bump_alloc);
		assert_eq!(bump_data.len(), 3);

		// Bump -> Cow
		let cow_data = column_data_to_cow::<Bump>(&bump_data);
		assert_eq!(cow_data, original);
	}

	#[test]
	fn test_column_data_bool_roundtrip() {
		let original = ColumnData::bool(vec![true, false, true]);
		let bump_alloc = BumpAlloc::new();

		let bump_data = column_data_to_bump::<Cow>(&original, &bump_alloc);
		let cow_data = column_data_to_cow::<Bump>(&bump_data);
		assert_eq!(cow_data, original);
	}

	#[test]
	fn test_column_data_utf8_roundtrip() {
		let original = ColumnData::utf8(vec![String::from("hello"), String::from("world")]);
		let bump_alloc = BumpAlloc::new();

		let bump_data = column_data_to_bump::<Cow>(&original, &bump_alloc);
		let cow_data = column_data_to_cow::<Bump>(&bump_data);
		assert_eq!(cow_data, original);
	}

	#[test]
	fn test_column_data_float8_roundtrip() {
		let original = ColumnData::float8(vec![1.5, 2.7, 3.9]);
		let bump_alloc = BumpAlloc::new();

		let bump_data = column_data_to_bump::<Cow>(&original, &bump_alloc);
		let cow_data = column_data_to_cow::<Bump>(&bump_data);
		assert_eq!(cow_data, original);
	}

	#[test]
	fn test_column_data_none_roundtrip() {
		let original = ColumnData::none_typed(Type::Boolean, 5);
		let bump_alloc = BumpAlloc::new();

		let bump_data = column_data_to_bump::<Cow>(&original, &bump_alloc);
		let cow_data = column_data_to_cow::<Bump>(&bump_data);
		assert_eq!(cow_data, original);
	}

	#[test]
	fn test_column_roundtrip() {
		let original = Column::int4("age", vec![25, 30, 35]);
		let bump_alloc = BumpAlloc::new();

		let bump_col = column_to_bump::<Cow>(&original, &bump_alloc);
		let cow_col = column_to_cow::<Bump>(&bump_col);
		assert_eq!(cow_col, original);
	}
}
