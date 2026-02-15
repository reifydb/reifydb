// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::fmt::Debug;

use reifydb_type::{
	storage::DataBitVec,
	value::{
		blob::Blob, date::Date, datetime::DateTime, dictionary::DictionaryEntryId, duration::Duration,
		number::safe::convert::SafeConvert, time::Time,
	},
};

use crate::value::column::ColumnData;

pub mod decimal;
pub mod int;
pub mod uint;
pub mod undefined;
pub mod uuid;
pub mod value;

pub trait Push<T> {
	fn push(&mut self, value: T);
}

impl ColumnData {
	pub fn push<T>(&mut self, value: T)
	where
		Self: Push<T>,
		T: Debug,
	{
		<Self as Push<T>>::push(self, value)
	}
}

macro_rules! impl_push {
	($t:ty, $variant:ident, $factory:ident) => {
		impl Push<$t> for ColumnData {
			fn push(&mut self, value: $t) {
				match self {
					ColumnData::$variant(container) => {
						container.push(value);
					}
					ColumnData::Option {
						inner,
						bitvec,
					} => {
						inner.push(value);
						DataBitVec::push(bitvec, true);
					}
					ColumnData::Undefined(container) => {
						let mut new_container =
							ColumnData::$factory(vec![<$t>::default(); container.len()]);
						if let ColumnData::$variant(new_container) = &mut new_container {
							new_container.push(value);
						}
						*self = new_container;
					}
					other => panic!(
						"called `push::<{}>()` on EngineColumnData::{:?}",
						stringify!($t),
						other.get_type()
					),
				}
			}
		}
	};
}

macro_rules! impl_numeric_push {
	($from:ty, $native_variant:ident, $factory:ident, $default:expr, [$(($variant:ident, $target:ty)),* $(,)?]) => {
		impl Push<$from> for ColumnData {
			fn push(&mut self, value: $from) {
				match self {
					$(
						ColumnData::$variant(container) => match <$from as SafeConvert<$target>>::checked_convert(value) {
							Some(v) => container.push(v),
							None => container.push_undefined(),
						},
					)*
					ColumnData::$native_variant(container) => {
						container.push(value);
					}
					ColumnData::Option { inner, bitvec } => {
						inner.push(value);
						DataBitVec::push(bitvec, true);
					}
					ColumnData::Undefined(container) => {
						let mut new_container = ColumnData::$factory(vec![$default; container.len()]);
						if let ColumnData::$native_variant(new_container) = &mut new_container {
							new_container.push(value);
						}
						*self = new_container;
					}
					other => {
						panic!(
							"called `push::<{}>()` on incompatible EngineColumnData::{:?}",
							stringify!($from),
							other.get_type()
						);
					}
				}
			}
		}
	};
}

impl Push<bool> for ColumnData {
	fn push(&mut self, value: bool) {
		match self {
			ColumnData::Bool(container) => {
				container.push(value);
			}
			ColumnData::Option {
				inner,
				bitvec,
			} => {
				inner.push(value);
				DataBitVec::push(bitvec, true);
			}
			ColumnData::Undefined(container) => {
				let mut new_container = ColumnData::bool(vec![false; container.len()]);
				if let ColumnData::Bool(new_container) = &mut new_container {
					new_container.push(value);
				}
				*self = new_container;
			}
			other => panic!("called `push::<bool>()` on EngineColumnData::{:?}", other.get_type()),
		}
	}
}

impl_push!(f32, Float4, float4);
impl_push!(f64, Float8, float8);
impl_push!(Date, Date, date);
impl_push!(DateTime, DateTime, datetime);
impl_push!(Time, Time, time);
impl_push!(Duration, Duration, duration);

// Signed integer push impls with cross-type SafeConvert
impl_numeric_push!(
	i8,
	Int1,
	int1,
	0i8,
	[
		(Float4, f32),
		(Float8, f64),
		(Int2, i16),
		(Int4, i32),
		(Int8, i64),
		(Int16, i128),
		(Uint1, u8),
		(Uint2, u16),
		(Uint4, u32),
		(Uint8, u64),
		(Uint16, u128),
	]
);

impl_numeric_push!(
	i16,
	Int2,
	int2,
	0i16,
	[
		(Float4, f32),
		(Float8, f64),
		(Int1, i8),
		(Int4, i32),
		(Int8, i64),
		(Int16, i128),
		(Uint1, u8),
		(Uint2, u16),
		(Uint4, u32),
		(Uint8, u64),
		(Uint16, u128),
	]
);

impl_numeric_push!(
	i32,
	Int4,
	int4,
	0i32,
	[
		(Float4, f32),
		(Float8, f64),
		(Int1, i8),
		(Int2, i16),
		(Int8, i64),
		(Int16, i128),
		(Uint1, u8),
		(Uint2, u16),
		(Uint4, u32),
		(Uint8, u64),
		(Uint16, u128),
	]
);

impl_numeric_push!(
	i64,
	Int8,
	int8,
	0i64,
	[
		(Float4, f32),
		(Float8, f64),
		(Int1, i8),
		(Int2, i16),
		(Int4, i32),
		(Int16, i128),
		(Uint1, u8),
		(Uint2, u16),
		(Uint4, u32),
		(Uint8, u64),
		(Uint16, u128),
	]
);

impl_numeric_push!(
	i128,
	Int16,
	int16,
	0i128,
	[
		(Float4, f32),
		(Float8, f64),
		(Int1, i8),
		(Int2, i16),
		(Int4, i32),
		(Int8, i64),
		(Uint1, u8),
		(Uint2, u16),
		(Uint4, u32),
		(Uint8, u64),
		(Uint16, u128),
	]
);

// Unsigned integer push impls with cross-type SafeConvert
impl_numeric_push!(
	u8,
	Uint1,
	uint1,
	0u8,
	[
		(Float4, f32),
		(Float8, f64),
		(Uint2, u16),
		(Uint4, u32),
		(Uint8, u64),
		(Uint16, u128),
		(Int1, i8),
		(Int2, i16),
		(Int4, i32),
		(Int8, i64),
		(Int16, i128),
	]
);

impl_numeric_push!(
	u16,
	Uint2,
	uint2,
	0u16,
	[
		(Float4, f32),
		(Float8, f64),
		(Uint1, u8),
		(Uint4, u32),
		(Uint8, u64),
		(Uint16, u128),
		(Int1, i8),
		(Int2, i16),
		(Int4, i32),
		(Int8, i64),
		(Int16, i128),
	]
);

impl_numeric_push!(
	u32,
	Uint4,
	uint4,
	0u32,
	[
		(Float4, f32),
		(Float8, f64),
		(Uint1, u8),
		(Uint2, u16),
		(Uint8, u64),
		(Uint16, u128),
		(Int1, i8),
		(Int2, i16),
		(Int4, i32),
		(Int8, i64),
		(Int16, i128),
	]
);

impl_numeric_push!(
	u64,
	Uint8,
	uint8,
	0u64,
	[
		(Float4, f32),
		(Float8, f64),
		(Uint1, u8),
		(Uint2, u16),
		(Uint4, u32),
		(Uint16, u128),
		(Int1, i8),
		(Int2, i16),
		(Int4, i32),
		(Int8, i64),
		(Int16, i128),
	]
);

impl_numeric_push!(
	u128,
	Uint16,
	uint16,
	0u128,
	[
		(Float4, f32),
		(Float8, f64),
		(Uint1, u8),
		(Uint2, u16),
		(Uint4, u32),
		(Uint8, u64),
		(Int1, i8),
		(Int2, i16),
		(Int4, i32),
		(Int8, i64),
		(Int16, i128),
	]
);

impl Push<Blob> for ColumnData {
	fn push(&mut self, value: Blob) {
		match self {
			ColumnData::Blob {
				container,
				..
			} => {
				container.push(value);
			}
			ColumnData::Option {
				inner,
				bitvec,
			} => {
				inner.push(value);
				DataBitVec::push(bitvec, true);
			}
			ColumnData::Undefined(container) => {
				let mut new_container = ColumnData::blob(vec![Blob::default(); container.len()]);
				if let ColumnData::Blob {
					container: new_container,
					..
				} = &mut new_container
				{
					new_container.push(value);
				}
				*self = new_container;
			}
			other => panic!("called `push::<Blob>()` on EngineColumnData::{:?}", other.get_type()),
		}
	}
}

impl Push<String> for ColumnData {
	fn push(&mut self, value: String) {
		match self {
			ColumnData::Utf8 {
				container,
				..
			} => {
				container.push(value);
			}
			ColumnData::Option {
				inner,
				bitvec,
			} => {
				inner.push(value);
				DataBitVec::push(bitvec, true);
			}
			ColumnData::Undefined(container) => {
				let mut new_container = ColumnData::utf8(vec![String::default(); container.len()]);
				if let ColumnData::Utf8 {
					container: new_container,
					..
				} = &mut new_container
				{
					new_container.push(value);
				}
				*self = new_container;
			}
			other => {
				panic!("called `push::<String>()` on EngineColumnData::{:?}", other.get_type())
			}
		}
	}
}

impl Push<DictionaryEntryId> for ColumnData {
	fn push(&mut self, value: DictionaryEntryId) {
		match self {
			ColumnData::DictionaryId(container) => {
				container.push(value);
			}
			ColumnData::Option {
				inner,
				bitvec,
			} => {
				inner.push(value);
				DataBitVec::push(bitvec, true);
			}
			ColumnData::Undefined(container) => {
				let mut new_container =
					ColumnData::dictionary_id(vec![DictionaryEntryId::default(); container.len()]);
				if let ColumnData::DictionaryId(new_container) = &mut new_container {
					new_container.push(value);
				}
				*self = new_container;
			}
			other => panic!(
				"called `push::<DictionaryEntryId>()` on EngineColumnData::{:?}",
				other.get_type()
			),
		}
	}
}

impl Push<&str> for ColumnData {
	fn push(&mut self, value: &str) {
		self.push(value.to_string());
	}
}
