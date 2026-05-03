// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Typed `Push<T>` trait and its blanket implementations for every primitive `ColumnBuffer` accepts.
//!
//! Each submodule (`int`, `uint`, `decimal`, `none`, `uuid`, `value`) implements `Push<T>` for the variants of
//! `ColumnBuffer` that store `T`. Pushing a value of the wrong type panics rather than silently coercing; correctness
//! at the column boundary is enforced by the type system at the call site, so a panic here is a planner bug, not a user
//! error.

use std::fmt::Debug;

use reifydb_type::{
	storage::DataBitVec,
	value::{
		blob::Blob, date::Date, datetime::DateTime, dictionary::DictionaryEntryId, duration::Duration,
		number::safe::convert::SafeConvert, time::Time,
	},
};

use crate::value::column::ColumnBuffer;

pub mod decimal;
pub mod int;
pub mod none;
pub mod uint;
pub mod uuid;
pub mod value;

pub trait Push<T> {
	fn push(&mut self, value: T);
}

impl ColumnBuffer {
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
		impl Push<$t> for ColumnBuffer {
			fn push(&mut self, value: $t) {
				match self {
					ColumnBuffer::$variant(container) => {
						container.push(value);
					}
					ColumnBuffer::Option {
						inner,
						bitvec,
					} => {
						inner.push(value);
						DataBitVec::push(bitvec, true);
					}
					other => panic!(
						"called `push::<{}>()` on ColumnBuffer::{:?}",
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
		impl Push<$from> for ColumnBuffer {
			fn push(&mut self, value: $from) {
				match self {
					$(
						ColumnBuffer::$variant(container) => match <$from as SafeConvert<$target>>::checked_convert(value) {
							Some(v) => container.push(v),
							None => container.push_default(),
						},
					)*
					ColumnBuffer::$native_variant(container) => {
						container.push(value);
					}
					ColumnBuffer::Option { inner, bitvec } => {
						inner.push(value);
						DataBitVec::push(bitvec, true);
					}
					other => {
						panic!(
							"called `push::<{}>()` on incompatible ColumnBuffer::{:?}",
							stringify!($from),
							other.get_type()
						);
					}
				}
			}
		}
	};
}

impl Push<bool> for ColumnBuffer {
	fn push(&mut self, value: bool) {
		match self {
			ColumnBuffer::Bool(container) => {
				container.push(value);
			}
			ColumnBuffer::Option {
				inner,
				bitvec,
			} => {
				inner.push(value);
				DataBitVec::push(bitvec, true);
			}
			other => panic!("called `push::<bool>()` on ColumnBuffer::{:?}", other.get_type()),
		}
	}
}

impl_push!(f32, Float4, float4);
impl_push!(f64, Float8, float8);
impl_push!(Date, Date, date);
impl_push!(DateTime, DateTime, datetime);
impl_push!(Time, Time, time);
impl_push!(Duration, Duration, duration);

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

impl Push<Blob> for ColumnBuffer {
	fn push(&mut self, value: Blob) {
		match self {
			ColumnBuffer::Blob {
				container,
				..
			} => {
				container.push(value);
			}
			ColumnBuffer::Option {
				inner,
				bitvec,
			} => {
				inner.push(value);
				DataBitVec::push(bitvec, true);
			}
			other => panic!("called `push::<Blob>()` on ColumnBuffer::{:?}", other.get_type()),
		}
	}
}

impl Push<String> for ColumnBuffer {
	fn push(&mut self, value: String) {
		match self {
			ColumnBuffer::Utf8 {
				container,
				..
			} => {
				container.push(value);
			}
			ColumnBuffer::Option {
				inner,
				bitvec,
			} => {
				inner.push(value);
				DataBitVec::push(bitvec, true);
			}
			other => {
				panic!("called `push::<String>()` on ColumnBuffer::{:?}", other.get_type())
			}
		}
	}
}

impl Push<DictionaryEntryId> for ColumnBuffer {
	fn push(&mut self, value: DictionaryEntryId) {
		match self {
			ColumnBuffer::DictionaryId(container) => {
				container.push(value);
			}
			ColumnBuffer::Option {
				inner,
				bitvec,
			} => {
				inner.push(value);
				DataBitVec::push(bitvec, true);
			}
			other => panic!("called `push::<DictionaryEntryId>()` on ColumnBuffer::{:?}", other.get_type()),
		}
	}
}

impl Push<&str> for ColumnBuffer {
	fn push(&mut self, value: &str) {
		self.push(value.to_string());
	}
}
