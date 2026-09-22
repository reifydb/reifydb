// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	blob::Blob,
	container::{
		decimal_array::uint16_to_native,
		dictionary_array::push_entry,
		temporal_array::{date_to_native, datetime_to_native, duration_to_native, time_to_native},
	},
	date::Date,
	datetime::DateTime,
	dictionary::DictionaryEntryId,
	duration::Duration,
	number::safe::convert::SafeConvert,
	time::Time,
};

use crate::value::column::builder::ColumnBuilder;

pub mod decimal;
pub mod int;
pub mod none;
pub mod typed;
pub mod uint;
pub mod uuid;
pub mod value;

pub trait Push<T> {
	fn push(&mut self, value: T);
}

macro_rules! impl_native_push {
	($t:ty, $variant:ident) => {
		impl Push<$t> for ColumnBuilder {
			fn push(&mut self, value: $t) {
				match self {
					ColumnBuilder::$variant(builder) => {
						builder.append_value(value);
					}
					ColumnBuilder::Option {
						inner,
						bitvec,
					} => {
						inner.push(value);
						bitvec.append(true);
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

macro_rules! impl_temporal_push {
	($t:ty, $variant:ident, $to_native:ident) => {
		impl Push<$t> for ColumnBuilder {
			fn push(&mut self, value: $t) {
				match self {
					ColumnBuilder::$variant(builder) => {
						builder.append_value($to_native(value));
					}
					ColumnBuilder::Option {
						inner,
						bitvec,
					} => {
						inner.push(value);
						bitvec.append(true);
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
	(native $from:ty,
		$own:ident,
		native [$(($variant:ident, $target:ty)),* $(,)?],
		wide [$(($wide_variant:ident, $wide_target:ty, $wide_to_native:path)),* $(,)?]
	) => {
		impl Push<$from> for ColumnBuilder {
			fn push(&mut self, value: $from) {
				match self {
					$(
						ColumnBuilder::$variant(builder) => match <$from as SafeConvert<$target>>::checked_convert(value) {
							Some(v) => builder.append_value(v),
							None => builder.append_value(<$target>::default()),
						},
					)*
					$(
						ColumnBuilder::$wide_variant(builder) => match <$from as SafeConvert<$wide_target>>::checked_convert(value) {
							Some(v) => builder.append_value($wide_to_native(v)),
							None => builder.append_value(Default::default()),
						},
					)*
					ColumnBuilder::$own(builder) => {
						builder.append_value(value);
					}
					ColumnBuilder::Option { inner, bitvec } => {
						inner.push(value);
						bitvec.append(true);
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
	(wide $from:ty,
		$own:ident via $own_to_native:path,
		native [$(($variant:ident, $target:ty)),* $(,)?],
		wide [$(($wide_variant:ident, $wide_target:ty, $wide_to_native:path)),* $(,)?]
	) => {
		impl Push<$from> for ColumnBuilder {
			fn push(&mut self, value: $from) {
				match self {
					$(
						ColumnBuilder::$variant(builder) => match <$from as SafeConvert<$target>>::checked_convert(value) {
							Some(v) => builder.append_value(v),
							None => builder.append_value(<$target>::default()),
						},
					)*
					$(
						ColumnBuilder::$wide_variant(builder) => match <$from as SafeConvert<$wide_target>>::checked_convert(value) {
							Some(v) => builder.append_value($wide_to_native(v)),
							None => builder.append_value(Default::default()),
						},
					)*
					ColumnBuilder::$own(builder) => {
						builder.append_value($own_to_native(value));
					}
					ColumnBuilder::Option { inner, bitvec } => {
						inner.push(value);
						bitvec.append(true);
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

impl Push<bool> for ColumnBuilder {
	fn push(&mut self, value: bool) {
		match self {
			ColumnBuilder::Bool(builder) => builder.append(value),
			ColumnBuilder::Option {
				inner,
				bitvec,
			} => {
				inner.push(value);
				bitvec.append(true);
			}
			other => panic!("called `push::<bool>()` on ColumnBuffer::{:?}", other.get_type()),
		}
	}
}

impl_native_push!(f32, Float4);
impl_native_push!(f64, Float8);
impl_temporal_push!(Date, Date, date_to_native);
impl_temporal_push!(DateTime, DateTime, datetime_to_native);
impl_temporal_push!(Time, Time, time_to_native);
impl_temporal_push!(Duration, Duration, duration_to_native);

impl_numeric_push!(
	native i8,
	Int1,
	native [(Float4, f32), (Float8, f64), (Int2, i16), (Int4, i32), (Int8, i64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Int16, i128)],
	wide [(Uint16, u128, uint16_to_native)]
);

impl_numeric_push!(
	native i16,
	Int2,
	native [(Float4, f32), (Float8, f64), (Int1, i8), (Int4, i32), (Int8, i64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Int16, i128)],
	wide [(Uint16, u128, uint16_to_native)]
);

impl_numeric_push!(
	native i32,
	Int4,
	native [(Float4, f32), (Float8, f64), (Int1, i8), (Int2, i16), (Int8, i64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Int16, i128)],
	wide [(Uint16, u128, uint16_to_native)]
);

impl_numeric_push!(
	native i64,
	Int8,
	native [(Float4, f32), (Float8, f64), (Int1, i8), (Int2, i16), (Int4, i32), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Int16, i128)],
	wide [(Uint16, u128, uint16_to_native)]
);

impl_numeric_push!(
	wide i128,
	Int16 via i128::from,
	native [(Float4, f32), (Float8, f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64)],
	wide [(Uint16, u128, uint16_to_native)]
);

impl_numeric_push!(
	native u8,
	Uint1,
	native [(Float4, f32), (Float8, f64), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)],
	wide [(Uint16, u128, uint16_to_native)]
);

impl_numeric_push!(
	native u16,
	Uint2,
	native [(Float4, f32), (Float8, f64), (Uint1, u8), (Uint4, u32), (Uint8, u64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)],
	wide [(Uint16, u128, uint16_to_native)]
);

impl_numeric_push!(
	native u32,
	Uint4,
	native [(Float4, f32), (Float8, f64), (Uint1, u8), (Uint2, u16), (Uint8, u64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)],
	wide [(Uint16, u128, uint16_to_native)]
);

impl_numeric_push!(
	native u64,
	Uint8,
	native [(Float4, f32), (Float8, f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)],
	wide [(Uint16, u128, uint16_to_native)]
);

impl_numeric_push!(
	wide u128,
	Uint16 via uint16_to_native,
	native [(Float4, f32), (Float8, f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)],
	wide []
);

impl Push<Blob> for ColumnBuilder {
	fn push(&mut self, value: Blob) {
		match self {
			ColumnBuilder::Blob {
				builder,
				..
			} => {
				builder.append_value(value.as_bytes());
			}
			ColumnBuilder::Option {
				inner,
				bitvec,
			} => {
				inner.push(value);
				bitvec.append(true);
			}
			other => panic!("called `push::<Blob>()` on ColumnBuffer::{:?}", other.get_type()),
		}
	}
}

impl Push<String> for ColumnBuilder {
	fn push(&mut self, value: String) {
		match self {
			ColumnBuilder::Utf8 {
				builder,
				..
			} => {
				builder.append_value(value);
			}
			ColumnBuilder::Option {
				inner,
				bitvec,
			} => {
				inner.push(value);
				bitvec.append(true);
			}
			other => {
				panic!("called `push::<String>()` on ColumnBuffer::{:?}", other.get_type())
			}
		}
	}
}

impl Push<DictionaryEntryId> for ColumnBuilder {
	fn push(&mut self, value: DictionaryEntryId) {
		match self {
			ColumnBuilder::DictionaryId {
				buffer,
				..
			} => push_entry(buffer, value),
			ColumnBuilder::Option {
				inner,
				bitvec,
			} => {
				inner.push(value);
				bitvec.append(true);
			}
			other => panic!("called `push::<DictionaryEntryId>()` on ColumnBuffer::{:?}", other.get_type()),
		}
	}
}

impl Push<&str> for ColumnBuilder {
	fn push(&mut self, value: &str) {
		self.push(value.to_string());
	}
}
