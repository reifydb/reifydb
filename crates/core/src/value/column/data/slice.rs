// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{
	Value, date::Date, datetime::DateTime, decimal::Decimal, duration::Duration, int::Int, time::Time, uint::Uint,
};

use crate::value::column::ColumnData;

pub trait AsSlice<T> {
	fn as_slice(&self) -> &[T];
}

impl ColumnData {
	pub fn as_slice<T>(&self) -> &[T]
	where
		Self: AsSlice<T>,
	{
		<Self as AsSlice<T>>::as_slice(self)
	}
}

impl AsSlice<bool> for ColumnData {
	fn as_slice(&self) -> &[bool] {
		match self {
			ColumnData::Bool(_) => {
				panic!("as_slice() is not supported for BitVec. Use to_vec() instead.")
			}
			other => {
				panic!("called `as_slice::<bool>()` on ColumnData::{:?}", other.get_type())
			}
		}
	}
}

macro_rules! impl_as_slice {
	($t:ty, $variant:ident) => {
		impl AsSlice<$t> for ColumnData {
			fn as_slice(&self) -> &[$t] {
				match self {
					ColumnData::$variant(container) => container.data().as_slice(),
					other => {
						panic!(
							"called `as_slice::<{}>()` on ColumnData::{:?}",
							stringify!($t),
							other.get_type()
						)
					}
				}
			}
		}
	};
	($t:ty, $variant:ident { container }) => {
		impl AsSlice<$t> for ColumnData {
			fn as_slice(&self) -> &[$t] {
				match self {
					ColumnData::$variant {
						container,
						..
					} => container.data().as_slice(),
					other => {
						panic!(
							"called `as_slice::<{}>()` on ColumnData::{:?}",
							stringify!($t),
							other.get_type()
						)
					}
				}
			}
		}
	};
}

impl_as_slice!(f32, Float4);
impl_as_slice!(f64, Float8);
impl_as_slice!(i8, Int1);
impl_as_slice!(i16, Int2);
impl_as_slice!(i32, Int4);
impl_as_slice!(i64, Int8);
impl_as_slice!(i128, Int16);
impl_as_slice!(u8, Uint1);
impl_as_slice!(u16, Uint2);
impl_as_slice!(u32, Uint4);
impl_as_slice!(u64, Uint8);
impl_as_slice!(u128, Uint16);
impl_as_slice!(
	String,
	Utf8 {
		container
	}
);
impl_as_slice!(Date, Date);
impl_as_slice!(DateTime, DateTime);
impl_as_slice!(Time, Time);
impl_as_slice!(Duration, Duration);
impl_as_slice!(
	Int,
	Int {
		container
	}
);
impl_as_slice!(
	Uint,
	Uint {
		container
	}
);
impl_as_slice!(
	Decimal,
	Decimal {
		container
	}
);

impl AsSlice<Box<Value>> for ColumnData {
	fn as_slice(&self) -> &[Box<Value>] {
		match self {
			ColumnData::Any(container) => container.data().as_slice(),
			other => {
				panic!("called `as_slice::<Box<Value>>()` on ColumnData::{:?}", other.get_type())
			}
		}
	}
}
