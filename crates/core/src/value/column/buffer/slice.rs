// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	container::temporal_array::{dates, datetimes, durations, times},
	date::Date,
	datetime::DateTime,
	duration::Duration,
	time::Time,
};

use crate::value::column::ColumnBuffer;

pub trait AsSlice<T> {
	fn as_slice(&self) -> &[T];
}

impl ColumnBuffer {
	pub fn as_slice<T>(&self) -> &[T]
	where
		Self: AsSlice<T>,
	{
		<Self as AsSlice<T>>::as_slice(self)
	}
}

impl AsSlice<bool> for ColumnBuffer {
	fn as_slice(&self) -> &[bool] {
		match self {
			ColumnBuffer::Bool(_) => {
				panic!("as_slice() is not supported for BooleanArray. Use to_vec() instead.")
			}
			other => {
				panic!("called `as_slice::<bool>()` on ColumnBuffer::{:?}", other.get_type())
			}
		}
	}
}

macro_rules! impl_as_slice {
	($t:ty, $variant:ident native) => {
		impl AsSlice<$t> for ColumnBuffer {
			fn as_slice(&self) -> &[$t] {
				match self {
					ColumnBuffer::$variant(array) => array.values(),
					ColumnBuffer::Option {
						inner,
						..
					} => inner.as_slice(),
					other => {
						panic!(
							"called `as_slice::<{}>()` on ColumnBuffer::{:?}",
							stringify!($t),
							other.get_type()
						)
					}
				}
			}
		}
	};
	($t:ty, $variant:ident typed $typed:ident) => {
		impl AsSlice<$t> for ColumnBuffer {
			fn as_slice(&self) -> &[$t] {
				match self {
					ColumnBuffer::$variant(array) => $typed(array),
					ColumnBuffer::Option {
						inner,
						..
					} => inner.as_slice(),
					other => {
						panic!(
							"called `as_slice::<{}>()` on ColumnBuffer::{:?}",
							stringify!($t),
							other.get_type()
						)
					}
				}
			}
		}
	};
}

impl_as_slice!(f32, Float4 native);
impl_as_slice!(f64, Float8 native);
impl_as_slice!(i8, Int1 native);
impl_as_slice!(i16, Int2 native);
impl_as_slice!(i32, Int4 native);
impl_as_slice!(i64, Int8 native);
impl_as_slice!(i128, Int16 native);
impl_as_slice!(u8, Uint1 native);
impl_as_slice!(u16, Uint2 native);
impl_as_slice!(u32, Uint4 native);
impl_as_slice!(u64, Uint8 native);

impl_as_slice!(Date, Date typed dates);
impl_as_slice!(DateTime, DateTime typed datetimes);
impl_as_slice!(Time, Time typed times);
impl_as_slice!(Duration, Duration typed durations);
