// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::{Date, DateTime, Decimal, Int, Interval, Time, Uint, Value};

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

impl AsSlice<f32> for ColumnData {
	fn as_slice(&self) -> &[f32] {
		match self {
			ColumnData::Float4(container) => container.data().as_slice(),
			other => {
				panic!("called `as_slice::<f32>()` on ColumnData::{:?}", other.get_type())
			}
		}
	}
}

impl AsSlice<f64> for ColumnData {
	fn as_slice(&self) -> &[f64] {
		match self {
			ColumnData::Float8(container) => container.data().as_slice(),
			other => {
				panic!("called `as_slice::<f64>()` on ColumnData::{:?}", other.get_type())
			}
		}
	}
}

impl AsSlice<i8> for ColumnData {
	fn as_slice(&self) -> &[i8] {
		match self {
			ColumnData::Int1(container) => container.data().as_slice(),
			other => {
				panic!("called `as_slice::<i8>()` on ColumnData::{:?}", other.get_type())
			}
		}
	}
}

impl AsSlice<i16> for ColumnData {
	fn as_slice(&self) -> &[i16] {
		match self {
			ColumnData::Int2(container) => container.data().as_slice(),
			other => {
				panic!("called `as_slice::<i16>()` on ColumnData::{:?}", other.get_type())
			}
		}
	}
}

impl AsSlice<i32> for ColumnData {
	fn as_slice(&self) -> &[i32] {
		match self {
			ColumnData::Int4(container) => container.data().as_slice(),
			other => {
				panic!("called `as_slice::<i32>()` on ColumnData::{:?}", other.get_type())
			}
		}
	}
}

impl AsSlice<i64> for ColumnData {
	fn as_slice(&self) -> &[i64] {
		match self {
			ColumnData::Int8(container) => container.data().as_slice(),
			other => {
				panic!("called `as_slice::<i64>()` on ColumnData::{:?}", other.get_type())
			}
		}
	}
}

impl AsSlice<i128> for ColumnData {
	fn as_slice(&self) -> &[i128] {
		match self {
			ColumnData::Int16(container) => container.data().as_slice(),
			other => {
				panic!("called `as_slice::<i128>()` on ColumnData::{:?}", other.get_type())
			}
		}
	}
}

impl AsSlice<u8> for ColumnData {
	fn as_slice(&self) -> &[u8] {
		match self {
			ColumnData::Uint1(container) => container.data().as_slice(),
			other => {
				panic!("called `as_slice::<u8>()` on ColumnData::{:?}", other.get_type())
			}
		}
	}
}

impl AsSlice<u16> for ColumnData {
	fn as_slice(&self) -> &[u16] {
		match self {
			ColumnData::Uint2(container) => container.data().as_slice(),
			other => {
				panic!("called `as_slice::<u16>()` on ColumnData::{:?}", other.get_type())
			}
		}
	}
}

impl AsSlice<u32> for ColumnData {
	fn as_slice(&self) -> &[u32] {
		match self {
			ColumnData::Uint4(container) => container.data().as_slice(),
			other => {
				panic!("called `as_slice::<u32>()` on ColumnData::{:?}", other.get_type())
			}
		}
	}
}

impl AsSlice<u64> for ColumnData {
	fn as_slice(&self) -> &[u64] {
		match self {
			ColumnData::Uint8(container) => container.data().as_slice(),
			other => {
				panic!("called `as_slice::<u64>()` on ColumnData::{:?}", other.get_type())
			}
		}
	}
}

impl AsSlice<u128> for ColumnData {
	fn as_slice(&self) -> &[u128] {
		match self {
			ColumnData::Uint16(container) => container.data().as_slice(),
			other => {
				panic!("called `as_slice::<u128>()` on ColumnData::{:?}", other.get_type())
			}
		}
	}
}

impl AsSlice<String> for ColumnData {
	fn as_slice(&self) -> &[String] {
		match self {
			ColumnData::Utf8 {
				container,
				..
			} => container.data().as_slice(),
			other => {
				panic!("called `as_slice::<String>()` on ColumnData::{:?}", other.get_type())
			}
		}
	}
}

impl AsSlice<Date> for ColumnData {
	fn as_slice(&self) -> &[Date] {
		match self {
			ColumnData::Date(container) => container.data().as_slice(),
			other => {
				panic!("called `as_slice::<Date>()` on ColumnData::{:?}", other.get_type())
			}
		}
	}
}

impl AsSlice<DateTime> for ColumnData {
	fn as_slice(&self) -> &[DateTime] {
		match self {
			ColumnData::DateTime(container) => container.data().as_slice(),
			other => {
				panic!("called `as_slice::<DateTime>()` on ColumnData::{:?}", other.get_type())
			}
		}
	}
}

impl AsSlice<Time> for ColumnData {
	fn as_slice(&self) -> &[Time] {
		match self {
			ColumnData::Time(container) => container.data().as_slice(),
			other => {
				panic!("called `as_slice::<Time>()` on ColumnData::{:?}", other.get_type())
			}
		}
	}
}

impl AsSlice<Interval> for ColumnData {
	fn as_slice(&self) -> &[Interval] {
		match self {
			ColumnData::Interval(container) => container.data().as_slice(),
			other => {
				panic!("called `as_slice::<Interval>()` on ColumnData::{:?}", other.get_type())
			}
		}
	}
}

impl AsSlice<Int> for ColumnData {
	fn as_slice(&self) -> &[Int] {
		match self {
			ColumnData::Int {
				container,
				..
			} => container.data().as_slice(),
			other => {
				panic!("called `as_slice::<Int>()` on ColumnData::{:?}", other.get_type())
			}
		}
	}
}

impl AsSlice<Uint> for ColumnData {
	fn as_slice(&self) -> &[Uint] {
		match self {
			ColumnData::Uint {
				container,
				..
			} => container.data().as_slice(),
			other => {
				panic!("called `as_slice::<Uint>()` on ColumnData::{:?}", other.get_type())
			}
		}
	}
}

impl AsSlice<Decimal> for ColumnData {
	fn as_slice(&self) -> &[Decimal] {
		match self {
			ColumnData::Decimal {
				container,
				..
			} => container.data().as_slice(),
			other => {
				panic!("called `as_slice::<Decimal>()` on ColumnData::{:?}", other.get_type())
			}
		}
	}
}

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
