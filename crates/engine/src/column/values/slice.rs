// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::ColumnValues;
use reifydb_core::{Date, DateTime, Interval, Time};

pub trait AsSlice<T> {
    fn as_slice(&self) -> &[T];
}

impl ColumnValues {
    pub fn as_slice<T>(&self) -> &[T]
    where
        Self: AsSlice<T>,
    {
        <Self as AsSlice<T>>::as_slice(self)
    }
}

impl AsSlice<bool> for ColumnValues {
    fn as_slice(&self) -> &[bool] {
        match self {
            ColumnValues::Bool(_) => {
                panic!("as_slice() is not supported for BitVec. Use to_vec() instead.")
            }
            other => panic!("called `as_slice::<bool>()` on ColumnValues::{:?}", other.get_type()),
        }
    }
}

impl AsSlice<f32> for ColumnValues {
    fn as_slice(&self) -> &[f32] {
        match self {
            ColumnValues::Float4(container) => container.values().as_slice(),
            other => panic!("called `as_slice::<f32>()` on ColumnValues::{:?}", other.get_type()),
        }
    }
}

impl AsSlice<f64> for ColumnValues {
    fn as_slice(&self) -> &[f64] {
        match self {
            ColumnValues::Float8(container) => container.values().as_slice(),
            other => panic!("called `as_slice::<f64>()` on ColumnValues::{:?}", other.get_type()),
        }
    }
}

impl AsSlice<i8> for ColumnValues {
    fn as_slice(&self) -> &[i8] {
        match self {
            ColumnValues::Int1(container) => container.values().as_slice(),
            other => panic!("called `as_slice::<i8>()` on ColumnValues::{:?}", other.get_type()),
        }
    }
}

impl AsSlice<i16> for ColumnValues {
    fn as_slice(&self) -> &[i16] {
        match self {
            ColumnValues::Int2(container) => container.values().as_slice(),
            other => panic!("called `as_slice::<i16>()` on ColumnValues::{:?}", other.get_type()),
        }
    }
}

impl AsSlice<i32> for ColumnValues {
    fn as_slice(&self) -> &[i32] {
        match self {
            ColumnValues::Int4(container) => container.values().as_slice(),
            other => panic!("called `as_slice::<i32>()` on ColumnValues::{:?}", other.get_type()),
        }
    }
}

impl AsSlice<i64> for ColumnValues {
    fn as_slice(&self) -> &[i64] {
        match self {
            ColumnValues::Int8(container) => container.values().as_slice(),
            other => panic!("called `as_slice::<i64>()` on ColumnValues::{:?}", other.get_type()),
        }
    }
}

impl AsSlice<i128> for ColumnValues {
    fn as_slice(&self) -> &[i128] {
        match self {
            ColumnValues::Int16(container) => container.values().as_slice(),
            other => panic!("called `as_slice::<i128>()` on ColumnValues::{:?}", other.get_type()),
        }
    }
}

impl AsSlice<u8> for ColumnValues {
    fn as_slice(&self) -> &[u8] {
        match self {
            ColumnValues::Uint1(container) => container.values().as_slice(),
            other => panic!("called `as_slice::<u8>()` on ColumnValues::{:?}", other.get_type()),
        }
    }
}

impl AsSlice<u16> for ColumnValues {
    fn as_slice(&self) -> &[u16] {
        match self {
            ColumnValues::Uint2(container) => container.values().as_slice(),
            other => panic!("called `as_slice::<u16>()` on ColumnValues::{:?}", other.get_type()),
        }
    }
}

impl AsSlice<u32> for ColumnValues {
    fn as_slice(&self) -> &[u32] {
        match self {
            ColumnValues::Uint4(container) => container.values().as_slice(),
            other => panic!("called `as_slice::<u32>()` on ColumnValues::{:?}", other.get_type()),
        }
    }
}

impl AsSlice<u64> for ColumnValues {
    fn as_slice(&self) -> &[u64] {
        match self {
            ColumnValues::Uint8(container) => container.values().as_slice(),
            other => panic!("called `as_slice::<u64>()` on ColumnValues::{:?}", other.get_type()),
        }
    }
}

impl AsSlice<u128> for ColumnValues {
    fn as_slice(&self) -> &[u128] {
        match self {
            ColumnValues::Uint16(container) => container.values().as_slice(),
            other => panic!("called `as_slice::<u128>()` on ColumnValues::{:?}", other.get_type()),
        }
    }
}

impl AsSlice<String> for ColumnValues {
    fn as_slice(&self) -> &[String] {
        match self {
            ColumnValues::Utf8(container) => container.values().as_slice(),
            other => {
                panic!("called `as_slice::<String>()` on ColumnValues::{:?}", other.get_type())
            }
        }
    }
}

impl AsSlice<Date> for ColumnValues {
    fn as_slice(&self) -> &[Date] {
        match self {
            ColumnValues::Date(container) => container.values().as_slice(),
            other => panic!("called `as_slice::<Date>()` on ColumnValues::{:?}", other.get_type()),
        }
    }
}

impl AsSlice<DateTime> for ColumnValues {
    fn as_slice(&self) -> &[DateTime] {
        match self {
            ColumnValues::DateTime(container) => container.values().as_slice(),
            other => {
                panic!("called `as_slice::<DateTime>()` on ColumnValues::{:?}", other.get_type())
            }
        }
    }
}

impl AsSlice<Time> for ColumnValues {
    fn as_slice(&self) -> &[Time] {
        match self {
            ColumnValues::Time(container) => container.values().as_slice(),
            other => panic!("called `as_slice::<Time>()` on ColumnValues::{:?}", other.get_type()),
        }
    }
}

impl AsSlice<Interval> for ColumnValues {
    fn as_slice(&self) -> &[Interval] {
        match self {
            ColumnValues::Interval(container) => container.values().as_slice(),
            other => {
                panic!("called `as_slice::<Interval>()` on ColumnValues::{:?}", other.get_type())
            }
        }
    }
}
