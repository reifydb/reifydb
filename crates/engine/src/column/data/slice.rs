// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::EngineColumnData;
use reifydb_core::{Date, DateTime, Interval, Time};

pub trait AsSlice<T> {
    fn as_slice(&self) -> &[T];
}

impl EngineColumnData {
    pub fn as_slice<T>(&self) -> &[T]
    where
        Self: AsSlice<T>,
    {
        <Self as AsSlice<T>>::as_slice(self)
    }
}

impl AsSlice<bool> for EngineColumnData {
    fn as_slice(&self) -> &[bool] {
        match self {
            EngineColumnData::Bool(_) => {
                panic!("as_slice() is not supported for BitVec. Use to_vec() instead.")
            }
            other => panic!("called `as_slice::<bool>()` on EngineColumnData::{:?}", other.get_type()),
        }
    }
}

impl AsSlice<f32> for EngineColumnData {
    fn as_slice(&self) -> &[f32] {
        match self {
            EngineColumnData::Float4(container) => container.data().as_slice(),
            other => panic!("called `as_slice::<f32>()` on EngineColumnData::{:?}", other.get_type()),
        }
    }
}

impl AsSlice<f64> for EngineColumnData {
    fn as_slice(&self) -> &[f64] {
        match self {
            EngineColumnData::Float8(container) => container.data().as_slice(),
            other => panic!("called `as_slice::<f64>()` on EngineColumnData::{:?}", other.get_type()),
        }
    }
}

impl AsSlice<i8> for EngineColumnData {
    fn as_slice(&self) -> &[i8] {
        match self {
            EngineColumnData::Int1(container) => container.data().as_slice(),
            other => panic!("called `as_slice::<i8>()` on EngineColumnData::{:?}", other.get_type()),
        }
    }
}

impl AsSlice<i16> for EngineColumnData {
    fn as_slice(&self) -> &[i16] {
        match self {
            EngineColumnData::Int2(container) => container.data().as_slice(),
            other => panic!("called `as_slice::<i16>()` on EngineColumnData::{:?}", other.get_type()),
        }
    }
}

impl AsSlice<i32> for EngineColumnData {
    fn as_slice(&self) -> &[i32] {
        match self {
            EngineColumnData::Int4(container) => container.data().as_slice(),
            other => panic!("called `as_slice::<i32>()` on EngineColumnData::{:?}", other.get_type()),
        }
    }
}

impl AsSlice<i64> for EngineColumnData {
    fn as_slice(&self) -> &[i64] {
        match self {
            EngineColumnData::Int8(container) => container.data().as_slice(),
            other => panic!("called `as_slice::<i64>()` on EngineColumnData::{:?}", other.get_type()),
        }
    }
}

impl AsSlice<i128> for EngineColumnData {
    fn as_slice(&self) -> &[i128] {
        match self {
            EngineColumnData::Int16(container) => container.data().as_slice(),
            other => panic!("called `as_slice::<i128>()` on EngineColumnData::{:?}", other.get_type()),
        }
    }
}

impl AsSlice<u8> for EngineColumnData {
    fn as_slice(&self) -> &[u8] {
        match self {
            EngineColumnData::Uint1(container) => container.data().as_slice(),
            other => panic!("called `as_slice::<u8>()` on EngineColumnData::{:?}", other.get_type()),
        }
    }
}

impl AsSlice<u16> for EngineColumnData {
    fn as_slice(&self) -> &[u16] {
        match self {
            EngineColumnData::Uint2(container) => container.data().as_slice(),
            other => panic!("called `as_slice::<u16>()` on EngineColumnData::{:?}", other.get_type()),
        }
    }
}

impl AsSlice<u32> for EngineColumnData {
    fn as_slice(&self) -> &[u32] {
        match self {
            EngineColumnData::Uint4(container) => container.data().as_slice(),
            other => panic!("called `as_slice::<u32>()` on EngineColumnData::{:?}", other.get_type()),
        }
    }
}

impl AsSlice<u64> for EngineColumnData {
    fn as_slice(&self) -> &[u64] {
        match self {
            EngineColumnData::Uint8(container) => container.data().as_slice(),
            other => panic!("called `as_slice::<u64>()` on EngineColumnData::{:?}", other.get_type()),
        }
    }
}

impl AsSlice<u128> for EngineColumnData {
    fn as_slice(&self) -> &[u128] {
        match self {
            EngineColumnData::Uint16(container) => container.data().as_slice(),
            other => panic!("called `as_slice::<u128>()` on EngineColumnData::{:?}", other.get_type()),
        }
    }
}

impl AsSlice<String> for EngineColumnData {
    fn as_slice(&self) -> &[String] {
        match self {
            EngineColumnData::Utf8(container) => container.data().as_slice(),
            other => {
                panic!("called `as_slice::<String>()` on EngineColumnData::{:?}", other.get_type())
            }
        }
    }
}

impl AsSlice<Date> for EngineColumnData {
    fn as_slice(&self) -> &[Date] {
        match self {
            EngineColumnData::Date(container) => container.data().as_slice(),
            other => panic!("called `as_slice::<Date>()` on EngineColumnData::{:?}", other.get_type()),
        }
    }
}

impl AsSlice<DateTime> for EngineColumnData {
    fn as_slice(&self) -> &[DateTime] {
        match self {
            EngineColumnData::DateTime(container) => container.data().as_slice(),
            other => {
                panic!("called `as_slice::<DateTime>()` on EngineColumnData::{:?}", other.get_type())
            }
        }
    }
}

impl AsSlice<Time> for EngineColumnData {
    fn as_slice(&self) -> &[Time] {
        match self {
            EngineColumnData::Time(container) => container.data().as_slice(),
            other => panic!("called `as_slice::<Time>()` on EngineColumnData::{:?}", other.get_type()),
        }
    }
}

impl AsSlice<Interval> for EngineColumnData {
    fn as_slice(&self) -> &[Interval] {
        match self {
            EngineColumnData::Interval(container) => container.data().as_slice(),
            other => {
                panic!("called `as_slice::<Interval>()` on EngineColumnData::{:?}", other.get_type())
            }
        }
    }
}
