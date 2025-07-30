// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::columnar::ColumnData;
use reifydb_core::DateTime;
use reifydb_core::Interval;
use reifydb_core::Time;
use reifydb_core::{Blob, Date};
use std::fmt::Debug;

mod i128;
mod i16;
mod i32;
mod i64;
mod i8;
mod u128;
mod u16;
mod u32;
mod u64;
mod u8;
mod undefined;
mod uuid;
mod value;

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

impl Push<bool> for ColumnData {
    fn push(&mut self, value: bool) {
        match self {
            ColumnData::Bool(container) => {
                container.push(value);
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
impl_push!(Interval, Interval, interval);
impl_push!(Blob, Blob, blob);

impl Push<String> for ColumnData {
    fn push(&mut self, value: String) {
        match self {
            ColumnData::Utf8(container) => {
                container.push(value);
            }
            ColumnData::Undefined(container) => {
                let mut new_container = ColumnData::utf8(vec![String::default(); container.len()]);
                if let ColumnData::Utf8(new_container) = &mut new_container {
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
