// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::EngineColumnData;
use reifydb_core::Date;
use reifydb_core::DateTime;
use reifydb_core::Interval;
use reifydb_core::Time;
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

impl EngineColumnData {
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
        impl Push<$t> for EngineColumnData {
            fn push(&mut self, value: $t) {
                match self {
                    EngineColumnData::$variant(container) => {
                        container.push(value);
                    }
                    EngineColumnData::Undefined(container) => {
                        let mut new_container =
                            EngineColumnData::$factory(vec![<$t>::default(); container.len()]);
                        if let EngineColumnData::$variant(new_container) = &mut new_container {
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

impl Push<bool> for EngineColumnData {
    fn push(&mut self, value: bool) {
        match self {
            EngineColumnData::Bool(container) => {
                container.push(value);
            }
            EngineColumnData::Undefined(container) => {
                let mut new_container = EngineColumnData::bool(vec![false; container.len()]);
                if let EngineColumnData::Bool(new_container) = &mut new_container {
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

impl Push<String> for EngineColumnData {
    fn push(&mut self, value: String) {
        match self {
            EngineColumnData::Utf8(container) => {
                container.push(value);
            }
            EngineColumnData::Undefined(container) => {
                let mut new_container =
                    EngineColumnData::utf8(vec![String::default(); container.len()]);
                if let EngineColumnData::Utf8(new_container) = &mut new_container {
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
