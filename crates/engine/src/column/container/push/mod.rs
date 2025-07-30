// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::ColumnValues;
use reifydb_core::{Date, DateTime, Interval, Time};
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

impl ColumnValues {
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
        impl Push<$t> for ColumnValues {
            fn push(&mut self, value: $t) {
                match self {
                    ColumnValues::$variant(container) => {
                        container.push(value);
                    }
                    ColumnValues::Undefined(container) => {
                        let mut new_container = ColumnValues::$factory(vec![<$t>::default(); container.len()]);
                        if let ColumnValues::$variant(new_container) = &mut new_container {
                            new_container.push(value);
                        }
                        *self = new_container;
                    }
                    other => panic!(
                        "called `push::<{}>()` on ColumnValues::{:?}",
                        stringify!($t),
                        other.get_type()
                    ),
                }
            }
        }
    };
}

impl Push<bool> for ColumnValues {
    fn push(&mut self, value: bool) {
        match self {
            ColumnValues::Bool(container) => {
                container.push(value);
            }
            ColumnValues::Undefined(container) => {
                let mut new_container = ColumnValues::bool(vec![false; container.len()]);
                if let ColumnValues::Bool(new_container) = &mut new_container {
                    new_container.push(value);
                }
                *self = new_container;
            }
            other => panic!(
                "called `push::<bool>()` on ColumnValues::{:?}",
                other.get_type()
            ),
        }
    }
}

impl_push!(f32, Float4, float4);
impl_push!(f64, Float8, float8);
impl_push!(Date, Date, date);
impl_push!(DateTime, DateTime, datetime);
impl_push!(Time, Time, time);
impl_push!(Interval, Interval, interval);


impl Push<String> for ColumnValues {
    fn push(&mut self, value: String) {
        match self {
            ColumnValues::Utf8(container) => {
                container.push(value);
            }
            ColumnValues::Undefined(container) => {
                let mut new_container = ColumnValues::utf8(vec![String::default(); container.len()]);
                if let ColumnValues::Utf8(new_container) = &mut new_container {
                    new_container.push(value);
                }
                *self = new_container;
            }
            other => panic!("called `push::<String>()` on ColumnValues::{:?}", other.get_type()),
        }
    }
}
