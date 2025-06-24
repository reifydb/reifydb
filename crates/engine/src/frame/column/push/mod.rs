// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::frame::ColumnValues;
use reifydb_core::CowVec;
use reifydb_core::num::{IsNumber, SafePromote};
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
    ($t:ty, $variant:ident) => {
        impl Push<$t> for ColumnValues {
            fn push(&mut self, value: $t) {
                match self {
                    ColumnValues::$variant(values, validity) => {
                        values.push(value);
                        validity.push(true);
                    }
                    ColumnValues::Undefined(len) => {
                        let mut values = vec![Default::default(); *len];
                        let mut validity = vec![false; *len];
                        values.push(value);
                        validity.push(true);

                        *self = ColumnValues::$variant(CowVec::new(values), CowVec::new(validity));
                    }
                    other => panic!(
                        "called `push::<{}>()` on ColumnValues::{:?}",
                        stringify!($t),
                        other.kind()
                    ),
                }
            }
        }
    };
}

impl_push!(bool, Bool);
impl_push!(f32, Float4);
impl_push!(f64, Float8);

impl Push<String> for ColumnValues {
    fn push(&mut self, value: String) {
        match self {
            ColumnValues::String(values, validity) => {
                values.push(value);
                validity.push(true);
            }
            ColumnValues::Undefined(len) => {
                let mut values = vec![String::default(); *len];
                let mut validity = vec![false; *len];
                values.push(value);
                validity.push(true);

                *self = ColumnValues::String(CowVec::new(values), CowVec::new(validity));
            }
            other => panic!("called `push::<String>()` on ColumnValues::{:?}", other.kind()),
        }
    }
}
