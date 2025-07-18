// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::ColumnValues;
use reifydb_core::{BitVec, CowVec, Date, DateTime, Interval, Time};
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
                    ColumnValues::$variant(values, bitvec) => {
                        values.push(value);
                        bitvec.push(true);
                    }
                    ColumnValues::Undefined(len) => {
                        let mut values = vec![Default::default(); *len];
                        let mut bitvec = BitVec::new(*len, false);
                        values.push(value);
                        bitvec.push(true);

                        *self = ColumnValues::$variant(CowVec::new(values), bitvec);
                    }
                    other => panic!(
                        "called `push::<{}>()` on ColumnValues::{:?}",
                        stringify!($t),
                        other.ty()
                    ),
                }
            }
        }
    };
}

impl_push!(bool, Bool);
impl_push!(f32, Float4);
impl_push!(f64, Float8);
impl_push!(Date, Date);
impl_push!(DateTime, DateTime);
impl_push!(Time, Time);
impl_push!(Interval, Interval);

impl Push<String> for ColumnValues {
    fn push(&mut self, value: String) {
        match self {
            ColumnValues::Utf8(values, bitvec) => {
                values.push(value);
                bitvec.push(true);
            }
            ColumnValues::Undefined(len) => {
                let mut values = vec![String::default(); *len];
                let mut bitvec = BitVec::new(*len, false);
                values.push(value);
                bitvec.push(true);

                *self = ColumnValues::Utf8(CowVec::new(values), bitvec);
            }
            other => panic!("called `push::<String>()` on ColumnValues::{:?}", other.ty()),
        }
    }
}
