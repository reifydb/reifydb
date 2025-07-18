// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::{ColumnValues, Push};
use reifydb_core::value::number::{SafeConvert, SafePromote};
use reifydb_core::{BitVec, CowVec};

impl Push<i8> for ColumnValues {
    fn push(&mut self, value: i8) {
        match self {
            ColumnValues::Float4(values, bitvec) => {
                match <i8 as SafeConvert<f32>>::checked_convert(value) {
                    Some(v) => {
                        values.push(v);
                        bitvec.push(true);
                    }
                    None => {
                        values.push(0.0);
                        bitvec.push(false);
                    }
                }
            }
            ColumnValues::Float8(values, bitvec) => {
                match <i8 as SafeConvert<f64>>::checked_convert(value) {
                    Some(v) => {
                        values.push(v);
                        bitvec.push(true);
                    }
                    None => {
                        values.push(0.0);
                        bitvec.push(false);
                    }
                }
            }
            ColumnValues::Int1(values, bitvec) => {
                values.push(value);
                bitvec.push(true);
            }
            ColumnValues::Int2(values, bitvec) => {
                match <i8 as SafePromote<i16>>::checked_promote(value) {
                    Some(v) => {
                        values.push(v);
                        bitvec.push(true);
                    }
                    None => {
                        values.push(0);
                        bitvec.push(false);
                    }
                }
            }
            ColumnValues::Int4(values, bitvec) => {
                match <i8 as SafePromote<i32>>::checked_promote(value) {
                    Some(v) => {
                        values.push(v);
                        bitvec.push(true);
                    }
                    None => {
                        values.push(0);
                        bitvec.push(false);
                    }
                }
            }
            ColumnValues::Int8(values, bitvec) => {
                match <i8 as SafePromote<i64>>::checked_promote(value) {
                    Some(v) => {
                        values.push(v);
                        bitvec.push(true);
                    }
                    None => {
                        values.push(0);
                        bitvec.push(false);
                    }
                }
            }
            ColumnValues::Int16(values, bitvec) => {
                match <i8 as SafePromote<i128>>::checked_promote(value) {
                    Some(v) => {
                        values.push(v);
                        bitvec.push(true);
                    }
                    None => {
                        values.push(0);
                        bitvec.push(false);
                    }
                }
            }
            ColumnValues::Uint1(values, bitvec) => {
                match <i8 as SafeConvert<u8>>::checked_convert(value) {
                    Some(v) => {
                        values.push(v);
                        bitvec.push(true);
                    }
                    None => {
                        values.push(0);
                        bitvec.push(false);
                    }
                }
            }
            ColumnValues::Uint2(values, bitvec) => {
                match <i8 as SafeConvert<u16>>::checked_convert(value) {
                    Some(v) => {
                        values.push(v);
                        bitvec.push(true);
                    }
                    None => {
                        values.push(0);
                        bitvec.push(false);
                    }
                }
            }
            ColumnValues::Uint4(values, bitvec) => {
                match <i8 as SafeConvert<u32>>::checked_convert(value) {
                    Some(v) => {
                        values.push(v);
                        bitvec.push(true);
                    }
                    None => {
                        values.push(0);
                        bitvec.push(false);
                    }
                }
            }
            ColumnValues::Uint8(values, bitvec) => {
                match <i8 as SafeConvert<u64>>::checked_convert(value) {
                    Some(v) => {
                        values.push(v);
                        bitvec.push(true);
                    }
                    None => {
                        values.push(0);
                        bitvec.push(false);
                    }
                }
            }
            ColumnValues::Uint16(values, bitvec) => {
                match <i8 as SafeConvert<u128>>::checked_convert(value) {
                    Some(v) => {
                        values.push(v);
                        bitvec.push(true);
                    }
                    None => {
                        values.push(0);
                        bitvec.push(false);
                    }
                }
            }
            ColumnValues::Undefined(len) => {
                let mut values = vec![0i8; *len];
                let mut bitvec = BitVec::new(*len, false);
                values.push(value);
                bitvec.push(true);
                *self = ColumnValues::Int1(CowVec::new(values), bitvec);
            }
            other => {
                panic!("called `push::<i8>()` on incompatible ColumnValues::{:?}", other.ty());
            }
        }
    }
}
