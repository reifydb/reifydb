// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{ColumnValues, ValueRef};

impl ColumnValues {
    pub fn get(&self, index: usize) -> ValueRef {
        match self {
            ColumnValues::Bool(v, b) => {
                if b[index] {
                    ValueRef::Bool(&v[index])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Float4(v, b) => {
                if b[index] {
                    ValueRef::Float4(&v[index])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Float8(v, b) => {
                if b[index] {
                    ValueRef::Float8(&v[index])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Int1(v, b) => {
                if b[index] {
                    ValueRef::Int1(&v[index])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Int2(v, b) => {
                if b[index] {
                    ValueRef::Int2(&v[index])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Int4(v, b) => {
                if b[index] {
                    ValueRef::Int4(&v[index])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Int8(v, b) => {
                if b[index] {
                    ValueRef::Int8(&v[index])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Int16(v, b) => {
                if b[index] {
                    ValueRef::Int16(&v[index])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::String(v, b) => {
                if b[index] {
                    ValueRef::String(&v[index])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Uint1(v, b) => {
                if b[index] {
                    ValueRef::Uint1(&v[index])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Uint2(v, b) => {
                if b[index] {
                    ValueRef::Uint2(&v[index])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Uint4(v, b) => {
                if b[index] {
                    ValueRef::Uint4(&v[index])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Uint8(v, b) => {
                if b[index] {
                    ValueRef::Uint8(&v[index])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Uint16(v, b) => {
                if b[index] {
                    ValueRef::Uint16(&v[index])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Undefined(_) => ValueRef::Undefined,
        }
    }
}
