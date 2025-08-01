// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::value::uuid::{Uuid4, Uuid7};
use crate::{Date, DateTime, Interval, Time, Type};

pub trait GetType {
    fn get_type() -> Type;
}

impl GetType for bool {
    fn get_type() -> Type {
        Type::Bool
    }
}

impl GetType for f32 {
    fn get_type() -> Type {
        Type::Float4
    }
}

impl GetType for f64 {
    fn get_type() -> Type {
        Type::Float8
    }
}

impl GetType for i8 {
    fn get_type() -> Type {
        Type::Int1
    }
}

impl GetType for i16 {
    fn get_type() -> Type {
        Type::Int2
    }
}

impl GetType for i32 {
    fn get_type() -> Type {
        Type::Int4
    }
}

impl GetType for i64 {
    fn get_type() -> Type {
        Type::Int8
    }
}

impl GetType for i128 {
    fn get_type() -> Type {
        Type::Int16
    }
}

impl GetType for String {
    fn get_type() -> Type {
        Type::Utf8
    }
}

impl GetType for u8 {
    fn get_type() -> Type {
        Type::Uint1
    }
}

impl GetType for u16 {
    fn get_type() -> Type {
        Type::Uint2
    }
}

impl GetType for u32 {
    fn get_type() -> Type {
        Type::Uint4
    }
}

impl GetType for u64 {
    fn get_type() -> Type {
        Type::Uint8
    }
}

impl GetType for u128 {
    fn get_type() -> Type {
        Type::Uint16
    }
}

impl GetType for Date {
    fn get_type() -> Type {
        Type::Date
    }
}

impl GetType for Time {
    fn get_type() -> Type {
        Type::Time
    }
}

impl GetType for DateTime {
    fn get_type() -> Type {
        Type::DateTime
    }
}

impl GetType for Interval {
    fn get_type() -> Type {
        Type::Interval
    }
}

impl GetType for Uuid4 {
    fn get_type() -> Type {
        Type::Uuid4
    }
}

impl GetType for Uuid7 {
    fn get_type() -> Type {
        Type::Uuid7
    }
}
