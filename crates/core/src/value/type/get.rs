// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Type;

pub trait GetType {
    fn ty(&self) -> Type;
}

impl GetType for bool {
    fn ty(&self) -> Type {
        Type::Bool
    }
}

impl GetType for f32 {
    fn ty(&self) -> Type {
        Type::Float4
    }
}

impl GetType for f64 {
    fn ty(&self) -> Type {
        Type::Float8
    }
}

impl GetType for i8 {
    fn ty(&self) -> Type {
        Type::Int1
    }
}

impl GetType for i16 {
    fn ty(&self) -> Type {
        Type::Int2
    }
}

impl GetType for i32 {
    fn ty(&self) -> Type {
        Type::Int4
    }
}

impl GetType for i64 {
    fn ty(&self) -> Type {
        Type::Int8
    }
}

impl GetType for i128 {
    fn ty(&self) -> Type {
        Type::Int16
    }
}

impl GetType for String {
    fn ty(&self) -> Type {
        Type::Utf8
    }
}

impl GetType for u8 {
    fn ty(&self) -> Type {
        Type::Uint1
    }
}

impl GetType for u16 {
    fn ty(&self) -> Type {
        Type::Uint2
    }
}

impl GetType for u32 {
    fn ty(&self) -> Type {
        Type::Uint4
    }
}

impl GetType for u64 {
    fn ty(&self) -> Type {
        Type::Uint8
    }
}

impl GetType for u128 {
    fn ty(&self) -> Type {
        Type::Uint16
    }
}