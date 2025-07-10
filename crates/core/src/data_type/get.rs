// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::DataType;

pub trait GetKind {
    fn data_type(&self) -> DataType;
}

impl GetKind for bool {
    fn data_type(&self) -> DataType {
        DataType::Bool
    }
}

impl GetKind for f32 {
    fn data_type(&self) -> DataType {
        DataType::Float4
    }
}

impl GetKind for f64 {
    fn data_type(&self) -> DataType {
        DataType::Float8
    }
}

impl GetKind for i8 {
    fn data_type(&self) -> DataType {
        DataType::Int1
    }
}

impl GetKind for i16 {
    fn data_type(&self) -> DataType {
        DataType::Int2
    }
}

impl GetKind for i32 {
    fn data_type(&self) -> DataType {
        DataType::Int4
    }
}

impl GetKind for i64 {
    fn data_type(&self) -> DataType {
        DataType::Int8
    }
}

impl GetKind for i128 {
    fn data_type(&self) -> DataType {
        DataType::Int16
    }
}

impl GetKind for String {
    fn data_type(&self) -> DataType {
        DataType::Utf8
    }
}

impl GetKind for u8 {
    fn data_type(&self) -> DataType {
        DataType::Uint1
    }
}

impl GetKind for u16 {
    fn data_type(&self) -> DataType {
        DataType::Uint2
    }
}

impl GetKind for u32 {
    fn data_type(&self) -> DataType {
        DataType::Uint4
    }
}

impl GetKind for u64 {
    fn data_type(&self) -> DataType {
        DataType::Uint8
    }
}

impl GetKind for u128 {
    fn data_type(&self) -> DataType {
        DataType::Uint16
    }
}
