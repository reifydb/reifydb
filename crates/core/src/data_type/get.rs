// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::DataType;

pub trait GetDataType {
    fn data_type(&self) -> DataType;
}

impl GetDataType for bool {
    fn data_type(&self) -> DataType {
        DataType::Bool
    }
}

impl GetDataType for f32 {
    fn data_type(&self) -> DataType {
        DataType::Float4
    }
}

impl GetDataType for f64 {
    fn data_type(&self) -> DataType {
        DataType::Float8
    }
}

impl GetDataType for i8 {
    fn data_type(&self) -> DataType {
        DataType::Int1
    }
}

impl GetDataType for i16 {
    fn data_type(&self) -> DataType {
        DataType::Int2
    }
}

impl GetDataType for i32 {
    fn data_type(&self) -> DataType {
        DataType::Int4
    }
}

impl GetDataType for i64 {
    fn data_type(&self) -> DataType {
        DataType::Int8
    }
}

impl GetDataType for i128 {
    fn data_type(&self) -> DataType {
        DataType::Int16
    }
}

impl GetDataType for String {
    fn data_type(&self) -> DataType {
        DataType::Utf8
    }
}

impl GetDataType for u8 {
    fn data_type(&self) -> DataType {
        DataType::Uint1
    }
}

impl GetDataType for u16 {
    fn data_type(&self) -> DataType {
        DataType::Uint2
    }
}

impl GetDataType for u32 {
    fn data_type(&self) -> DataType {
        DataType::Uint4
    }
}

impl GetDataType for u64 {
    fn data_type(&self) -> DataType {
        DataType::Uint8
    }
}

impl GetDataType for u128 {
    fn data_type(&self) -> DataType {
        DataType::Uint16
    }
}
