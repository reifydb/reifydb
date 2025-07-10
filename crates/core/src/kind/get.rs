// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Kind;

pub trait GetKind {
    fn kind(&self) -> Kind;
}

impl GetKind for bool {
    fn kind(&self) -> Kind {
        Kind::Bool
    }
}

impl GetKind for f32 {
    fn kind(&self) -> Kind {
        Kind::Float4
    }
}

impl GetKind for f64 {
    fn kind(&self) -> Kind {
        Kind::Float8
    }
}

impl GetKind for i8 {
    fn kind(&self) -> Kind {
        Kind::Int1
    }
}

impl GetKind for i16 {
    fn kind(&self) -> Kind {
        Kind::Int2
    }
}

impl GetKind for i32 {
    fn kind(&self) -> Kind {
        Kind::Int4
    }
}

impl GetKind for i64 {
    fn kind(&self) -> Kind {
        Kind::Int8
    }
}

impl GetKind for i128 {
    fn kind(&self) -> Kind {
        Kind::Int16
    }
}

impl GetKind for String {
    fn kind(&self) -> Kind {
        Kind::Utf8
    }
}

impl GetKind for u8 {
    fn kind(&self) -> Kind {
        Kind::Uint1
    }
}

impl GetKind for u16 {
    fn kind(&self) -> Kind {
        Kind::Uint2
    }
}

impl GetKind for u32 {
    fn kind(&self) -> Kind {
        Kind::Uint4
    }
}

impl GetKind for u64 {
    fn kind(&self) -> Kind {
        Kind::Uint8
    }
}

impl GetKind for u128 {
    fn kind(&self) -> Kind {
        Kind::Uint16
    }
}
