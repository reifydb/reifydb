// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#[derive(Debug, Clone, PartialEq)]
pub enum AstLiteral {
    Boolean(AstLiteralBoolean),
    Float4(AstLiteralFloat4),
    Float8(AstLiteralFloat8),
    Int1(AstLiteralInt1),
    Int2(AstLiteralInt2),
    Int4(AstLiteralInt4),
    Int8(AstLiteralInt8),
    Int16(AstLiteralInt16),
    Text(AstLiteralText),
    Uint1(AstLiteralUint1),
    Uint2(AstLiteralUint2),
    Uint4(AstLiteralUint4),
    Uint8(AstLiteralUint8),
    Uint16(AstLiteralUint16),
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralBoolean {
    pub value: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralFloat4 {
    pub value: f32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralFloat8 {
    pub value: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralInt1 {
    pub value: i8,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralInt2 {
    pub value: i16,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralInt4 {
    pub value: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralInt8 {
    pub value: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralInt16 {
    pub value: i128,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralText {
    pub value: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralUint1 {
    pub value: u8,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralUint2 {
    pub value: u16,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralUint4 {
    pub value: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralUint8 {
    pub value: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralUint16 {
    pub value: u128,
}
