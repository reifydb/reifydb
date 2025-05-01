// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#[derive(Debug)]
pub enum LiteralExpression {
    Boolean(BooleanExpression),
    Float4(Float4Expression),
    Float8(Float8Expression),
    Int1(Int1Expression),
    Int2(Int2Expression),
    Int4(Int4Expression),
    Int8(Int8Expression),
    Int16(Int16Expression),
    Text(TextExpression),
    Uint1(Uint1Expression),
    Uint2(Uint2Expression),
    Uint4(Uint4Expression),
    Uint8(Uint8Expression),
    Uint16(Uint16Expression),
}

#[derive(Debug)]
pub struct BooleanExpression {
    pub value: bool,
}

#[derive(Debug)]
pub struct Float4Expression {
    pub value: f32,
}

#[derive(Debug)]
pub struct Float8Expression {
    pub value: f64,
}

#[derive(Debug)]
pub struct Int1Expression {
    pub value: i8,
}

#[derive(Debug)]
pub struct Int2Expression {
    pub value: i16,
}

#[derive(Debug)]
pub struct Int4Expression {
    pub value: i32,
}

#[derive(Debug)]
pub struct Int8Expression {
    pub value: i64,
}

#[derive(Debug)]
pub struct Int16Expression {
    pub value: i128,
}

#[derive(Debug)]
pub struct TextExpression {
    pub value: String,
}

#[derive(Debug)]
pub struct Uint1Expression {
    pub value: u8,
}

#[derive(Debug)]
pub struct Uint2Expression {
    pub value: u16,
}

#[derive(Debug)]
pub struct Uint4Expression {
    pub value: u32,
}

#[derive(Debug)]
pub struct Uint8Expression {
    pub value: u64,
}

#[derive(Debug)]
pub struct Uint16Expression {
    pub value: u128,
}
