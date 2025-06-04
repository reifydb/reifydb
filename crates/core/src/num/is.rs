// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub trait IsFloat {}
impl IsFloat for f32 {}
impl IsFloat for f64 {}

pub trait IsInt {}
impl IsInt for i8 {}
impl IsInt for i16 {}
impl IsInt for i32 {}
impl IsInt for i64 {}
impl IsInt for i128 {}

pub trait IsUint {}
impl IsUint for u8 {}
impl IsUint for u16 {}
impl IsUint for u32 {}
impl IsUint for u64 {}
impl IsUint for u128 {}

