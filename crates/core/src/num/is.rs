// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::fmt::Debug;

pub trait IsNumber: Copy + Debug + PartialOrd {}

pub trait IsFloat: IsNumber {}

impl IsNumber for f32 {}
impl IsFloat for f32 {}

impl IsNumber for f64 {}
impl IsFloat for f64 {}

pub trait IsInt: IsNumber {}

impl IsNumber for i8 {}
impl IsInt for i8 {}

impl IsNumber for i16 {}
impl IsInt for i16 {}

impl IsNumber for i32 {}
impl IsInt for i32 {}

impl IsNumber for i64 {}
impl IsInt for i64 {}

impl IsNumber for i128 {}
impl IsInt for i128 {}

pub trait IsUint: IsNumber {}

impl IsNumber for u8 {}
impl IsUint for u8 {}

impl IsNumber for u16 {}
impl IsUint for u16 {}

impl IsNumber for u32 {}
impl IsUint for u32 {}

impl IsNumber for u64 {}
impl IsUint for u64 {}

impl IsNumber for u128 {}
impl IsUint for u128 {}
