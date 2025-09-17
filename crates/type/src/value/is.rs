// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use std::fmt::{Debug, Display};

use super::{int::Int, uint::Uint};
use crate::{Date, DateTime, Decimal, Interval, Time, Uuid4, Uuid7, value::r#type::GetType};

pub trait IsNumber: Display + Clone + Debug + PartialEq + PartialOrd + GetType + Default {}
pub trait IsTemporal: Display + Clone + Debug + PartialEq + PartialOrd + GetType + Default {}
pub trait IsUuid: Display + Clone + Debug + PartialEq + PartialOrd + GetType + Default {}

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

impl IsNumber for Int {}
impl IsInt for Int {}

impl IsNumber for Uint {}
impl IsUint for Uint {}

impl IsNumber for Decimal {}
impl IsFloat for Decimal {}

pub trait IsDate: IsTemporal {}
pub trait IsTime: IsTemporal {}

impl IsTemporal for Date {}
impl IsDate for Date {}

impl IsTemporal for DateTime {}
impl IsDate for DateTime {}
impl IsTime for DateTime {}

impl IsTemporal for Time {}
impl IsTime for Time {}

impl IsTemporal for Interval {}

impl IsUuid for Uuid4 {}
impl IsUuid for Uuid7 {}
