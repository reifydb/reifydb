// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::fmt;

pub mod value;

pub type FunctionIndex = usize;

#[derive(Clone, Debug, PartialEq)]
pub struct ExternalIndex(pub u32);

#[derive(Debug, Clone, PartialEq)]
pub enum ValueType {
	I32,
	I64,
	F32,
	F64,
	RefExtern,
	RefFunc,
}

impl fmt::Display for ValueType {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			ValueType::I32 => write!(f, "i32"),
			ValueType::I64 => write!(f, "i64"),
			ValueType::F32 => write!(f, "f32"),
			ValueType::F64 => write!(f, "f64"),
			ValueType::RefExtern => write!(f, "extern"),
			ValueType::RefFunc => write!(f, "func"),
		}
	}
}

#[derive(Debug, PartialEq)]
pub enum Trap {
	Error(String),
}

impl fmt::Display for Trap {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Trap::Error(msg) => write!(f, "{}", msg),
		}
	}
}
