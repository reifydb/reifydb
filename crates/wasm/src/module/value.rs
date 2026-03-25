// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::fmt::{self, Display, Formatter};

use crate::module::{ExternalIndex, FunctionIndex, ValueType};

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
	I32(i32),
	I64(i64),
	F32(f32),
	F64(f64),
	RefFunc(FunctionIndex),
	RefExtern(ExternalIndex),
	RefNull(ValueType),
}

impl Display for Value {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			Value::I32(v) => write!(f, "i32({})", v),
			Value::I64(v) => write!(f, "i64({})", v),
			Value::F32(v) => write!(f, "f32({})", v),
			Value::F64(v) => write!(f, "f64({})", v),
			Value::RefFunc(idx) => write!(f, "func({})", idx),
			Value::RefExtern(idx) => write!(f, "extern({})", idx.0),
			Value::RefNull(ty) => write!(f, "null({})", ty),
		}
	}
}

impl From<Value> for ValueType {
	fn from(value: Value) -> Self {
		match value {
			Value::I32(_) => ValueType::I32,
			Value::I64(_) => ValueType::I64,
			Value::F32(_) => ValueType::F32,
			Value::F64(_) => ValueType::F64,
			Value::RefFunc(_) => ValueType::RefFunc,
			Value::RefExtern(_) => ValueType::RefExtern,
			Value::RefNull(value_type) => value_type,
		}
	}
}

impl From<i32> for Value {
	fn from(value: i32) -> Self {
		Value::I32(value)
	}
}

impl From<i64> for Value {
	fn from(value: i64) -> Self {
		Value::I64(value)
	}
}

impl From<f32> for Value {
	fn from(value: f32) -> Self {
		Value::F32(value)
	}
}

impl From<f64> for Value {
	fn from(value: f64) -> Self {
		Value::F64(value)
	}
}

impl From<Value> for i32 {
	fn from(value: Value) -> Self {
		match value {
			Value::I32(value) => value,
			_ => panic!("type mismatch"),
		}
	}
}

impl From<Value> for i64 {
	fn from(value: Value) -> Self {
		match value {
			Value::I64(value) => value,
			_ => panic!("type mismatch"),
		}
	}
}

impl From<Value> for f32 {
	fn from(value: Value) -> Self {
		match value {
			Value::F32(value) => value,
			_ => panic!("type mismatch"),
		}
	}
}

impl From<Value> for f64 {
	fn from(value: Value) -> Self {
		match value {
			Value::F64(value) => value,
			_ => panic!("type mismatch"),
		}
	}
}

#[cfg(not(target_arch = "wasm32"))]
impl From<Value> for wasmtime::Val {
	fn from(value: Value) -> Self {
		match value {
			Value::I32(v) => wasmtime::Val::I32(v),
			Value::I64(v) => wasmtime::Val::I64(v),
			Value::F32(v) => wasmtime::Val::F32(v.to_bits()),
			Value::F64(v) => wasmtime::Val::F64(v.to_bits()),
			Value::RefFunc(_) => wasmtime::Val::null_func_ref(),
			Value::RefExtern(_) => wasmtime::Val::null_any_ref(),
			Value::RefNull(ValueType::RefFunc) => wasmtime::Val::null_func_ref(),
			Value::RefNull(_) => wasmtime::Val::null_any_ref(),
		}
	}
}

#[cfg(not(target_arch = "wasm32"))]
impl Value {
	pub fn from_wasmtime(val: wasmtime::Val) -> Self {
		match val {
			wasmtime::Val::I32(v) => Value::I32(v),
			wasmtime::Val::I64(v) => Value::I64(v),
			wasmtime::Val::F32(bits) => Value::F32(f32::from_bits(bits)),
			wasmtime::Val::F64(bits) => Value::F64(f64::from_bits(bits)),
			wasmtime::Val::FuncRef(None) => Value::RefNull(ValueType::RefFunc),
			wasmtime::Val::FuncRef(Some(_)) => Value::RefFunc(0),
			_ => Value::RefNull(ValueType::RefExtern),
		}
	}
}
