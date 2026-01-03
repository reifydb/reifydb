// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Builtin function registry for namespace::function() calls.

use std::collections::HashMap;

use crate::{
	error::{Result, VmError},
	vmcore::state::OperandValue,
};

/// Builtin function signature: takes arguments, returns optional result.
pub type BuiltinFn = fn(&[OperandValue]) -> Result<Option<OperandValue>>;

/// Registry of builtin functions, keyed by "namespace::name".
pub struct BuiltinRegistry {
	functions: HashMap<String, BuiltinFn>,
}

impl BuiltinRegistry {
	/// Create a new builtin registry with default functions registered.
	pub fn new() -> Self {
		let mut registry = Self {
			functions: HashMap::new(),
		};
		registry.register_defaults();
		registry
	}

	/// Register default builtin functions.
	fn register_defaults(&mut self) {
		// console namespace
		self.register("console::log", builtin_console_log);
	}

	/// Register a builtin function.
	pub fn register(&mut self, name: &str, func: BuiltinFn) {
		self.functions.insert(name.to_string(), func);
	}

	/// Look up a builtin function by name.
	pub fn get(&self, name: &str) -> Option<&BuiltinFn> {
		self.functions.get(name)
	}

	/// Call a builtin function by name.
	pub fn call(&self, name: &str, args: &[OperandValue]) -> Result<Option<OperandValue>> {
		let func = self.get(name).ok_or_else(|| VmError::UndefinedFunction {
			name: name.to_string(),
		})?;
		func(args)
	}
}

impl Default for BuiltinRegistry {
	fn default() -> Self {
		Self::new()
	}
}

// ============================================================================
// Builtin Functions
// ============================================================================

/// console::log - Print values to stdout.
fn builtin_console_log(args: &[OperandValue]) -> Result<Option<OperandValue>> {
	for arg in args {
		print_value(arg);
	}
	Ok(None)
}

/// Print a value to stdout.
pub fn print_value(value: &OperandValue) {
	match value {
		OperandValue::Scalar(v) => match v {
			reifydb_type::Value::Undefined => println!("undefined"),
			reifydb_type::Value::Boolean(b) => println!("{}", b),
			reifydb_type::Value::Int8(n) => println!("{}", n),
			reifydb_type::Value::Float8(f) => println!("{}", f),
			reifydb_type::Value::Utf8(s) => println!("{}", s),
			_ => println!("{:?}", v),
		},
		OperandValue::Record(r) => {
			print!("{{ ");
			for (i, (name, val)) in r.fields.iter().enumerate() {
				if i > 0 {
					print!(", ");
				}
				print!("{}: {:?}", name, val);
			}
			println!(" }}");
		}
		OperandValue::Frame(cols) => {
			println!("Frame({} columns, {} rows)", cols.len(), cols.row_count());
		}
		_ => println!("{:?}", value),
	}
}
