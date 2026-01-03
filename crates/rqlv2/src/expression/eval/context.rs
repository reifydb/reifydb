// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Evaluation context for compiled expressions.

use std::{collections::HashMap, sync::Arc};

use reifydb_core::value::column::{ColumnData, Columns};
use reifydb_type::Value;

use super::{ScriptFunctionCaller, value::EvalValue};
use crate::expression::types::EvalError;

/// Context for expression evaluation with captured scope variables.
#[derive(Default, Clone)]
pub struct EvalContext {
	/// Captured variable values from scope (by variable ID).
	pub variables: HashMap<u32, EvalValue>,

	/// Current row values for correlated subquery execution.
	/// Maps column names to their values for the current outer row.
	pub current_row_values: Option<HashMap<String, Value>>,

	/// Script function caller for executing user-defined functions.
	/// Provided by the VM at runtime.
	pub script_functions: Option<Arc<dyn ScriptFunctionCaller>>,
}

impl EvalContext {
	/// Create an empty evaluation context.
	pub fn new() -> Self {
		Self {
			variables: HashMap::new(),
			current_row_values: None,
			script_functions: None,
		}
	}

	/// Create a context with the given variables.
	pub fn with_variables(variables: HashMap<u32, EvalValue>) -> Self {
		Self {
			variables,
			current_row_values: None,
			script_functions: None,
		}
	}

	/// Create a context with a script function caller.
	pub fn with_script_functions(caller: Arc<dyn ScriptFunctionCaller>) -> Self {
		Self {
			variables: HashMap::new(),
			current_row_values: None,
			script_functions: Some(caller),
		}
	}

	/// Get a variable value by ID.
	pub fn get_var(&self, id: u32) -> Option<&EvalValue> {
		self.variables.get(&id)
	}

	/// Set a variable value by ID.
	pub fn set_var(&mut self, id: u32, value: EvalValue) {
		self.variables.insert(id, value);
	}

	/// Get a value from current_row_values (for correlated subquery column lookup).
	pub fn get_outer_column(&self, name: &str) -> Option<&Value> {
		self.current_row_values.as_ref()?.get(name)
	}

	/// Create a new context with outer row values for correlated subquery execution.
	pub fn with_outer_row(&self, outer_values: HashMap<String, Value>) -> Self {
		Self {
			variables: self.variables.clone(),
			current_row_values: Some(outer_values),
			script_functions: self.script_functions.clone(),
		}
	}

	/// Call a script function by name with columnar arguments.
	///
	/// # Arguments
	/// * `name` - The name of the script function to call
	/// * `args` - Columnar arguments (one column per parameter)
	/// * `row_count` - Number of rows to produce in the result
	///
	/// # Returns
	/// Columnar result data.
	pub fn call_script_function(
		&self,
		name: &str,
		args: &Columns,
		row_count: usize,
	) -> Result<ColumnData, EvalError> {
		self.script_functions
			.as_ref()
			.ok_or_else(|| EvalError::UnsupportedOperation {
				operation: format!("script function '{}' (no executor available)", name),
			})?
			.call(name, args, row_count)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_eval_context_new() {
		let ctx = EvalContext::new();
		assert!(ctx.variables.is_empty());
		assert!(ctx.current_row_values.is_none());
	}

	#[test]
	fn test_eval_context_with_variables() {
		let mut vars = HashMap::new();
		vars.insert(1, EvalValue::Scalar(Value::Int8(42)));
		vars.insert(2, EvalValue::Scalar(Value::Boolean(true)));

		let ctx = EvalContext::with_variables(vars);

		assert!(matches!(ctx.get_var(1), Some(EvalValue::Scalar(Value::Int8(42)))));
		assert!(matches!(ctx.get_var(2), Some(EvalValue::Scalar(Value::Boolean(true)))));
		assert!(ctx.get_var(3).is_none());
	}

	#[test]
	fn test_eval_context_set_var() {
		let mut ctx = EvalContext::new();
		ctx.set_var(1, EvalValue::Scalar(Value::Int8(100)));

		assert!(matches!(ctx.get_var(1), Some(EvalValue::Scalar(Value::Int8(100)))));
	}

	#[test]
	fn test_eval_context_with_outer_row() {
		let mut outer_values = HashMap::new();
		outer_values.insert("id".to_string(), Value::Int8(1));
		outer_values.insert("name".to_string(), Value::Utf8("test".into()));

		let ctx = EvalContext::new().with_outer_row(outer_values);

		assert!(matches!(ctx.get_outer_column("id"), Some(Value::Int8(1))));
		assert!(ctx.get_outer_column("missing").is_none());
	}
}
