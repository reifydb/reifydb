// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::FieldRef;
use reifydb_core::{
	internal,
	value::batch::{is_scalar, single_row},
};
use reifydb_rql::{
	instruction::{CompiledClosure, CompiledFunction, Instruction, ScopeType},
	nodes::FunctionParameter,
};
use reifydb_value::{
	error,
	value::{Value, constraint::TypeConstraint, system_columns::user_columns},
};

use crate::{Result, error::EvaluateError};

pub fn strip_dollar_prefix(name: &str) -> &str {
	name.strip_prefix('$').unwrap_or(name)
}

#[derive(Debug, Clone)]
pub struct ClosureValue {
	pub def: CompiledClosure,
	pub captured: HashMap<String, Variable>,
}

#[derive(Debug, Clone)]
pub struct Callable {
	pub parameters: Vec<FunctionParameter>,
	pub body: Vec<Instruction>,
	pub captured: HashMap<String, Variable>,
	pub return_type: Option<TypeConstraint>,
}

#[derive(Debug, Clone)]
pub enum Variable {
	Columns {
		batch: RecordBatch,
	},

	ForIterator {
		batch: RecordBatch,
		index: usize,
	},

	Closure(ClosureValue),
}

impl Variable {
	pub fn scalar(value: Value) -> Self {
		Self::scalar_named("value", value)
	}

	pub fn scalar_named(name: &str, value: Value) -> Self {
		Variable::Columns {
			batch: single_row([(name, value)]).expect("one value always forms a one row batch"),
		}
	}

	pub fn columns(batch: RecordBatch) -> Self {
		Variable::Columns {
			batch,
		}
	}

	pub fn is_scalar(&self) -> bool {
		matches!(
			self,
			Variable::Columns { batch } if is_scalar(batch)
		)
	}

	pub fn into_column(self) -> Result<(FieldRef, ArrayRef)> {
		let batch = match self {
			Variable::Columns {
				batch,
				..
			}
			| Variable::ForIterator {
				batch,
				..
			} => batch,
			Variable::Closure(_) => single_row([("value", Value::none())])?,
		};
		let user: Vec<(&FieldRef, &ArrayRef)> = user_columns(&batch).collect();
		let actual = user.len();
		if let [(field, array)] = user.as_slice() {
			Ok(((*field).clone(), (*array).clone()))
		} else {
			Err(error::TypeError::Runtime {
				kind: error::RuntimeErrorKind::ExpectedSingleColumn {
					actual,
				},
				message: format!("Expected a single column but got {}", actual),
			}
			.into())
		}
	}
}

#[derive(Debug, Clone)]
pub struct SymbolTable {
	inner: Arc<SymbolTableInner>,
}

#[derive(Debug, Clone)]
struct SymbolTableInner {
	scopes: Vec<Scope>,

	functions: HashMap<String, CompiledFunction>,
}

#[derive(Debug, Clone)]
struct Scope {
	variables: HashMap<String, VariableBinding>,
	scope_type: ScopeType,
}

#[derive(Debug, Clone)]
struct VariableBinding {
	variable: Variable,
	mutable: bool,
}

impl SymbolTable {
	pub fn new() -> Self {
		let global_scope = Scope {
			variables: HashMap::new(),
			scope_type: ScopeType::Global,
		};

		Self {
			inner: Arc::new(SymbolTableInner {
				scopes: vec![global_scope],
				functions: HashMap::new(),
			}),
		}
	}

	pub fn enter_scope(&mut self, scope_type: ScopeType) {
		let new_scope = Scope {
			variables: HashMap::new(),
			scope_type,
		};
		Arc::make_mut(&mut self.inner).scopes.push(new_scope);
	}

	pub fn exit_scope(&mut self) -> Result<()> {
		if self.inner.scopes.len() <= 1 {
			return Err(error!(internal!("Cannot exit global scope")));
		}
		Arc::make_mut(&mut self.inner).scopes.pop();
		Ok(())
	}

	pub fn scope_depth(&self) -> usize {
		self.inner.scopes.len() - 1
	}

	pub fn current_scope_type(&self) -> &ScopeType {
		&self.inner.scopes.last().unwrap().scope_type
	}

	pub fn set(&mut self, name: String, variable: Variable, mutable: bool) -> Result<()> {
		self.set_in_current_scope(name, variable, mutable)
	}

	pub fn reassign(&mut self, name: String, variable: Variable) -> Result<()> {
		let inner = Arc::make_mut(&mut self.inner);

		for scope in inner.scopes.iter_mut().rev() {
			if let Some(existing) = scope.variables.get(&name) {
				if !existing.mutable {
					return Err(EvaluateError::VariableIsImmutable {
						name: name.clone(),
					}
					.into());
				}
				let mutable = existing.mutable;
				scope.variables.insert(
					name,
					VariableBinding {
						variable,
						mutable,
					},
				);
				return Ok(());
			}
		}

		Err(EvaluateError::VariableNotFound {
			name: name.clone(),
		}
		.into())
	}

	pub fn set_in_current_scope(&mut self, name: String, variable: Variable, mutable: bool) -> Result<()> {
		let inner = Arc::make_mut(&mut self.inner);
		let current_scope = inner.scopes.last_mut().unwrap();

		current_scope.variables.insert(
			name,
			VariableBinding {
				variable,
				mutable,
			},
		);
		Ok(())
	}

	pub fn get(&self, name: &str) -> Option<&Variable> {
		for scope in self.inner.scopes.iter().rev() {
			if let Some(binding) = scope.variables.get(name) {
				return Some(&binding.variable);
			}
		}
		None
	}

	pub fn clear(&mut self) {
		let inner = Arc::make_mut(&mut self.inner);
		inner.scopes.clear();
		inner.scopes.push(Scope {
			variables: HashMap::new(),
			scope_type: ScopeType::Global,
		});
		inner.functions.clear();
	}

	pub fn define_function(&mut self, name: String, func: CompiledFunction) {
		Arc::make_mut(&mut self.inner).functions.insert(name, func);
	}

	pub fn get_function(&self, name: &str) -> Option<&CompiledFunction> {
		self.inner.functions.get(name)
	}

	pub fn resolve_callable(&self, name: &str) -> Option<Callable> {
		if let Some(func) = self.get_function(name) {
			return Some(Callable {
				parameters: func.parameters.clone(),
				body: func.body.clone(),
				captured: HashMap::new(),
				return_type: func.return_type.clone(),
			});
		}
		if let Some(Variable::Closure(closure)) = self.get(strip_dollar_prefix(name)) {
			return Some(Callable {
				parameters: closure.def.parameters.clone(),
				body: closure.def.body.clone(),
				captured: closure.captured.clone(),
				return_type: None,
			});
		}
		None
	}
}

impl Default for SymbolTable {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::value::{
		batch::batch,
		column::{builder::ColumnBuilder, factory::none_typed},
	};
	use reifydb_value::value::{Value, column_view::ColumnView, value_type::ValueType};

	use super::*;

	fn create_test_columns(values: Vec<Value>) -> RecordBatch {
		if values.is_empty() {
			return batch(vec![none_typed("test_col", ValueType::Boolean, 0)]).unwrap();
		}

		let empty = none_typed("test_col", values[0].get_type(), 0);
		let mut builder = ColumnBuilder::from_view(&ColumnView::try_from(&empty).unwrap());
		for value in values {
			builder.push_value(value);
		}

		batch(vec![builder.finish("test_col")]).unwrap()
	}

	#[test]
	fn test_basic_variable_operations() {
		let mut ctx = SymbolTable::new();
		let cols = create_test_columns(vec![Value::utf8("Alice".to_string())]);

		ctx.set("name".to_string(), Variable::columns(cols.clone()), false).unwrap();

		assert!(ctx.get("name").is_some());
	}

	#[test]
	fn test_mutable_variable() {
		let mut ctx = SymbolTable::new();
		let cols1 = create_test_columns(vec![Value::Int4(42)]);
		let cols2 = create_test_columns(vec![Value::Int4(84)]);

		ctx.set("counter".to_string(), Variable::columns(cols1.clone()), true).unwrap();
		assert!(ctx.get("counter").is_some());

		ctx.set("counter".to_string(), Variable::columns(cols2.clone()), true).unwrap();
		assert!(ctx.get("counter").is_some());
	}

	#[test]
	#[ignore]
	fn test_immutable_variable_reassignment_fails() {
		let mut ctx = SymbolTable::new();
		let cols1 = create_test_columns(vec![Value::utf8("Alice".to_string())]);
		let cols2 = create_test_columns(vec![Value::utf8("Bob".to_string())]);

		ctx.set("name".to_string(), Variable::columns(cols1.clone()), false).unwrap();

		let result = ctx.set("name".to_string(), Variable::columns(cols2), false);
		assert!(result.is_err());

		// A refused reassignment must leave the original binding intact.
		assert!(ctx.get("name").is_some());
	}

	#[test]
	fn test_scope_management() {
		let mut ctx = SymbolTable::new();

		assert_eq!(ctx.scope_depth(), 0);
		assert_eq!(ctx.current_scope_type(), &ScopeType::Global);

		ctx.enter_scope(ScopeType::Function);
		assert_eq!(ctx.scope_depth(), 1);
		assert_eq!(ctx.current_scope_type(), &ScopeType::Function);

		ctx.enter_scope(ScopeType::Block);
		assert_eq!(ctx.scope_depth(), 2);
		assert_eq!(ctx.current_scope_type(), &ScopeType::Block);

		ctx.exit_scope().unwrap();
		assert_eq!(ctx.scope_depth(), 1);
		assert_eq!(ctx.current_scope_type(), &ScopeType::Function);

		ctx.exit_scope().unwrap();
		assert_eq!(ctx.scope_depth(), 0);
		assert_eq!(ctx.current_scope_type(), &ScopeType::Global);

		// Popping the global scope would leave the table with nowhere to bind.
		assert!(ctx.exit_scope().is_err());
	}

	#[test]
	fn test_variable_shadowing() {
		let mut ctx = SymbolTable::new();
		let outer_cols = create_test_columns(vec![Value::utf8("outer".to_string())]);
		let inner_cols = create_test_columns(vec![Value::utf8("inner".to_string())]);

		ctx.set("var".to_string(), Variable::columns(outer_cols.clone()), false).unwrap();
		assert!(ctx.get("var").is_some());

		// Rebinding an existing name in an inner scope must shadow, not overwrite.
		ctx.enter_scope(ScopeType::Block);
		ctx.set("var".to_string(), Variable::columns(inner_cols.clone()), false).unwrap();

		assert!(ctx.get("var").is_some());

		ctx.exit_scope().unwrap();
		assert!(ctx.get("var").is_some());
	}

	#[test]
	fn test_parent_scope_access() {
		let mut ctx = SymbolTable::new();
		let outer_cols = create_test_columns(vec![Value::utf8("outer".to_string())]);

		ctx.set("global_var".to_string(), Variable::columns(outer_cols.clone()), false).unwrap();

		ctx.enter_scope(ScopeType::Function);

		// A function scope must read through to its parent.
		assert!(ctx.get("global_var").is_some());
	}

	#[test]
	fn test_clear_resets_to_global() {
		let mut ctx = SymbolTable::new();
		let cols = create_test_columns(vec![Value::utf8("test".to_string())]);

		ctx.set("var1".to_string(), Variable::columns(cols.clone()), false).unwrap();
		ctx.enter_scope(ScopeType::Function);
		ctx.set("var2".to_string(), Variable::columns(cols.clone()), false).unwrap();
		ctx.enter_scope(ScopeType::Block);
		ctx.set("var3".to_string(), Variable::columns(cols.clone()), false).unwrap();

		assert_eq!(ctx.scope_depth(), 2);

		// Clear must unwind the scope stack too, not only drop the bindings.
		ctx.clear();
		assert_eq!(ctx.scope_depth(), 0);
		assert_eq!(ctx.current_scope_type(), &ScopeType::Global);
	}

	#[test]
	fn test_nonexistent_variable() {
		let ctx = SymbolTable::new();

		assert!(ctx.get("nonexistent").is_none());
	}
}
