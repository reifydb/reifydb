// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{
	internal,
	value::column::{ColumnWithName, columns::Columns},
};
use reifydb_rql::instruction::{CompiledClosure, CompiledFunction, ScopeType};
use reifydb_type::{error, fragment::Fragment, value::Value};

use crate::{Result, error::EngineError};

#[derive(Debug, Clone)]
pub struct Stack {
	variables: Vec<Variable>,
}

impl Stack {
	pub fn new() -> Self {
		Self {
			variables: Vec::new(),
		}
	}

	pub fn push(&mut self, value: Variable) {
		self.variables.push(value);
	}

	pub fn pop(&mut self) -> Result<Variable> {
		self.variables.pop().ok_or_else(|| error!(internal!("VM data stack underflow")))
	}

	pub fn peek(&self) -> Option<&Variable> {
		self.variables.last()
	}

	pub fn is_empty(&self) -> bool {
		self.variables.is_empty()
	}

	pub fn len(&self) -> usize {
		self.variables.len()
	}
}

impl Default for Stack {
	fn default() -> Self {
		Self::new()
	}
}

#[derive(Debug, Clone)]
pub struct ClosureValue {
	pub def: CompiledClosure,
	pub captured: HashMap<String, Variable>,
}

#[derive(Debug, Clone)]
pub enum Variable {
	Columns {
		columns: Columns,
	},

	ForIterator {
		columns: Columns,
		index: usize,
	},

	Closure(ClosureValue),
}

impl Variable {
	pub fn scalar(value: Value) -> Self {
		Variable::Columns {
			columns: Columns::single_row([("value", value)]),
		}
	}

	pub fn scalar_named(name: &str, value: Value) -> Self {
		let mut columns = Columns::single_row([("value", value)]);
		columns.names.make_mut()[0] = Fragment::internal(name);
		Variable::Columns {
			columns,
		}
	}

	pub fn columns(columns: Columns) -> Self {
		Variable::Columns {
			columns,
		}
	}

	pub fn is_scalar(&self) -> bool {
		matches!(
			self,
			Variable::Columns { columns } if columns.is_scalar()
		)
	}

	pub fn as_columns(&self) -> Option<&Columns> {
		match self {
			Variable::Columns {
				columns,
				..
			}
			| Variable::ForIterator {
				columns,
				..
			} => Some(columns),
			Variable::Closure(_) => None,
		}
	}

	pub fn into_column(self) -> Result<ColumnWithName> {
		let cols = match self {
			Variable::Columns {
				columns: c,
				..
			}
			| Variable::ForIterator {
				columns: c,
				..
			} => c,
			Variable::Closure(_) => Columns::single_row([("value", Value::none())]),
		};
		let actual = cols.len();
		if actual == 1 {
			let name = cols.names.into_inner().into_iter().next().unwrap();
			let data = cols.columns.into_inner().into_iter().next().unwrap();
			Ok(ColumnWithName::new(name, data))
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
pub enum ControlFlow {
	Normal,
	Break,
	Continue,
	Return(Option<Columns>),
}

impl ControlFlow {
	pub fn is_normal(&self) -> bool {
		matches!(self, ControlFlow::Normal)
	}
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
					return Err(EngineError::VariableIsImmutable {
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

		Err(EngineError::VariableNotFound {
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

	pub fn get_with_scope(&self, name: &str) -> Option<(&Variable, usize)> {
		for (depth_from_end, scope) in self.inner.scopes.iter().rev().enumerate() {
			if let Some(binding) = scope.variables.get(name) {
				let scope_depth = self.inner.scopes.len() - 1 - depth_from_end;
				return Some((&binding.variable, scope_depth));
			}
		}
		None
	}

	pub fn exists_in_current_scope(&self, name: &str) -> bool {
		self.inner.scopes.last().unwrap().variables.contains_key(name)
	}

	pub fn exists_in_any_scope(&self, name: &str) -> bool {
		self.get(name).is_some()
	}

	pub fn is_mutable(&self, name: &str) -> bool {
		for scope in self.inner.scopes.iter().rev() {
			if let Some(binding) = scope.variables.get(name) {
				return binding.mutable;
			}
		}
		false
	}

	pub fn all_variable_names(&self) -> Vec<String> {
		let mut names = Vec::new();
		for (scope_idx, scope) in self.inner.scopes.iter().enumerate() {
			for name in scope.variables.keys() {
				names.push(format!("{}@scope{}", name, scope_idx));
			}
		}
		names
	}

	pub fn visible_variable_names(&self) -> Vec<String> {
		let mut visible = HashMap::new();

		for scope in &self.inner.scopes {
			for name in scope.variables.keys() {
				visible.insert(name.clone(), ());
			}
		}

		visible.keys().cloned().collect()
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

	pub fn function_exists(&self, name: &str) -> bool {
		self.inner.functions.contains_key(name)
	}
}

impl Default for SymbolTable {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer};
	use reifydb_type::value::{Value, r#type::Type};

	use super::*;

	// Helper function to create test columns
	fn create_test_columns(values: Vec<Value>) -> Columns {
		if values.is_empty() {
			let column_data = ColumnBuffer::none_typed(Type::Boolean, 0);
			let column = ColumnWithName::new("test_col", column_data);
			return Columns::new(vec![column]);
		}

		let mut column_data = ColumnBuffer::none_typed(Type::Boolean, 0);
		for value in values {
			column_data.push_value(value);
		}

		let column = ColumnWithName::new("test_col", column_data);
		Columns::new(vec![column])
	}

	#[test]
	fn test_basic_variable_operations() {
		let mut ctx = SymbolTable::new();
		let cols = create_test_columns(vec![Value::utf8("Alice".to_string())]);

		// Set a variable
		ctx.set("name".to_string(), Variable::columns(cols.clone()), false).unwrap();

		// Get the variable
		assert!(ctx.get("name").is_some());
		assert!(!ctx.is_mutable("name"));
		assert!(ctx.exists_in_any_scope("name"));
		assert!(ctx.exists_in_current_scope("name"));
	}

	#[test]
	fn test_mutable_variable() {
		let mut ctx = SymbolTable::new();
		let cols1 = create_test_columns(vec![Value::Int4(42)]);
		let cols2 = create_test_columns(vec![Value::Int4(84)]);

		// Set as mutable
		ctx.set("counter".to_string(), Variable::columns(cols1.clone()), true).unwrap();
		assert!(ctx.is_mutable("counter"));
		assert!(ctx.get("counter").is_some());

		// Update mutable variable
		ctx.set("counter".to_string(), Variable::columns(cols2.clone()), true).unwrap();
		assert!(ctx.get("counter").is_some());
	}

	#[test]
	#[ignore]
	fn test_immutable_variable_reassignment_fails() {
		let mut ctx = SymbolTable::new();
		let cols1 = create_test_columns(vec![Value::utf8("Alice".to_string())]);
		let cols2 = create_test_columns(vec![Value::utf8("Bob".to_string())]);

		// Set as immutable
		ctx.set("name".to_string(), Variable::columns(cols1.clone()), false).unwrap();

		// Try to reassign immutable variable - should fail
		let result = ctx.set("name".to_string(), Variable::columns(cols2), false);
		assert!(result.is_err());

		// Original value should be preserved
		assert!(ctx.get("name").is_some());
	}

	#[test]
	fn test_scope_management() {
		let mut ctx = SymbolTable::new();

		// Initially in global scope
		assert_eq!(ctx.scope_depth(), 0);
		assert_eq!(ctx.current_scope_type(), &ScopeType::Global);

		// Enter a function scope
		ctx.enter_scope(ScopeType::Function);
		assert_eq!(ctx.scope_depth(), 1);
		assert_eq!(ctx.current_scope_type(), &ScopeType::Function);

		// Enter a block scope
		ctx.enter_scope(ScopeType::Block);
		assert_eq!(ctx.scope_depth(), 2);
		assert_eq!(ctx.current_scope_type(), &ScopeType::Block);

		// Exit block scope
		ctx.exit_scope().unwrap();
		assert_eq!(ctx.scope_depth(), 1);
		assert_eq!(ctx.current_scope_type(), &ScopeType::Function);

		// Exit function scope
		ctx.exit_scope().unwrap();
		assert_eq!(ctx.scope_depth(), 0);
		assert_eq!(ctx.current_scope_type(), &ScopeType::Global);

		// Cannot exit global scope
		assert!(ctx.exit_scope().is_err());
	}

	#[test]
	fn test_variable_shadowing() {
		let mut ctx = SymbolTable::new();
		let outer_cols = create_test_columns(vec![Value::utf8("outer".to_string())]);
		let inner_cols = create_test_columns(vec![Value::utf8("inner".to_string())]);

		// Set variable in global scope
		ctx.set("var".to_string(), Variable::columns(outer_cols.clone()), false).unwrap();
		assert!(ctx.get("var").is_some());

		// Enter new scope and shadow the variable
		ctx.enter_scope(ScopeType::Block);
		ctx.set("var".to_string(), Variable::columns(inner_cols.clone()), false).unwrap();

		// Should see the inner variable
		assert!(ctx.get("var").is_some());
		assert!(ctx.exists_in_current_scope("var"));

		// Exit scope - should see outer variable again
		ctx.exit_scope().unwrap();
		assert!(ctx.get("var").is_some());
	}

	#[test]
	fn test_parent_scope_access() {
		let mut ctx = SymbolTable::new();
		let outer_cols = create_test_columns(vec![Value::utf8("outer".to_string())]);

		// Set variable in global scope
		ctx.set("global_var".to_string(), Variable::columns(outer_cols.clone()), false).unwrap();

		// Enter new scope
		ctx.enter_scope(ScopeType::Function);

		// Should still be able to access parent scope variable
		assert!(ctx.get("global_var").is_some());
		assert!(!ctx.exists_in_current_scope("global_var"));
		assert!(ctx.exists_in_any_scope("global_var"));

		// Get with scope information
		let (_, scope_depth) = ctx.get_with_scope("global_var").unwrap();
		assert_eq!(scope_depth, 0); // Found in global scope
	}

	#[test]
	fn test_scope_specific_mutability() {
		let mut ctx = SymbolTable::new();
		let cols1 = create_test_columns(vec![Value::utf8("value1".to_string())]);
		let cols2 = create_test_columns(vec![Value::utf8("value2".to_string())]);

		// Set immutable variable in global scope
		ctx.set("var".to_string(), Variable::columns(cols1.clone()), false).unwrap();

		// Enter new scope and create new variable with same name (shadowing)
		ctx.enter_scope(ScopeType::Block);
		ctx.set("var".to_string(), Variable::columns(cols2.clone()), true).unwrap(); // This one is mutable

		// Should be mutable in current scope
		assert!(ctx.is_mutable("var"));

		// Exit scope - should be immutable again (from global scope)
		ctx.exit_scope().unwrap();
		assert!(!ctx.is_mutable("var"));
	}

	#[test]
	fn test_visible_variable_names() {
		let mut ctx = SymbolTable::new();
		let cols = create_test_columns(vec![Value::utf8("test".to_string())]);

		// Set variables in global scope
		ctx.set("global1".to_string(), Variable::columns(cols.clone()), false).unwrap();
		ctx.set("global2".to_string(), Variable::columns(cols.clone()), false).unwrap();

		let global_visible = ctx.visible_variable_names();
		assert_eq!(global_visible.len(), 2);
		assert!(global_visible.contains(&"global1".to_string()));
		assert!(global_visible.contains(&"global2".to_string()));

		// Enter new scope and add more variables
		ctx.enter_scope(ScopeType::Function);
		ctx.set("local1".to_string(), Variable::columns(cols.clone()), false).unwrap();
		ctx.set("global1".to_string(), Variable::columns(cols.clone()), false).unwrap(); // Shadow global1

		let function_visible = ctx.visible_variable_names();
		assert_eq!(function_visible.len(), 3); // global1 (shadowed), global2, local1
		assert!(function_visible.contains(&"global1".to_string()));
		assert!(function_visible.contains(&"global2".to_string()));
		assert!(function_visible.contains(&"local1".to_string()));
	}

	#[test]
	fn test_clear_resets_to_global() {
		let mut ctx = SymbolTable::new();
		let cols = create_test_columns(vec![Value::utf8("test".to_string())]);

		// Add variables and enter scopes
		ctx.set("var1".to_string(), Variable::columns(cols.clone()), false).unwrap();
		ctx.enter_scope(ScopeType::Function);
		ctx.set("var2".to_string(), Variable::columns(cols.clone()), false).unwrap();
		ctx.enter_scope(ScopeType::Block);
		ctx.set("var3".to_string(), Variable::columns(cols.clone()), false).unwrap();

		assert_eq!(ctx.scope_depth(), 2);
		assert_eq!(ctx.visible_variable_names().len(), 3);

		// Clear should reset to global scope with no variables
		ctx.clear();
		assert_eq!(ctx.scope_depth(), 0);
		assert_eq!(ctx.current_scope_type(), &ScopeType::Global);
		assert_eq!(ctx.visible_variable_names().len(), 0);
	}

	#[test]
	fn test_nonexistent_variable() {
		let ctx = SymbolTable::new();

		assert!(ctx.get("nonexistent").is_none());
		assert!(!ctx.exists_in_any_scope("nonexistent"));
		assert!(!ctx.exists_in_current_scope("nonexistent"));
		assert!(!ctx.is_mutable("nonexistent"));
		assert!(ctx.get_with_scope("nonexistent").is_none());
	}
}
