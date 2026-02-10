// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use diagnostic::runtime::{variable_is_immutable, variable_not_found};
use reifydb_core::{internal, value::column::columns::Columns};
use reifydb_rql::instruction::{CompiledFunctionDef, ScopeType};
use reifydb_type::{
	error,
	error::{Error, diagnostic},
	value::Value,
};

/// The VM data stack for intermediate results
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

	pub fn pop(&mut self) -> crate::Result<Variable> {
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

/// A variable can be either a scalar value, columnar data, or a FOR loop iterator
#[derive(Debug, Clone)]
pub enum Variable {
	/// A scalar value that can be used directly in expressions
	Scalar(Value),
	/// Columnar data that requires explicit conversion to scalar
	Columns(Columns),
	/// A FOR loop iterator tracking position in a result set
	ForIterator {
		columns: Columns,
		index: usize,
	},
}

impl Variable {
	/// Create a scalar variable
	pub fn scalar(value: Value) -> Self {
		Variable::Scalar(value)
	}

	/// Create a columns variable
	pub fn columns(columns: Columns) -> Self {
		Variable::Columns(columns)
	}

	/// Get the scalar value if this is a scalar variable
	pub fn as_scalar(&self) -> Option<&Value> {
		match self {
			Variable::Scalar(value) => Some(value),
			_ => None,
		}
	}

	/// Get the columns if this is a columns variable
	pub fn as_columns(&self) -> Option<&Columns> {
		match self {
			Variable::Columns(columns) => Some(columns),
			_ => None,
		}
	}
}

/// Context for storing and managing variables during query execution with scope support
#[derive(Debug, Clone)]
pub struct SymbolTable {
	scopes: Vec<Scope>,
	/// User-defined functions (pre-compiled)
	functions: HashMap<String, CompiledFunctionDef>,
}

/// Represents a single scope containing variables
#[derive(Debug, Clone)]
struct Scope {
	variables: HashMap<String, VariableBinding>,
	scope_type: ScopeType,
}

/// Control flow signal for loop and function constructs
#[derive(Debug, Clone)]
pub enum ControlFlow {
	Normal,
	Break,
	Continue,
	Return(Option<Value>),
}

impl ControlFlow {
	pub fn is_normal(&self) -> bool {
		matches!(self, ControlFlow::Normal)
	}
}

/// Represents a variable binding with its value and mutability
#[derive(Debug, Clone)]
struct VariableBinding {
	variable: Variable,
	mutable: bool,
}

impl SymbolTable {
	/// Create a new variable context with a global scope
	pub fn new() -> Self {
		let global_scope = Scope {
			variables: HashMap::new(),
			scope_type: ScopeType::Global,
		};

		Self {
			scopes: vec![global_scope],
			functions: HashMap::new(),
		}
	}

	/// Enter a new scope (push onto stack)
	pub fn enter_scope(&mut self, scope_type: ScopeType) {
		let new_scope = Scope {
			variables: HashMap::new(),
			scope_type,
		};
		self.scopes.push(new_scope);
	}

	/// Exit the current scope (pop from stack)
	/// Returns error if trying to exit the global scope
	pub fn exit_scope(&mut self) -> crate::Result<()> {
		if self.scopes.len() <= 1 {
			return Err(error!(internal!("Cannot exit global scope")));
		}
		self.scopes.pop();
		Ok(())
	}

	/// Get the current scope depth (0 = global scope)
	pub fn scope_depth(&self) -> usize {
		self.scopes.len() - 1
	}

	/// Get the type of the current scope
	pub fn current_scope_type(&self) -> &ScopeType {
		&self.scopes.last().unwrap().scope_type
	}

	/// Set a variable in the current (innermost) scope (allows shadowing)
	pub fn set(&mut self, name: String, variable: Variable, mutable: bool) -> crate::Result<()> {
		self.set_in_current_scope(name, variable, mutable)
	}

	/// Reassign an existing variable (checks mutability)
	/// Searches from innermost to outermost scope to find the variable
	pub fn reassign(&mut self, name: String, variable: Variable) -> crate::Result<()> {
		// Search from innermost scope to outermost scope
		for scope in self.scopes.iter_mut().rev() {
			if let Some(existing) = scope.variables.get(&name) {
				if !existing.mutable {
					return Err(Error(variable_is_immutable(&name)));
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

		Err(Error(variable_not_found(&name)))
	}

	/// Set a variable specifically in the current scope
	/// Allows shadowing - new variable declarations can shadow existing ones
	pub fn set_in_current_scope(&mut self, name: String, variable: Variable, mutable: bool) -> crate::Result<()> {
		let current_scope = self.scopes.last_mut().unwrap();

		// Allow shadowing - simply insert the new variable binding
		current_scope.variables.insert(
			name,
			VariableBinding {
				variable,
				mutable,
			},
		);
		Ok(())
	}

	/// Get a variable by searching from innermost to outermost scope
	pub fn get(&self, name: &str) -> Option<&Variable> {
		// Search from innermost scope (end of vector) to outermost scope (beginning)
		for scope in self.scopes.iter().rev() {
			if let Some(binding) = scope.variables.get(name) {
				return Some(&binding.variable);
			}
		}
		None
	}

	/// Get a variable with its scope depth information
	pub fn get_with_scope(&self, name: &str) -> Option<(&Variable, usize)> {
		// Search from innermost scope to outermost scope
		for (depth_from_end, scope) in self.scopes.iter().rev().enumerate() {
			if let Some(binding) = scope.variables.get(name) {
				let scope_depth = self.scopes.len() - 1 - depth_from_end;
				return Some((&binding.variable, scope_depth));
			}
		}
		None
	}

	/// Check if a variable exists in the current scope only
	pub fn exists_in_current_scope(&self, name: &str) -> bool {
		self.scopes.last().unwrap().variables.contains_key(name)
	}

	/// Check if a variable exists in any scope (searches all scopes)
	pub fn exists_in_any_scope(&self, name: &str) -> bool {
		self.get(name).is_some()
	}

	/// Check if a variable is mutable (searches from innermost scope)
	pub fn is_mutable(&self, name: &str) -> bool {
		for scope in self.scopes.iter().rev() {
			if let Some(binding) = scope.variables.get(name) {
				return binding.mutable;
			}
		}
		false
	}

	/// Get all variable names from all scopes (for debugging)
	pub fn all_variable_names(&self) -> Vec<String> {
		let mut names = Vec::new();
		for (scope_idx, scope) in self.scopes.iter().enumerate() {
			for name in scope.variables.keys() {
				names.push(format!("{}@scope{}", name, scope_idx));
			}
		}
		names
	}

	/// Get variable names visible in current scope (respects shadowing)
	pub fn visible_variable_names(&self) -> Vec<String> {
		let mut visible = HashMap::new();

		// Process scopes from outermost to innermost so inner scopes override outer ones
		for scope in &self.scopes {
			for name in scope.variables.keys() {
				visible.insert(name.clone(), ());
			}
		}

		visible.keys().cloned().collect()
	}

	/// Clear all variables in all scopes (reset to just global scope)
	pub fn clear(&mut self) {
		self.scopes.clear();
		self.scopes.push(Scope {
			variables: HashMap::new(),
			scope_type: ScopeType::Global,
		});
		self.functions.clear();
	}

	/// Define a user-defined function (pre-compiled)
	pub fn define_function(&mut self, name: String, func: CompiledFunctionDef) {
		self.functions.insert(name, func);
	}

	/// Get a user-defined function by name
	pub fn get_function(&self, name: &str) -> Option<&CompiledFunctionDef> {
		self.functions.get(name)
	}

	/// Check if a function exists
	pub fn function_exists(&self, name: &str) -> bool {
		self.functions.contains_key(name)
	}
}

impl Default for SymbolTable {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::value::column::{Column, data::ColumnData};
	use reifydb_type::value::Value;

	use super::*;

	// Helper function to create test columns
	fn create_test_columns(values: Vec<Value>) -> Columns {
		if values.is_empty() {
			let column_data = ColumnData::undefined(0);
			let column = Column::new("test_col", column_data);
			return Columns::new(vec![column]);
		}

		let mut column_data = ColumnData::undefined(0);
		for value in values {
			column_data.push_value(value);
		}

		let column = Column::new("test_col", column_data);
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
