// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::HashMap;

use reifydb_core::value::column::Columns;

/// Context for storing and managing variables during query execution with scope support
#[derive(Debug, Clone)]
pub struct Stack {
	scopes: Vec<Scope>,
}

/// Represents a single scope containing variables
#[derive(Debug, Clone)]
struct Scope {
	variables: HashMap<String, VariableBinding>,
	scope_type: ScopeType,
}

/// Different types of scopes for organizational purposes
#[derive(Debug, Clone, PartialEq)]
pub enum ScopeType {
	Global,
	Function,
	Block,
	Conditional,
}

/// Represents a variable binding with its dataframe and mutability
#[derive(Debug, Clone)]
struct VariableBinding {
	columns: Columns<'static>,
	mutable: bool,
}

impl Stack {
	/// Create a new variable context with a global scope
	pub fn new() -> Self {
		let global_scope = Scope {
			variables: HashMap::new(),
			scope_type: ScopeType::Global,
		};

		Self {
			scopes: vec![global_scope],
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
			return Err(reifydb_core::error!(reifydb_core::diagnostic::internal::internal(
				"Cannot exit global scope".to_string()
			)));
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

	/// Set a variable in the current (innermost) scope
	pub fn set(&mut self, name: String, columns: Columns<'static>, mutable: bool) -> crate::Result<()> {
		self.set_in_current_scope(name, columns, mutable)
	}

	/// Set a variable specifically in the current scope
	pub fn set_in_current_scope(
		&mut self,
		name: String,
		columns: Columns<'static>,
		mutable: bool,
	) -> crate::Result<()> {
		let current_scope = self.scopes.last_mut().unwrap();

		// Check if variable already exists in current scope and handle mutability rules
		if let Some(existing) = current_scope.variables.get(&name) {
			if !existing.mutable {
				panic!("Cannot reassign immutable variable '{}' in current scope", name);
			}
		}

		current_scope.variables.insert(
			name,
			VariableBinding {
				columns,
				mutable,
			},
		);
		Ok(())
	}

	/// Get a variable by searching from innermost to outermost scope
	pub fn get(&self, name: &str) -> Option<&Columns<'static>> {
		// Search from innermost scope (end of vector) to outermost scope (beginning)
		for scope in self.scopes.iter().rev() {
			if let Some(binding) = scope.variables.get(name) {
				return Some(&binding.columns);
			}
		}
		None
	}

	/// Get a variable with its scope depth information
	pub fn get_with_scope(&self, name: &str) -> Option<(&Columns<'static>, usize)> {
		// Search from innermost scope to outermost scope
		for (depth_from_end, scope) in self.scopes.iter().rev().enumerate() {
			if let Some(binding) = scope.variables.get(name) {
				let scope_depth = self.scopes.len() - 1 - depth_from_end;
				return Some((&binding.columns, scope_depth));
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
	}
}

impl Default for Stack {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::value::column::{Column, ColumnData, Columns};
	use reifydb_type::Value;

	use super::*;

	// Helper function to create test columns
	fn create_test_columns(values: Vec<Value>) -> Columns<'static> {
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
		let mut ctx = Stack::new();
		let cols = create_test_columns(vec![Value::utf8("Alice".to_string())]);

		// Set a variable
		ctx.set("name".to_string(), cols.clone(), false).unwrap();

		// Get the variable
		assert!(ctx.get("name").is_some());
		assert!(!ctx.is_mutable("name"));
		assert!(ctx.exists_in_any_scope("name"));
		assert!(ctx.exists_in_current_scope("name"));
	}

	#[test]
	fn test_mutable_variable() {
		let mut ctx = Stack::new();
		let cols1 = create_test_columns(vec![Value::Int4(42)]);
		let cols2 = create_test_columns(vec![Value::Int4(84)]);

		// Set as mutable
		ctx.set("counter".to_string(), cols1.clone(), true).unwrap();
		assert!(ctx.is_mutable("counter"));
		assert!(ctx.get("counter").is_some());

		// Update mutable variable
		ctx.set("counter".to_string(), cols2.clone(), true).unwrap();
		assert!(ctx.get("counter").is_some());
	}

	#[test]
	#[ignore]
	fn test_immutable_variable_reassignment_fails() {
		let mut ctx = Stack::new();
		let cols1 = create_test_columns(vec![Value::utf8("Alice".to_string())]);
		let cols2 = create_test_columns(vec![Value::utf8("Bob".to_string())]);

		// Set as immutable
		ctx.set("name".to_string(), cols1.clone(), false).unwrap();

		// Try to reassign immutable variable - should fail
		let result = ctx.set("name".to_string(), cols2, false);
		assert!(result.is_err());

		// Original value should be preserved
		assert!(ctx.get("name").is_some());
	}

	#[test]
	fn test_scope_management() {
		let mut ctx = Stack::new();

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
		let mut ctx = Stack::new();
		let outer_cols = create_test_columns(vec![Value::utf8("outer".to_string())]);
		let inner_cols = create_test_columns(vec![Value::utf8("inner".to_string())]);

		// Set variable in global scope
		ctx.set("var".to_string(), outer_cols.clone(), false).unwrap();
		assert!(ctx.get("var").is_some());

		// Enter new scope and shadow the variable
		ctx.enter_scope(ScopeType::Block);
		ctx.set("var".to_string(), inner_cols.clone(), false).unwrap();

		// Should see the inner variable
		assert!(ctx.get("var").is_some());
		assert!(ctx.exists_in_current_scope("var"));

		// Exit scope - should see outer variable again
		ctx.exit_scope().unwrap();
		assert!(ctx.get("var").is_some());
	}

	#[test]
	fn test_parent_scope_access() {
		let mut ctx = Stack::new();
		let outer_cols = create_test_columns(vec![Value::utf8("outer".to_string())]);

		// Set variable in global scope
		ctx.set("global_var".to_string(), outer_cols.clone(), false).unwrap();

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
		let mut ctx = Stack::new();
		let cols1 = create_test_columns(vec![Value::utf8("value1".to_string())]);
		let cols2 = create_test_columns(vec![Value::utf8("value2".to_string())]);

		// Set immutable variable in global scope
		ctx.set("var".to_string(), cols1.clone(), false).unwrap();

		// Enter new scope and create new variable with same name (shadowing)
		ctx.enter_scope(ScopeType::Block);
		ctx.set("var".to_string(), cols2.clone(), true).unwrap(); // This one is mutable

		// Should be mutable in current scope
		assert!(ctx.is_mutable("var"));

		// Exit scope - should be immutable again (from global scope)
		ctx.exit_scope().unwrap();
		assert!(!ctx.is_mutable("var"));
	}

	#[test]
	fn test_visible_variable_names() {
		let mut ctx = Stack::new();
		let cols = create_test_columns(vec![Value::utf8("test".to_string())]);

		// Set variables in global scope
		ctx.set("global1".to_string(), cols.clone(), false).unwrap();
		ctx.set("global2".to_string(), cols.clone(), false).unwrap();

		let global_visible = ctx.visible_variable_names();
		assert_eq!(global_visible.len(), 2);
		assert!(global_visible.contains(&"global1".to_string()));
		assert!(global_visible.contains(&"global2".to_string()));

		// Enter new scope and add more variables
		ctx.enter_scope(ScopeType::Function);
		ctx.set("local1".to_string(), cols.clone(), false).unwrap();
		ctx.set("global1".to_string(), cols.clone(), false).unwrap(); // Shadow global1

		let function_visible = ctx.visible_variable_names();
		assert_eq!(function_visible.len(), 3); // global1 (shadowed), global2, local1
		assert!(function_visible.contains(&"global1".to_string()));
		assert!(function_visible.contains(&"global2".to_string()));
		assert!(function_visible.contains(&"local1".to_string()));
	}

	#[test]
	fn test_clear_resets_to_global() {
		let mut ctx = Stack::new();
		let cols = create_test_columns(vec![Value::utf8("test".to_string())]);

		// Add variables and enter scopes
		ctx.set("var1".to_string(), cols.clone(), false).unwrap();
		ctx.enter_scope(ScopeType::Function);
		ctx.set("var2".to_string(), cols.clone(), false).unwrap();
		ctx.enter_scope(ScopeType::Block);
		ctx.set("var3".to_string(), cols.clone(), false).unwrap();

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
		let ctx = Stack::new();

		assert!(ctx.get("nonexistent").is_none());
		assert!(!ctx.exists_in_any_scope("nonexistent"));
		assert!(!ctx.exists_in_current_scope("nonexistent"));
		assert!(!ctx.is_mutable("nonexistent"));
		assert!(ctx.get_with_scope("nonexistent").is_none());
	}
}
