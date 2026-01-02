// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Scope management for variable bindings.

use std::collections::HashMap;

use super::state::OperandValue;

/// A single scope level.
#[derive(Debug, Clone)]
pub struct Scope {
	/// Variables bound in this scope by name.
	variables: HashMap<String, OperandValue>,
	/// Variables bound in this scope by ID (for RQLv2 bytecode).
	variables_by_id: HashMap<u32, OperandValue>,
}

impl Scope {
	/// Create a new empty scope.
	pub fn new() -> Self {
		Self {
			variables: HashMap::new(),
			variables_by_id: HashMap::new(),
		}
	}

	/// Get a variable from this scope.
	pub fn get(&self, name: &str) -> Option<&OperandValue> {
		self.variables.get(name)
	}

	/// Set a variable in this scope.
	pub fn set(&mut self, name: String, value: OperandValue) {
		self.variables.insert(name, value);
	}

	/// Get a variable from this scope by ID.
	pub fn get_by_id(&self, id: u32) -> Option<&OperandValue> {
		self.variables_by_id.get(&id)
	}

	/// Set a variable in this scope by ID.
	pub fn set_by_id(&mut self, id: u32, value: OperandValue) {
		self.variables_by_id.insert(id, value);
	}

	/// Check if a variable exists in this scope.
	pub fn contains(&self, name: &str) -> bool {
		self.variables.contains_key(name)
	}

	/// Check if a variable ID exists in this scope.
	pub fn contains_id(&self, id: u32) -> bool {
		self.variables_by_id.contains_key(&id)
	}

	/// Iterate over all variables in this scope.
	pub fn iter(&self) -> impl Iterator<Item = (&String, &OperandValue)> {
		self.variables.iter()
	}
}

impl Default for Scope {
	fn default() -> Self {
		Self::new()
	}
}

/// Chain of scopes for variable resolution.
#[derive(Debug)]
pub struct ScopeChain {
	scopes: Vec<Scope>,
}

impl ScopeChain {
	/// Create a new scope chain with global scope.
	pub fn new() -> Self {
		Self {
			scopes: vec![Scope::new()], // Global scope
		}
	}

	/// Push a new scope.
	pub fn push(&mut self) {
		self.scopes.push(Scope::new());
	}

	/// Pop the current scope.
	/// Returns None if only global scope remains (can't pop global).
	pub fn pop(&mut self) -> Option<Scope> {
		if self.scopes.len() > 1 {
			self.scopes.pop()
		} else {
			None // Don't pop global scope
		}
	}

	/// Look up a variable (searches from innermost to outermost scope).
	pub fn get(&self, name: &str) -> Option<&OperandValue> {
		for scope in self.scopes.iter().rev() {
			if let Some(value) = scope.get(name) {
				return Some(value);
			}
		}
		None
	}

	/// Set a variable in the current (innermost) scope.
	pub fn set(&mut self, name: String, value: OperandValue) {
		if let Some(scope) = self.scopes.last_mut() {
			scope.set(name, value);
		}
	}

	/// Look up a variable by ID (searches from innermost to outermost scope).
	pub fn get_by_id(&self, id: u32) -> Option<&OperandValue> {
		for scope in self.scopes.iter().rev() {
			if let Some(value) = scope.get_by_id(id) {
				return Some(value);
			}
		}
		None
	}

	/// Set a variable by ID in the current (innermost) scope.
	pub fn set_by_id(&mut self, id: u32, value: OperandValue) {
		if let Some(scope) = self.scopes.last_mut() {
			scope.set_by_id(id, value);
		}
	}

	/// Update an existing variable (searches all scopes from inner to outer).
	/// Returns true if variable was found and updated, false otherwise.
	pub fn update(&mut self, name: &str, value: OperandValue) -> bool {
		for scope in self.scopes.iter_mut().rev() {
			if scope.contains(name) {
				scope.set(name.to_string(), value);
				return true;
			}
		}
		false
	}

	/// Update an existing variable by ID (searches all scopes from inner to outer).
	/// Returns true if variable was found and updated, false otherwise.
	pub fn update_by_id(&mut self, id: u32, value: OperandValue) -> bool {
		for scope in self.scopes.iter_mut().rev() {
			if scope.contains_id(id) {
				scope.set_by_id(id, value);
				return true;
			}
		}
		false
	}

	/// Set a variable in the global scope.
	pub fn set_global(&mut self, name: String, value: OperandValue) {
		if let Some(scope) = self.scopes.first_mut() {
			scope.set(name, value);
		}
	}

	/// Current scope depth.
	pub fn depth(&self) -> usize {
		self.scopes.len()
	}

	/// Pop scopes until we reach the target depth.
	pub fn pop_to_depth(&mut self, target_depth: usize) {
		while self.scopes.len() > target_depth {
			self.pop();
		}
	}

	/// Iterate over all scopes from innermost to outermost.
	pub fn iter(&self) -> impl Iterator<Item = &Scope> {
		self.scopes.iter().rev()
	}
}

impl Default for ScopeChain {
	fn default() -> Self {
		Self::new()
	}
}
