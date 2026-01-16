// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Variable scope management.

use bumpalo::collections::Vec as BumpVec;

use super::core::{PlanError, PlanErrorKind, Planner, Result};
use crate::{
	plan::types::{OutputSchema, Variable},
	token::span::Span,
};

/// Variable scope for tracking local variables.
pub(super) struct Scope<'bump> {
	pub(super) variables: BumpVec<'bump, (&'bump str, u32)>,
}

impl<'bump, 'cat> Planner<'bump, 'cat> {
	/// Push a new variable scope.
	pub(super) fn push_scope(&mut self) {
		self.scopes.push(Scope {
			variables: BumpVec::new_in(self.bump),
		});
	}

	/// Pop the current variable scope.
	pub(super) fn pop_scope(&mut self) {
		self.scopes.pop();
	}

	/// Declare a variable in the current scope.
	pub(super) fn declare_variable(&mut self, name: &'bump str) -> u32 {
		let id = self.next_variable_id;
		self.next_variable_id += 1;
		if let Some(scope) = self.scopes.last_mut() {
			scope.variables.push((name, id));
		}
		id
	}

	/// Resolve a variable from the current scopes.
	pub(super) fn resolve_variable(&self, name: &str, span: Span) -> Result<&'bump Variable<'bump>> {
		for scope in self.scopes.iter().rev() {
			for (var_name, var_id) in scope.variables.iter() {
				if *var_name == name {
					return Ok(self.bump.alloc(Variable {
						name: self.bump.alloc_str(name),
						variable_id: *var_id,
						span,
					}));
				}
			}
		}
		Err(PlanError {
			kind: PlanErrorKind::VariableNotFound(name.to_string()),
			span,
		})
	}

	/// Store a schema for a variable (used for pipeline-valued variables).
	pub(super) fn store_variable_schema(&mut self, variable_id: u32, schema: OutputSchema<'bump>) {
		self.variable_schemas.push((variable_id, schema));
	}

	/// Get the schema for a variable by ID.
	pub(super) fn get_variable_schema(&self, variable_id: u32) -> Option<&OutputSchema<'bump>> {
		for (id, schema) in self.variable_schemas.iter() {
			if *id == variable_id {
				return Some(schema);
			}
		}
		None
	}
}
