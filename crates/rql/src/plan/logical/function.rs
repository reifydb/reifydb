// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::constraint::TypeConstraint;

use crate::{
	bump::{BumpFragment, BumpVec},
	expression::Expression,
};

/// A function parameter in the logical plan
#[derive(Debug, Clone)]
pub struct FunctionParameter<'bump> {
	/// Parameter name (includes $)
	pub name: BumpFragment<'bump>,
	/// Optional type constraint
	pub type_constraint: Option<TypeConstraint>,
}

/// Define a user-defined function
#[derive(Debug)]
pub struct DefineFunctionNode<'bump> {
	/// Function name
	pub name: BumpFragment<'bump>,
	/// Function parameters
	pub parameters: Vec<FunctionParameter<'bump>>,
	/// Optional return type constraint
	pub return_type: Option<TypeConstraint>,
	/// Function body as a list of statement plans
	pub body: Vec<BumpVec<'bump, super::LogicalPlan<'bump>>>,
}

/// Return statement
#[derive(Debug)]
pub struct ReturnNode {
	/// Optional return value expression
	pub value: Option<Expression>,
}

/// Call a function (built-in or user-defined)
#[derive(Debug)]
pub struct CallFunctionNode<'bump> {
	/// Function name to call
	pub name: BumpFragment<'bump>,
	/// Arguments to pass
	pub arguments: Vec<Expression>,
	/// Whether this was invoked via CALL (procedure) vs bare function call
	pub is_procedure_call: bool,
}
