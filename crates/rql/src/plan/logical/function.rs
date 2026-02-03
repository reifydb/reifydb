// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{fragment::Fragment, value::constraint::TypeConstraint};

use crate::expression::Expression;

/// A function parameter in the logical plan
#[derive(Debug, Clone)]
pub struct FunctionParameter {
	/// Parameter name (includes $)
	pub name: Fragment,
	/// Optional type constraint
	pub type_constraint: Option<TypeConstraint>,
}

/// Define a user-defined function
#[derive(Debug)]
pub struct DefineFunctionNode {
	/// Function name
	pub name: Fragment,
	/// Function parameters
	pub parameters: Vec<FunctionParameter>,
	/// Optional return type constraint
	pub return_type: Option<TypeConstraint>,
	/// Function body as a list of statement plans
	pub body: Vec<Vec<super::LogicalPlan>>,
}

/// Return statement
#[derive(Debug)]
pub struct ReturnNode {
	/// Optional return value expression
	pub value: Option<Expression>,
}

/// Call a function (built-in or user-defined)
#[derive(Debug)]
pub struct CallFunctionNode {
	/// Function name to call
	pub name: Fragment,
	/// Arguments to pass
	pub arguments: Vec<Expression>,
}
