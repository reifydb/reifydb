// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::{self, Display, Formatter};

use reifydb_value::value::constraint::TypeConstraint;

use super::LogicalPlan;
use crate::{
	bump::{BumpFragment, BumpVec},
	expression::Expression,
};

#[derive(Debug, Clone)]
pub struct FunctionParameter<'bump> {
	pub name: BumpFragment<'bump>,

	pub type_constraint: Option<TypeConstraint>,
}

#[derive(Debug)]
pub struct DefineFunctionNode<'bump> {
	pub name: BumpFragment<'bump>,

	pub parameters: Vec<FunctionParameter<'bump>>,

	pub return_type: Option<TypeConstraint>,

	pub body: Vec<BumpVec<'bump, LogicalPlan<'bump>>>,
}

#[derive(Debug)]
pub enum ReturnValue<'bump> {
	Expression(Expression),
	Statement(BumpVec<'bump, LogicalPlan<'bump>>),
}

impl<'bump> Display for ReturnValue<'bump> {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			ReturnValue::Expression(expr) => write!(f, "{}", expr),
			ReturnValue::Statement(plans) => write!(f, "Statement({} plans)", plans.len()),
		}
	}
}

#[derive(Debug)]
pub struct ReturnNode<'bump> {
	pub value: Option<ReturnValue<'bump>>,
}

#[derive(Debug)]
pub struct CallFunctionNode<'bump> {
	pub name: BumpFragment<'bump>,

	pub arguments: Vec<Expression>,

	pub is_procedure_call: bool,
}
