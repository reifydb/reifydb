// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::constraint::TypeConstraint;

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
pub struct ReturnNode {
	pub value: Option<Expression>,
}

#[derive(Debug)]
pub struct CallFunctionNode<'bump> {
	pub name: BumpFragment<'bump>,

	pub arguments: Vec<Expression>,

	pub is_procedure_call: bool,
}
