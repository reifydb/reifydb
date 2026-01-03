// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Control flow plan nodes.

use crate::{
	plan::{Plan, Variable, node::expr::PlanExpr},
	token::Span,
};

/// Conditional (if/else).
#[derive(Debug, Clone, Copy)]
pub struct ConditionalNode<'bump> {
	pub condition: &'bump PlanExpr<'bump>,
	pub then_branch: &'bump [&'bump Plan<'bump>],
	pub else_ifs: &'bump [ElseIfBranch<'bump>],
	pub else_branch: Option<&'bump [&'bump Plan<'bump>]>,
	pub span: Span,
}

/// Else-if branch.
#[derive(Debug, Clone, Copy)]
pub struct ElseIfBranch<'bump> {
	pub condition: &'bump PlanExpr<'bump>,
	pub body: &'bump [&'bump Plan<'bump>],
}

/// Loop statement.
#[derive(Debug, Clone, Copy)]
pub struct LoopNode<'bump> {
	pub body: &'bump [&'bump Plan<'bump>],
	pub span: Span,
}

/// For loop.
#[derive(Debug, Clone, Copy)]
pub struct ForNode<'bump> {
	pub variable: &'bump Variable<'bump>,
	pub iterable: &'bump PlanExpr<'bump>,
	pub body: &'bump [&'bump Plan<'bump>],
	pub span: Span,
}

/// Variable declaration (let).
#[derive(Debug, Clone, Copy)]
pub struct DeclareNode<'bump> {
	pub variable: &'bump Variable<'bump>,
	pub value: DeclareValue<'bump>,
	pub span: Span,
}

/// Value for variable declaration.
#[derive(Debug, Clone, Copy)]
pub enum DeclareValue<'bump> {
	Expression(&'bump PlanExpr<'bump>),
	Plan(&'bump [&'bump Plan<'bump>]),
}

/// Variable assignment.
#[derive(Debug, Clone, Copy)]
pub struct AssignNode<'bump> {
	pub variable: &'bump Variable<'bump>,
	pub value: DeclareValue<'bump>,
	pub span: Span,
}

/// Return statement.
#[derive(Debug, Clone, Copy)]
pub struct ReturnNode<'bump> {
	pub value: Option<&'bump PlanExpr<'bump>>,
	pub span: Span,
}

/// Break from loop.
#[derive(Debug, Clone, Copy)]
pub struct BreakNode {
	pub span: Span,
}

/// Continue to next iteration.
#[derive(Debug, Clone, Copy)]
pub struct ContinueNode {
	pub span: Span,
}

/// Script function definition (fn name() { body }).
#[derive(Debug, Clone, Copy)]
pub struct DefineScriptFunctionNode<'bump> {
	pub name: &'bump str,
	pub body: &'bump [&'bump Plan<'bump>],
	pub span: Span,
}

/// Script function call.
#[derive(Debug, Clone, Copy)]
pub struct CallScriptFunctionNode<'bump> {
	pub name: &'bump str,
	pub span: Span,
}
