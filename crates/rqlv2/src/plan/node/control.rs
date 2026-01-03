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

/// For loop iterable value.
#[derive(Debug, Clone, Copy)]
pub enum ForIterableValue<'bump> {
	/// Single expression to iterate (e.g., array, range)
	Expression(&'bump PlanExpr<'bump>),
	/// Pipeline result to iterate (e.g., from table | filter)
	Plan(&'bump [&'bump Plan<'bump>]),
}

/// For loop.
#[derive(Debug, Clone, Copy)]
pub struct ForNode<'bump> {
	pub variable: &'bump Variable<'bump>,
	pub iterable: ForIterableValue<'bump>,
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

/// Expression node - evaluates expression and produces a value.
///
/// In an expression-oriented language, every construct produces a value.
/// This node wraps a PlanExpr for use in statement/plan contexts.
#[derive(Debug, Clone, Copy)]
pub struct ExprNode<'bump> {
	pub expr: &'bump PlanExpr<'bump>,
	pub span: Span,
}
