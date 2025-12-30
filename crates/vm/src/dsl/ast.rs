// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::token::Span;
use crate::expr::{BinaryOp, UnaryOp};

/// A complete DSL program - a list of statements.
#[derive(Debug, Clone)]
pub struct DslAst {
	pub statements: Vec<StatementAst>,
	pub span: Span,
}

impl DslAst {
	/// Create a new DSL AST with a single pipeline (for backwards compatibility).
	pub fn from_pipeline(pipeline: PipelineAst, span: Span) -> Self {
		Self {
			statements: vec![StatementAst::Pipeline(pipeline)],
			span,
		}
	}
}

/// Top-level statement.
#[derive(Debug, Clone)]
pub enum StatementAst {
	/// Pipeline expression.
	Pipeline(PipelineAst),

	/// Let binding: let name = expr
	Let(LetAst),

	/// Function definition: def name(params) { body }
	Def(DefAst),

	/// If expression: if cond { then } else { else }
	If(IfAst),

	/// Function call: name(args)
	Call(CallAst),

	/// Loop statement: loop { body }
	Loop(LoopAst),

	/// Break statement: break
	Break(BreakAst),

	/// Continue statement: continue
	Continue(ContinueAst),

	/// For loop: for $var in pipeline { body }
	For(ForAst),

	/// Module function call: module::function(args)
	ModuleCall(ModuleCallAst),

	/// Assignment: $var = expr (updates existing variable)
	Assign(AssignAst),

	/// Expression statement - leaves value on stack for implicit return
	Expression(ExpressionAst),
}

impl StatementAst {
	pub fn span(&self) -> Span {
		match self {
			StatementAst::Pipeline(p) => p.span,
			StatementAst::Let(l) => l.span,
			StatementAst::Def(d) => d.span,
			StatementAst::If(i) => i.span,
			StatementAst::Call(c) => c.span,
			StatementAst::Loop(l) => l.span,
			StatementAst::Break(b) => b.span,
			StatementAst::Continue(c) => c.span,
			StatementAst::For(f) => f.span,
			StatementAst::ModuleCall(m) => m.span,
			StatementAst::Assign(a) => a.span,
			StatementAst::Expression(e) => e.span,
		}
	}
}

/// Value in a let statement - can be expression or pipeline.
#[derive(Debug, Clone)]
pub enum LetValue {
	/// Scalar expression (e.g., `$user.id`, `42`, `"hello"`)
	Expr(ExprAst),
	/// Pipeline (e.g., `scan users | filter age > 20`)
	Pipeline(Box<PipelineAst>),
}

impl LetValue {
	pub fn span(&self) -> Span {
		match self {
			LetValue::Expr(e) => e.span(),
			LetValue::Pipeline(p) => p.span,
		}
	}
}

/// Let binding: let name = value
#[derive(Debug, Clone)]
pub struct LetAst {
	pub name: String,
	pub value: LetValue,
	pub span: Span,
}

/// Assignment: $var = expr (updates existing variable, doesn't create new binding)
#[derive(Debug, Clone)]
pub struct AssignAst {
	pub name: String,
	pub value: ExprAst,
	pub span: Span,
}

/// Expression statement - leaves value on operand stack for implicit return.
#[derive(Debug, Clone)]
pub struct ExpressionAst {
	pub expr: ExprAst,
	pub span: Span,
}

/// Function definition: def name(params) { body }
#[derive(Debug, Clone)]
pub struct DefAst {
	pub name: String,
	pub parameters: Vec<ParameterAst>,
	pub body: Vec<StatementAst>,
	pub span: Span,
}

/// Function parameter.
#[derive(Debug, Clone)]
pub struct ParameterAst {
	pub name: String,
	pub param_type: Option<String>,
	pub span: Span,
}

/// If expression: if condition { then_branch } else { else_branch }
#[derive(Debug, Clone)]
pub struct IfAst {
	pub condition: ExprAst,
	pub then_branch: Vec<StatementAst>,
	pub else_branch: Option<Vec<StatementAst>>,
	pub span: Span,
}

/// Function call: name(args)
#[derive(Debug, Clone)]
pub struct CallAst {
	pub function_name: String,
	pub arguments: Vec<ExprAst>,
	pub span: Span,
}

/// Loop statement: loop { body }
#[derive(Debug, Clone)]
pub struct LoopAst {
	pub body: Vec<StatementAst>,
	pub span: Span,
}

/// Break statement: break
#[derive(Debug, Clone)]
pub struct BreakAst {
	pub span: Span,
}

/// Continue statement: continue
#[derive(Debug, Clone)]
pub struct ContinueAst {
	pub span: Span,
}

/// For loop: for $var in pipeline { body }
#[derive(Debug, Clone)]
pub struct ForAst {
	pub variable: String,
	pub iterable: Box<StatementAst>,
	pub body: Vec<StatementAst>,
	pub span: Span,
}

/// Module function call: module::function(args)
#[derive(Debug, Clone)]
pub struct ModuleCallAst {
	pub module: String,
	pub function: String,
	pub arguments: Vec<ExprAst>,
	pub span: Span,
}

/// A pipeline is a series of stages connected by |.
#[derive(Debug, Clone)]
pub struct PipelineAst {
	pub stages: Vec<StageAst>,
	pub span: Span,
}

/// A single pipeline stage.
#[derive(Debug, Clone)]
pub enum StageAst {
	Scan(ScanAst),
	Filter(FilterAst),
	Select(SelectAst),
	Take(TakeAst),
	Extend(ExtendAst),
	Sort(SortAst),
}

impl StageAst {
	pub fn span(&self) -> Span {
		match self {
			StageAst::Scan(s) => s.span,
			StageAst::Filter(s) => s.span,
			StageAst::Select(s) => s.span,
			StageAst::Take(s) => s.span,
			StageAst::Extend(s) => s.span,
			StageAst::Sort(s) => s.span,
		}
	}
}

/// Scan stage: `scan <table_name>`.
#[derive(Debug, Clone)]
pub struct ScanAst {
	pub table_name: String,
	pub span: Span,
}

/// Filter stage: `filter <predicate>`.
#[derive(Debug, Clone)]
pub struct FilterAst {
	pub predicate: ExprAst,
	pub span: Span,
}

/// Select stage: `select [col1, col2, ...]`.
#[derive(Debug, Clone)]
pub struct SelectAst {
	pub columns: Vec<String>,
	pub span: Span,
}

/// Take stage: `take <limit>`.
#[derive(Debug, Clone)]
pub struct TakeAst {
	pub limit: u64,
	pub span: Span,
}

/// Extend stage: `extend { name: expr, ... }`.
#[derive(Debug, Clone)]
pub struct ExtendAst {
	pub extensions: Vec<(String, ExprAst)>,
	pub span: Span,
}

/// Sort stage: `sort col1 asc, col2 desc`.
#[derive(Debug, Clone)]
pub struct SortAst {
	pub columns: Vec<(String, SortOrder)>,
	pub span: Span,
}

/// Sort order direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
	Asc,
	Desc,
}

impl Default for SortOrder {
	fn default() -> Self {
		SortOrder::Asc
	}
}

/// Subquery kind for EXISTS/NOT EXISTS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubqueryKind {
	/// Scalar subquery: extracts a single value.
	Scalar,
	/// EXISTS: returns true if subquery has any rows.
	Exists,
	/// NOT EXISTS: returns true if subquery has no rows.
	NotExists,
}

/// Expression AST (used in filter predicates and computed columns).
#[derive(Debug, Clone)]
pub enum ExprAst {
	/// Column reference.
	Column {
		name: String,
		span: Span,
	},

	/// Variable reference ($name).
	Variable {
		name: String,
		span: Span,
	},

	/// Integer literal.
	Int {
		value: i64,
		span: Span,
	},

	/// Float literal.
	Float {
		value: f64,
		span: Span,
	},

	/// String literal.
	String {
		value: String,
		span: Span,
	},

	/// Boolean literal.
	Bool {
		value: bool,
		span: Span,
	},

	/// Null literal.
	Null {
		span: Span,
	},

	/// Binary operations.
	BinaryOp {
		op: BinaryOp,
		left: Box<ExprAst>,
		right: Box<ExprAst>,
		span: Span,
	},

	/// Unary operations.
	UnaryOp {
		op: UnaryOp,
		operand: Box<ExprAst>,
		span: Span,
	},

	/// Parenthesized expression.
	Paren {
		inner: Box<ExprAst>,
		span: Span,
	},

	/// Function call in expression context.
	Call {
		function_name: String,
		arguments: Vec<ExprAst>,
		span: Span,
	},

	/// Field access: expr.field
	FieldAccess {
		object: Box<ExprAst>,
		field: String,
		span: Span,
	},

	/// Subquery expression: (scan t | ...) or exists(...) or not exists(...)
	Subquery {
		kind: SubqueryKind,
		pipeline: Box<PipelineAst>,
		span: Span,
	},

	/// IN with inline list: expr in (val1, val2, ...)
	InList {
		expr: Box<ExprAst>,
		values: Vec<ExprAst>,
		negated: bool,
		span: Span,
	},

	/// IN with subquery: expr in (scan t | select [col])
	InSubquery {
		expr: Box<ExprAst>,
		pipeline: Box<PipelineAst>,
		negated: bool,
		span: Span,
	},
}

impl ExprAst {
	pub fn span(&self) -> Span {
		match self {
			ExprAst::Column {
				span,
				..
			} => *span,
			ExprAst::Variable {
				span,
				..
			} => *span,
			ExprAst::Int {
				span,
				..
			} => *span,
			ExprAst::Float {
				span,
				..
			} => *span,
			ExprAst::String {
				span,
				..
			} => *span,
			ExprAst::Bool {
				span,
				..
			} => *span,
			ExprAst::Null {
				span,
			} => *span,
			ExprAst::BinaryOp {
				span,
				..
			} => *span,
			ExprAst::UnaryOp {
				span,
				..
			} => *span,
			ExprAst::Paren {
				span,
				..
			} => *span,
			ExprAst::Call {
				span,
				..
			} => *span,
			ExprAst::FieldAccess {
				span,
				..
			} => *span,
			ExprAst::Subquery {
				span,
				..
			} => *span,
			ExprAst::InList {
				span,
				..
			} => *span,
			ExprAst::InSubquery {
				span,
				..
			} => *span,
		}
	}
}
