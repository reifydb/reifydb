// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Expression types for the unified AST.

pub mod collection;
pub mod identifier;
pub mod literal;
pub mod operator;
pub mod query;
pub mod special;

use collection::*;
use identifier::*;
use literal::*;
use operator::*;
use query::*;
use special::*;

use crate::token::span::Span;

/// Unified expression type for both query predicates and scripting.
#[derive(Debug, Clone, Copy)]
pub enum Expr<'bump> {
	// === Literals ===
	/// Literal value (integer, float, string, bool, etc.)
	Literal(Literal<'bump>),

	// === Identifiers & References ===
	/// Simple identifier: column_name, table_name
	Identifier(Identifier<'bump>),
	/// Qualified identifier: namespace.table, table.column
	QualifiedIdent(QualifiedIdent<'bump>),
	/// Variable reference: $name, $123
	Variable(Variable<'bump>),
	/// Wildcard: *
	Wildcard(WildcardExpr),
	/// ROWNUM pseudo-column
	Rownum(RownumExpr),
	/// $env environment reference
	Environment(EnvironmentExpr),

	// === Operators ===
	/// Binary operation: a + b, a AND b, etc.
	Binary(BinaryExpr<'bump>),
	/// Unary operation: -a, NOT a, etc.
	Unary(UnaryExpr<'bump>),

	// === Query Operations ===
	/// FROM clause - data source
	From(FromExpr<'bump>),
	/// FILTER clause - predicate
	Filter(FilterExpr<'bump>),
	/// MAP/SELECT clause - projection
	Map(MapExpr<'bump>),
	/// EXTEND clause - add computed columns
	Extend(ExtendExpr<'bump>),
	/// AGGREGATE clause - grouping and aggregation
	Aggregate(AggregateExpr<'bump>),
	/// SORT clause - ordering
	Sort(SortExpr<'bump>),
	/// DISTINCT clause - unique rows
	Distinct(DistinctExpr<'bump>),
	/// TAKE clause - limit
	Take(TakeExpr<'bump>),
	/// JOIN operations
	Join(JoinExpr<'bump>),
	/// MERGE clause
	Merge(MergeExpr<'bump>),
	/// WINDOW clause
	Window(WindowExpr<'bump>),

	// === Special Expressions ===
	/// BETWEEN expression: x BETWEEN low AND high
	Between(BetweenExpr<'bump>),
	/// IN expression: x IN [values] or x NOT IN [values]
	In(InExpr<'bump>),
	/// CAST expression: CAST(expr, type)
	Cast(CastExpr<'bump>),
	/// Function call: func(args)
	Call(CallExpr<'bump>),
	/// APPLY expression
	Apply(ApplyExpr<'bump>),

	// === Collections ===
	/// List expression: [a, b, c]
	List(ListExpr<'bump>),
	/// Tuple expression: (a, b, c)
	Tuple(TupleExpr<'bump>),
	/// Inline object/record: { key: value, ... }
	Inline(InlineExpr<'bump>),

	// === Control Flow Expressions ===
	/// Conditional expression: if cond then else
	IfExpr(IfExpr<'bump>),
	/// Loop expression: loop { body }
	LoopExpr(LoopExpr<'bump>),
	/// For loop expression: for $var in iterable { body }
	ForExpr(ForExpr<'bump>),

	// === Subquery ===
	/// Subquery: { FROM ... | ... }
	SubQuery(SubQueryExpr<'bump>),
	/// EXISTS expression: EXISTS(subquery) or NOT EXISTS(subquery)
	Exists(ExistsExpr<'bump>),
	/// Parenthesized expression
	Paren(&'bump Expr<'bump>),
}

impl<'bump> Expr<'bump> {
	/// Get the span of this expression.
	pub fn span(&self) -> Span {
		match self {
			Expr::Literal(l) => l.span(),
			Expr::Identifier(i) => i.span,
			Expr::QualifiedIdent(q) => q.span,
			Expr::Variable(v) => v.span,
			Expr::Wildcard(w) => w.span,
			Expr::Rownum(r) => r.span,
			Expr::Environment(e) => e.span,
			Expr::Binary(b) => b.span,
			Expr::Unary(u) => u.span,
			Expr::From(f) => f.span(),
			Expr::Filter(f) => f.span,
			Expr::Map(m) => m.span,
			Expr::Extend(e) => e.span,
			Expr::Aggregate(a) => a.span,
			Expr::Sort(s) => s.span,
			Expr::Distinct(d) => d.span,
			Expr::Take(t) => t.span,
			Expr::Join(j) => j.span(),
			Expr::Merge(m) => m.span,
			Expr::Window(w) => w.span,
			Expr::Between(b) => b.span,
			Expr::In(i) => i.span,
			Expr::Cast(c) => c.span,
			Expr::Call(c) => c.span,
			Expr::Apply(a) => a.span,
			Expr::List(l) => l.span,
			Expr::Tuple(t) => t.span,
			Expr::Inline(i) => i.span,
			Expr::IfExpr(i) => i.span,
			Expr::LoopExpr(l) => l.span,
			Expr::ForExpr(f) => f.span,
			Expr::SubQuery(s) => s.span,
			Expr::Exists(e) => e.span,
			Expr::Paren(p) => p.span(),
		}
	}
}
