// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Query expression types (FROM, FILTER, MAP, JOIN, etc.).

use super::{Expr, Variable};
use crate::token::span::Span;

/// FROM clause - data source.
#[derive(Debug, Clone, Copy)]
pub enum FromExpr<'bump> {
	/// FROM table_name, FROM namespace.table
	Source(SourceRef<'bump>),
	/// FROM $variable
	Variable(FromVariable<'bump>),
	/// FROM [{ ... }, { ... }] - inline data
	Inline(FromInline<'bump>),
	/// FROM generator_func { params }
	Generator(FromGenerator<'bump>),
	/// FROM $env
	Environment(FromEnvironment),
}

impl<'bump> FromExpr<'bump> {
	/// Get the span of this FROM expression.
	pub fn span(&self) -> Span {
		match self {
			FromExpr::Source(s) => s.span,
			FromExpr::Variable(v) => v.span,
			FromExpr::Inline(i) => i.span,
			FromExpr::Generator(g) => g.span,
			FromExpr::Environment(e) => e.span,
		}
	}
}

/// Reference to a table/view/source.
#[derive(Debug, Clone, Copy)]
pub struct SourceRef<'bump> {
	pub namespace: Option<&'bump str>,
	pub name: &'bump str,
	pub alias: Option<&'bump str>,
	pub index_hint: Option<&'bump str>,
	pub span: Span,
}

impl<'bump> SourceRef<'bump> {
	/// Create a new source reference.
	pub fn new(name: &'bump str, span: Span) -> Self {
		Self {
			namespace: None,
			name,
			alias: None,
			index_hint: None,
			span,
		}
	}

	/// Set the namespace.
	pub fn with_namespace(mut self, namespace: &'bump str) -> Self {
		self.namespace = Some(namespace);
		self
	}

	/// Set the alias.
	pub fn with_alias(mut self, alias: &'bump str) -> Self {
		self.alias = Some(alias);
		self
	}

	/// Set the index hint.
	pub fn with_index_hint(mut self, hint: &'bump str) -> Self {
		self.index_hint = Some(hint);
		self
	}
}

/// FROM $variable
#[derive(Debug, Clone, Copy)]
pub struct FromVariable<'bump> {
	pub variable: Variable<'bump>,
	pub span: Span,
}

/// FROM inline data
#[derive(Debug, Clone, Copy)]
pub struct FromInline<'bump> {
	pub rows: &'bump [Expr<'bump>],
	pub span: Span,
}

/// FROM generator
#[derive(Debug, Clone, Copy)]
pub struct FromGenerator<'bump> {
	pub name: &'bump str,
	pub params: &'bump [Expr<'bump>],
	pub span: Span,
}

/// FROM $env
#[derive(Debug, Clone, Copy)]
pub struct FromEnvironment {
	pub span: Span,
}

/// FILTER clause.
#[derive(Debug, Clone, Copy)]
pub struct FilterExpr<'bump> {
	pub predicate: &'bump Expr<'bump>,
	pub span: Span,
}

impl<'bump> FilterExpr<'bump> {
	/// Create a new filter expression.
	pub fn new(predicate: &'bump Expr<'bump>, span: Span) -> Self {
		Self {
			predicate,
			span,
		}
	}
}

/// MAP/SELECT clause - projection.
#[derive(Debug, Clone, Copy)]
pub struct MapExpr<'bump> {
	pub projections: &'bump [Expr<'bump>],
	pub span: Span,
}

impl<'bump> MapExpr<'bump> {
	/// Create a new map expression.
	pub fn new(projections: &'bump [Expr<'bump>], span: Span) -> Self {
		Self {
			projections,
			span,
		}
	}
}

/// EXTEND clause - add computed columns.
#[derive(Debug, Clone, Copy)]
pub struct ExtendExpr<'bump> {
	pub extensions: &'bump [Expr<'bump>],
	pub span: Span,
}

impl<'bump> ExtendExpr<'bump> {
	/// Create a new extend expression.
	pub fn new(extensions: &'bump [Expr<'bump>], span: Span) -> Self {
		Self {
			extensions,
			span,
		}
	}
}

/// AGGREGATE clause.
#[derive(Debug, Clone, Copy)]
pub struct AggregateExpr<'bump> {
	pub group_by: &'bump [Expr<'bump>],
	pub aggregations: &'bump [Expr<'bump>],
	pub span: Span,
}

impl<'bump> AggregateExpr<'bump> {
	/// Create a new aggregate expression.
	pub fn new(group_by: &'bump [Expr<'bump>], aggregations: &'bump [Expr<'bump>], span: Span) -> Self {
		Self {
			group_by,
			aggregations,
			span,
		}
	}
}

/// SORT clause.
#[derive(Debug, Clone, Copy)]
pub struct SortExpr<'bump> {
	pub columns: &'bump [SortColumn<'bump>],
	pub span: Span,
}

impl<'bump> SortExpr<'bump> {
	/// Create a new sort expression.
	pub fn new(columns: &'bump [SortColumn<'bump>], span: Span) -> Self {
		Self {
			columns,
			span,
		}
	}
}

/// Sort column with optional direction.
#[derive(Debug, Clone, Copy)]
pub struct SortColumn<'bump> {
	pub expr: &'bump Expr<'bump>,
	pub direction: Option<SortDirection>,
}

impl<'bump> SortColumn<'bump> {
	/// Create a new sort column.
	pub fn new(expr: &'bump Expr<'bump>, direction: Option<SortDirection>) -> Self {
		Self {
			expr,
			direction,
		}
	}
}

/// Sort direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SortDirection {
	#[default]
	Asc,
	Desc,
}

/// DISTINCT clause.
#[derive(Debug, Clone, Copy)]
pub struct DistinctExpr<'bump> {
	pub columns: &'bump [Expr<'bump>],
	pub span: Span,
}

impl<'bump> DistinctExpr<'bump> {
	/// Create a new distinct expression.
	pub fn new(columns: &'bump [Expr<'bump>], span: Span) -> Self {
		Self {
			columns,
			span,
		}
	}
}

/// TAKE clause - limit.
#[derive(Debug, Clone, Copy)]
pub struct TakeExpr<'bump> {
	pub count: &'bump Expr<'bump>,
	pub span: Span,
}

impl<'bump> TakeExpr<'bump> {
	/// Create a new take expression.
	pub fn new(count: &'bump Expr<'bump>, span: Span) -> Self {
		Self {
			count,
			span,
		}
	}
}

/// JOIN operations.
#[derive(Debug, Clone, Copy)]
pub enum JoinExpr<'bump> {
	/// INNER JOIN
	Inner(JoinInner<'bump>),
	/// LEFT JOIN
	Left(JoinLeft<'bump>),
	/// NATURAL JOIN
	Natural(JoinNatural<'bump>),
}

/// Source for a JOIN operation.
#[derive(Debug, Clone, Copy)]
pub enum JoinSource<'bump> {
	/// Subquery: { FROM ... | FILTER ... }
	SubQuery(&'bump Expr<'bump>),
	/// Direct table reference - reads from primitive storage
	Primitive(JoinPrimitive<'bump>),
}

/// Direct table reference for JOIN - reads from primitive storage.
#[derive(Debug, Clone, Copy)]
pub struct JoinPrimitive<'bump> {
	pub source: SourceRef<'bump>,
}

impl<'bump> JoinExpr<'bump> {
	/// Get the span of this JOIN expression.
	pub fn span(&self) -> Span {
		match self {
			JoinExpr::Inner(j) => j.span,
			JoinExpr::Left(j) => j.span,
			JoinExpr::Natural(j) => j.span,
		}
	}
}

/// INNER JOIN
#[derive(Debug, Clone, Copy)]
pub struct JoinInner<'bump> {
	pub source: JoinSource<'bump>,
	pub using_clause: UsingClause<'bump>,
	pub alias: &'bump str,
	pub span: Span,
}

/// LEFT JOIN
#[derive(Debug, Clone, Copy)]
pub struct JoinLeft<'bump> {
	pub source: JoinSource<'bump>,
	pub using_clause: UsingClause<'bump>,
	pub alias: &'bump str,
	pub span: Span,
}

/// NATURAL JOIN
#[derive(Debug, Clone, Copy)]
pub struct JoinNatural<'bump> {
	pub source: JoinSource<'bump>,
	pub alias: &'bump str,
	pub span: Span,
}

/// USING clause for joins.
#[derive(Debug, Clone, Copy)]
pub struct UsingClause<'bump> {
	pub pairs: &'bump [JoinPair<'bump>],
	pub span: Span,
}

impl<'bump> UsingClause<'bump> {
	/// Create a new using clause.
	pub fn new(pairs: &'bump [JoinPair<'bump>], span: Span) -> Self {
		Self {
			pairs,
			span,
		}
	}
}

/// Join condition pair.
#[derive(Debug, Clone, Copy)]
pub struct JoinPair<'bump> {
	pub left: &'bump Expr<'bump>,
	pub right: &'bump Expr<'bump>,
	pub connector: Option<JoinConnector>,
}

impl<'bump> JoinPair<'bump> {
	/// Create a new join pair.
	pub fn new(left: &'bump Expr<'bump>, right: &'bump Expr<'bump>, connector: Option<JoinConnector>) -> Self {
		Self {
			left,
			right,
			connector,
		}
	}
}

/// Join connector (AND/OR between conditions).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinConnector {
	And,
	Or,
}

/// MERGE clause.
#[derive(Debug, Clone, Copy)]
pub struct MergeExpr<'bump> {
	pub subquery: &'bump Expr<'bump>,
	pub span: Span,
}

impl<'bump> MergeExpr<'bump> {
	/// Create a new merge expression.
	pub fn new(subquery: &'bump Expr<'bump>, span: Span) -> Self {
		Self {
			subquery,
			span,
		}
	}
}

/// WINDOW clause.
#[derive(Debug, Clone, Copy)]
pub struct WindowExpr<'bump> {
	pub config: &'bump [WindowConfig<'bump>],
	pub aggregations: &'bump [Expr<'bump>],
	pub group_by: &'bump [Expr<'bump>],
	pub span: Span,
}

impl<'bump> WindowExpr<'bump> {
	/// Create a new window expression.
	pub fn new(
		config: &'bump [WindowConfig<'bump>],
		aggregations: &'bump [Expr<'bump>],
		group_by: &'bump [Expr<'bump>],
		span: Span,
	) -> Self {
		Self {
			config,
			aggregations,
			group_by,
			span,
		}
	}
}

/// Window configuration entry.
#[derive(Debug, Clone, Copy)]
pub struct WindowConfig<'bump> {
	pub key: &'bump str,
	pub value: &'bump Expr<'bump>,
}

impl<'bump> WindowConfig<'bump> {
	/// Create a new window config.
	pub fn new(key: &'bump str, value: &'bump Expr<'bump>) -> Self {
		Self {
			key,
			value,
		}
	}
}
