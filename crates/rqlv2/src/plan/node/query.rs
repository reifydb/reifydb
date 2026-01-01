// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Query plan nodes.

use crate::{
	plan::{CatalogColumn, Index, Plan, Primitive, Table, Variable, node::expr::PlanExpr},
	token::Span,
};

/// Scan a primitive data source.
#[derive(Debug, Clone, Copy)]
pub struct ScanNode<'bump> {
	pub primitive: Primitive<'bump>,
	pub alias: Option<&'bump str>,
	pub span: Span,
}

/// Scan with index hint.
#[derive(Debug, Clone, Copy)]
pub struct IndexScanNode<'bump> {
	pub primitive: &'bump Table<'bump>,
	pub index: &'bump Index<'bump>,
	pub alias: Option<&'bump str>,
	pub span: Span,
}

/// Filter rows by predicate.
#[derive(Debug, Clone, Copy)]
pub struct FilterNode<'bump> {
	pub input: &'bump Plan<'bump>,
	pub predicate: &'bump PlanExpr<'bump>,
	pub span: Span,
}

/// Project columns (MAP).
#[derive(Debug, Clone, Copy)]
pub struct ProjectNode<'bump> {
	pub input: Option<&'bump Plan<'bump>>,
	pub projections: &'bump [Projection<'bump>],
	pub span: Span,
}

/// Single projection item.
#[derive(Debug, Clone, Copy)]
pub struct Projection<'bump> {
	pub expr: &'bump PlanExpr<'bump>,
	pub alias: Option<&'bump str>,
	pub span: Span,
}

/// Extend with computed columns.
#[derive(Debug, Clone, Copy)]
pub struct ExtendNode<'bump> {
	pub input: Option<&'bump Plan<'bump>>,
	pub extensions: &'bump [Projection<'bump>],
	pub span: Span,
}

/// Aggregate with grouping.
#[derive(Debug, Clone, Copy)]
pub struct AggregateNode<'bump> {
	pub input: &'bump Plan<'bump>,
	pub group_by: &'bump [&'bump PlanExpr<'bump>],
	pub aggregations: &'bump [Projection<'bump>],
	pub span: Span,
}

/// Sort/Order by.
#[derive(Debug, Clone, Copy)]
pub struct SortNode<'bump> {
	pub input: &'bump Plan<'bump>,
	pub keys: &'bump [SortKey<'bump>],
	pub span: Span,
}

/// Sort key.
#[derive(Debug, Clone, Copy)]
pub struct SortKey<'bump> {
	pub expr: &'bump PlanExpr<'bump>,
	pub direction: SortDirection,
	pub nulls: NullsOrder,
}

/// Sort direction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SortDirection {
	#[default]
	Asc,
	Desc,
}

/// Nulls ordering.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum NullsOrder {
	#[default]
	First,
	Last,
}

/// Take/Limit rows.
#[derive(Debug, Clone, Copy)]
pub struct TakeNode<'bump> {
	pub input: &'bump Plan<'bump>,
	pub count: u64,
	pub span: Span,
}

/// Distinct on columns.
#[derive(Debug, Clone, Copy)]
pub struct DistinctNode<'bump> {
	pub input: &'bump Plan<'bump>,
	pub columns: &'bump [&'bump CatalogColumn<'bump>],
	pub span: Span,
}

/// Inner join.
#[derive(Debug, Clone, Copy)]
pub struct JoinInnerNode<'bump> {
	pub left: &'bump Plan<'bump>,
	pub right: &'bump Plan<'bump>,
	pub on: &'bump [JoinCondition<'bump>],
	pub alias: Option<&'bump str>,
	pub span: Span,
}

/// Left join.
#[derive(Debug, Clone, Copy)]
pub struct JoinLeftNode<'bump> {
	pub left: &'bump Plan<'bump>,
	pub right: &'bump Plan<'bump>,
	pub on: &'bump [JoinCondition<'bump>],
	pub alias: Option<&'bump str>,
	pub span: Span,
}

/// Natural join.
#[derive(Debug, Clone, Copy)]
pub struct JoinNaturalNode<'bump> {
	pub left: &'bump Plan<'bump>,
	pub right: &'bump Plan<'bump>,
	pub join_type: JoinType,
	pub alias: Option<&'bump str>,
	pub span: Span,
}

/// Join condition.
#[derive(Debug, Clone, Copy)]
pub struct JoinCondition<'bump> {
	pub left: &'bump PlanExpr<'bump>,
	pub right: &'bump PlanExpr<'bump>,
}

/// Join type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
	Inner,
	Left,
	Right,
	Full,
}

/// Merge two streams.
#[derive(Debug, Clone, Copy)]
pub struct MergeNode<'bump> {
	pub left: &'bump Plan<'bump>,
	pub right: &'bump Plan<'bump>,
	pub span: Span,
}

/// Window aggregation.
#[derive(Debug, Clone, Copy)]
pub struct WindowNode<'bump> {
	pub input: Option<&'bump Plan<'bump>>,
	pub window_type: WindowType,
	pub size: WindowSize,
	pub slide: Option<WindowSlide>,
	pub group_by: &'bump [&'bump PlanExpr<'bump>],
	pub aggregations: &'bump [Projection<'bump>],
	pub span: Span,
}

/// Window type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowType {
	Tumbling,
	Sliding,
	Session,
}

/// Window size.
#[derive(Debug, Clone, Copy)]
pub enum WindowSize {
	Rows(u64),
	Duration(core::time::Duration),
}

/// Window slide.
#[derive(Debug, Clone, Copy)]
pub enum WindowSlide {
	Rows(u64),
	Duration(core::time::Duration),
}

/// Apply operator.
#[derive(Debug, Clone, Copy)]
pub struct ApplyNode<'bump> {
	pub input: Option<&'bump Plan<'bump>>,
	pub operator: &'bump str,
	pub arguments: &'bump [&'bump PlanExpr<'bump>],
	pub span: Span,
}

/// Inline data (literal rows).
#[derive(Debug, Clone, Copy)]
pub struct InlineDataNode<'bump> {
	pub rows: &'bump [&'bump [&'bump PlanExpr<'bump>]],
	pub span: Span,
}

/// Generator function.
#[derive(Debug, Clone, Copy)]
pub struct GeneratorNode<'bump> {
	pub name: &'bump str,
	pub arguments: &'bump [&'bump PlanExpr<'bump>],
	pub span: Span,
}

/// Variable source (FROM $var).
#[derive(Debug, Clone, Copy)]
pub struct VariableSourceNode<'bump> {
	pub variable: &'bump Variable<'bump>,
	pub span: Span,
}

/// Environment source (FROM $env).
#[derive(Debug, Clone, Copy)]
pub struct EnvironmentNode {
	pub span: Span,
}

/// O(1) point lookup by row number.
#[derive(Debug, Clone, Copy)]
pub struct RowPointLookupNode<'bump> {
	pub source: Primitive<'bump>,
	pub row_number: u64,
	pub span: Span,
}

/// O(k) list lookup by row numbers.
#[derive(Debug, Clone, Copy)]
pub struct RowListLookupNode<'bump> {
	pub source: Primitive<'bump>,
	pub row_numbers: &'bump [u64],
	pub span: Span,
}

/// Range scan by row numbers.
#[derive(Debug, Clone, Copy)]
pub struct RowRangeScanNode<'bump> {
	pub source: Primitive<'bump>,
	pub start: u64,
	pub end: u64,
	pub span: Span,
}

/// Extract scalar from 1x1 result.
#[derive(Debug, Clone, Copy)]
pub struct ScalarizeNode<'bump> {
	pub input: &'bump Plan<'bump>,
	pub span: Span,
}
