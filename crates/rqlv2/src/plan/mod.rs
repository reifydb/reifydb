// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Unified execution plan for rqlv2.
//!
//! This module provides a single plan representation that is:
//! - Bump-allocated for memory efficiency
//! - Fully resolved with catalog IDs and metadata
//! - Annotated with spans for error diagnostics

pub mod compile;
pub mod explain;
pub mod node;
pub mod types;

// Re-export key types
pub use node::*;
pub use types::*;

use crate::token::Span;

/// The unified execution plan for rqlv2.
///
/// This is the single plan representation - no logical/physical split.
/// All identifiers are resolved and the plan is ready for execution.
#[derive(Debug, Clone, Copy)]
pub enum Plan<'bump> {
	// === Query Operations ===
	/// Scan a primitive data source (table, view, ring buffer, etc.)
	Scan(ScanNode<'bump>),
	/// Scan with index hint
	IndexScan(IndexScanNode<'bump>),
	/// Filter rows by predicate
	Filter(FilterNode<'bump>),
	/// Project columns (MAP)
	Project(ProjectNode<'bump>),
	/// Extend with computed columns
	Extend(ExtendNode<'bump>),
	/// Aggregate with grouping
	Aggregate(AggregateNode<'bump>),
	/// Sort/Order by
	Sort(SortNode<'bump>),
	/// Take/Limit rows
	Take(TakeNode<'bump>),
	/// Distinct on columns
	Distinct(DistinctNode<'bump>),
	/// Inner join
	JoinInner(JoinInnerNode<'bump>),
	/// Left join
	JoinLeft(JoinLeftNode<'bump>),
	/// Natural join
	JoinNatural(JoinNaturalNode<'bump>),
	/// Merge two streams
	Merge(MergeNode<'bump>),
	/// Window aggregation
	Window(WindowNode<'bump>),
	/// Apply operator
	Apply(ApplyNode<'bump>),

	// === Optimized Row Access ===
	/// O(1) point lookup by row number
	RowPointLookup(RowPointLookupNode<'bump>),
	/// O(k) list lookup by row numbers
	RowListLookup(RowListLookupNode<'bump>),
	/// Range scan by row numbers
	RowRangeScan(RowRangeScanNode<'bump>),

	// === DML Operations ===
	/// Insert into table
	Insert(InsertNode<'bump>),
	/// Update rows
	Update(UpdateNode<'bump>),
	/// Delete rows
	Delete(DeleteNode<'bump>),

	// === DDL Operations ===
	/// Create table/view/etc
	Create(CreateNode<'bump>),
	/// Alter table/sequence/etc
	Alter(AlterNode<'bump>),
	/// Drop table/view/etc
	Drop(DropNode<'bump>),

	// === Control Flow ===
	/// Conditional (if/else)
	Conditional(ConditionalNode<'bump>),
	/// Loop statement
	Loop(LoopNode<'bump>),
	/// For loop
	For(ForNode<'bump>),
	/// Variable declaration (let)
	Declare(DeclareNode<'bump>),
	/// Variable assignment
	Assign(AssignNode<'bump>),
	/// Return statement
	Return(ReturnNode<'bump>),
	/// Break from loop
	Break(BreakNode),
	/// Continue to next iteration
	Continue(ContinueNode),
	/// Script function definition (fn name() { ... })
	DefineScriptFunction(DefineScriptFunctionNode<'bump>),
	/// Call script function
	CallScriptFunction(CallScriptFunctionNode<'bump>),

	// === Other ===
	/// Inline data (literal rows)
	InlineData(InlineDataNode<'bump>),
	/// Generator function
	Generator(GeneratorNode<'bump>),
	/// Variable source (FROM $var)
	VariableSource(VariableSourceNode<'bump>),
	/// Environment source (FROM $env)
	Environment(EnvironmentNode),
	/// Extract scalar from 1x1 result
	Scalarize(ScalarizeNode<'bump>),
}

impl<'bump> Plan<'bump> {
	/// Get the span of this plan node for error reporting.
	pub fn span(&self) -> Span {
		match self {
			Plan::Scan(n) => n.span,
			Plan::IndexScan(n) => n.span,
			Plan::Filter(n) => n.span,
			Plan::Project(n) => n.span,
			Plan::Extend(n) => n.span,
			Plan::Aggregate(n) => n.span,
			Plan::Sort(n) => n.span,
			Plan::Take(n) => n.span,
			Plan::Distinct(n) => n.span,
			Plan::JoinInner(n) => n.span,
			Plan::JoinLeft(n) => n.span,
			Plan::JoinNatural(n) => n.span,
			Plan::Merge(n) => n.span,
			Plan::Window(n) => n.span,
			Plan::Apply(n) => n.span,
			Plan::RowPointLookup(n) => n.span,
			Plan::RowListLookup(n) => n.span,
			Plan::RowRangeScan(n) => n.span,
			Plan::Insert(n) => n.span,
			Plan::Update(n) => n.span,
			Plan::Delete(n) => n.span,
			Plan::Create(n) => n.span(),
			Plan::Alter(n) => n.span(),
			Plan::Drop(n) => n.span,
			Plan::Conditional(n) => n.span,
			Plan::Loop(n) => n.span,
			Plan::For(n) => n.span,
			Plan::Declare(n) => n.span,
			Plan::Assign(n) => n.span,
			Plan::Return(n) => n.span,
			Plan::Break(n) => n.span,
			Plan::Continue(n) => n.span,
			Plan::DefineScriptFunction(n) => n.span,
			Plan::CallScriptFunction(n) => n.span,
			Plan::InlineData(n) => n.span,
			Plan::Generator(n) => n.span,
			Plan::VariableSource(n) => n.span,
			Plan::Environment(n) => n.span,
			Plan::Scalarize(n) => n.span,
		}
	}
}
