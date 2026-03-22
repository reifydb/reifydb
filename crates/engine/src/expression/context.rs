// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::LazyLock;

use reifydb_core::{
	interface::{
		catalog::property::{ColumnPropertyKind, ColumnSaturationPolicy, DEFAULT_COLUMN_SATURATION_POLICY},
		evaluate::TargetColumn,
	},
	value::column::columns::Columns,
};
use reifydb_function::registry::Functions;
use reifydb_runtime::context::RuntimeContext;
use reifydb_type::{params::Params, value::identity::IdentityId};

use crate::{
	arena::QueryArena,
	transform::context::TransformContext,
	vm::{stack::SymbolTable, volcano::query::QueryContext},
};

/// Session-scoped evaluation context — holds the 7 fields that are invariant
/// within a given operator. Provides factory methods to produce `EvalContext`
/// values that vary only in `columns` and `row_count`.
#[derive(Clone, Copy)]
pub struct EvalSession<'a> {
	pub params: &'a Params,
	pub symbols: &'a SymbolTable,
	pub functions: &'a Functions,
	pub runtime_context: &'a RuntimeContext,
	pub arena: Option<&'a QueryArena>,
	pub identity: IdentityId,
	pub is_aggregate_context: bool,
}

impl<'a> EvalSession<'a> {
	/// Main constructor — produces an `EvalContext` with `target=None` and `take=None`.
	pub fn eval(&self, columns: Columns, row_count: usize) -> EvalContext<'a> {
		EvalContext {
			target: None,
			columns,
			row_count,
			take: None,
			params: self.params,
			symbols: self.symbols,
			is_aggregate_context: self.is_aggregate_context,
			functions: self.functions,
			runtime_context: self.runtime_context,
			arena: self.arena,
			identity: self.identity,
		}
	}

	/// Shorthand for `eval(Columns::empty(), 1)`.
	pub fn eval_empty(&self) -> EvalContext<'a> {
		self.eval(Columns::empty(), 1)
	}

	/// Shorthand for `eval(columns, 1)` with `take=Some(1)`.
	pub fn eval_join(&self, columns: Columns) -> EvalContext<'a> {
		let mut ctx = self.eval(columns, 1);
		ctx.take = Some(1);
		ctx
	}

	/// Build from a `TransformContext` + stored `QueryContext` (volcano nodes with input).
	pub fn from_transform(ctx: &'a TransformContext, stored: &'a QueryContext) -> Self {
		Self {
			params: ctx.params,
			symbols: &stored.symbols,
			functions: ctx.functions,
			runtime_context: ctx.runtime_context,
			arena: None,
			identity: stored.identity,
			is_aggregate_context: false,
		}
	}

	/// Build from a `QueryContext` (without-input nodes, joins, inline).
	pub fn from_query(ctx: &'a QueryContext) -> Self {
		Self {
			params: &ctx.params,
			symbols: &ctx.symbols,
			functions: &ctx.services.functions,
			runtime_context: &ctx.services.runtime_context,
			arena: None,
			identity: ctx.identity,
			is_aggregate_context: false,
		}
	}

	/// Build a testing session with static empty values.
	pub fn testing() -> EvalSession<'static> {
		static EMPTY_PARAMS: LazyLock<Params> = LazyLock::new(|| Params::None);
		static EMPTY_SYMBOL_TABLE: LazyLock<SymbolTable> = LazyLock::new(|| SymbolTable::new());
		static EMPTY_FUNCTIONS: LazyLock<Functions> = LazyLock::new(|| Functions::empty());
		static DEFAULT_RUNTIME_CONTEXT: LazyLock<RuntimeContext> = LazyLock::new(|| RuntimeContext::default());
		EvalSession {
			params: &EMPTY_PARAMS,
			symbols: &EMPTY_SYMBOL_TABLE,
			functions: &EMPTY_FUNCTIONS,
			runtime_context: &DEFAULT_RUNTIME_CONTEXT,
			arena: None,
			identity: IdentityId::root(),
			is_aggregate_context: false,
		}
	}
}

pub struct EvalContext<'a> {
	pub target: Option<TargetColumn>,
	pub columns: Columns,
	pub row_count: usize,
	pub take: Option<usize>,
	pub params: &'a Params,
	pub symbols: &'a SymbolTable,
	// TODO: This is a temporary hack to support aggregate functions in StandardColumnEvaluator
	// Should be replaced with proper function detection or separate aggregation methods
	pub is_aggregate_context: bool,
	pub functions: &'a Functions,
	pub runtime_context: &'a RuntimeContext,
	pub arena: Option<&'a QueryArena>,
	pub identity: IdentityId,
}

impl<'a> EvalContext<'a> {
	pub fn testing() -> Self {
		EvalSession::testing().eval_empty()
	}

	pub(crate) fn saturation_policy(&self) -> ColumnSaturationPolicy {
		self.target
			.as_ref()
			.and_then(|t| {
				t.properties().into_iter().find_map(|p| match p {
					ColumnPropertyKind::Saturation(policy) => Some(policy),
				})
			})
			.unwrap_or(DEFAULT_COLUMN_SATURATION_POLICY.clone())
	}
}

/// Compile-time context for resolving functions and UDFs.
pub struct CompileContext<'a> {
	pub functions: &'a Functions,
	pub symbols: &'a SymbolTable,
}
