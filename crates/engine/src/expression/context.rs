// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::LazyLock;

use reifydb_core::{
	interface::{
		catalog::property::{ColumnPropertyKind, ColumnSaturationStrategy, DEFAULT_COLUMN_SATURATION_STRATEGY},
		evaluate::TargetColumn,
	},
	value::column::columns::Columns,
};
use reifydb_extension::transform::context::TransformContext;
use reifydb_routine::function::registry::Functions;
use reifydb_runtime::context::{RuntimeContext, clock::Clock};
use reifydb_type::{params::Params, value::identity::IdentityId};

use crate::{
	arena::QueryArena,
	vm::{stack::SymbolTable, volcano::query::QueryContext},
};

pub struct EvalContext<'a> {
	pub target: Option<TargetColumn>,
	pub columns: Columns,
	pub row_count: usize,
	pub take: Option<usize>,
	pub params: &'a Params,
	pub symbols: &'a SymbolTable,
	pub is_aggregate_context: bool,
	pub functions: &'a Functions,
	pub runtime_context: &'a RuntimeContext,
	pub arena: Option<&'a QueryArena>,
	pub identity: IdentityId,
}

impl<'a> EvalContext<'a> {
	pub fn testing() -> EvalContext<'static> {
		static EMPTY_PARAMS: LazyLock<Params> = LazyLock::new(|| Params::None);
		static EMPTY_SYMBOL_TABLE: LazyLock<SymbolTable> = LazyLock::new(SymbolTable::new);
		static EMPTY_FUNCTIONS: LazyLock<Functions> = LazyLock::new(Functions::empty);
		static DEFAULT_RUNTIME_CONTEXT: LazyLock<RuntimeContext> =
			LazyLock::new(|| RuntimeContext::with_clock(Clock::Real));

		EvalContext {
			target: None,
			columns: Columns::empty(),
			row_count: 1,
			take: None,
			params: &EMPTY_PARAMS,
			symbols: &EMPTY_SYMBOL_TABLE,
			is_aggregate_context: false,
			functions: &EMPTY_FUNCTIONS,
			runtime_context: &DEFAULT_RUNTIME_CONTEXT,
			arena: None,
			identity: IdentityId::root(),
		}
	}

	/// Sibling context with fresh `columns` / `row_count`, sharing all invariant refs.
	pub fn with_eval(&self, columns: Columns, row_count: usize) -> EvalContext<'a> {
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

	pub fn with_eval_empty(&self) -> EvalContext<'a> {
		self.with_eval(Columns::empty(), 1)
	}

	pub fn with_eval_join(&self, columns: Columns) -> EvalContext<'a> {
		let mut ctx = self.with_eval(columns, 1);
		ctx.take = Some(1);
		ctx
	}

	pub fn from_query(ctx: &'a QueryContext) -> Self {
		EvalContext {
			target: None,
			columns: Columns::empty(),
			row_count: 1,
			take: None,
			params: &ctx.params,
			symbols: &ctx.symbols,
			is_aggregate_context: false,
			functions: &ctx.services.functions,
			runtime_context: &ctx.services.runtime_context,
			arena: None,
			identity: ctx.identity,
		}
	}

	pub fn from_transform(ctx: &'a TransformContext, stored: &'a QueryContext) -> Self {
		EvalContext {
			target: None,
			columns: Columns::empty(),
			row_count: 1,
			take: None,
			params: ctx.params,
			symbols: &stored.symbols,
			is_aggregate_context: false,
			functions: ctx.functions,
			runtime_context: ctx.runtime_context,
			arena: None,
			identity: stored.identity,
		}
	}

	pub(crate) fn saturation_policy(&self) -> ColumnSaturationStrategy {
		self.target
			.as_ref()
			.and_then(|t| {
				t.properties()
					.into_iter()
					.map(|p| {
						let ColumnPropertyKind::Saturation(policy) = p;
						policy
					})
					.next()
			})
			.unwrap_or(DEFAULT_COLUMN_SATURATION_STRATEGY.clone())
	}
}

/// Compile-time context for resolving functions and UDFs.
pub struct CompileContext<'a> {
	pub functions: &'a Functions,
	pub symbols: &'a SymbolTable,
}
