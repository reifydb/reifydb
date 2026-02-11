// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod arith;
pub mod column;
pub mod compiled;
pub mod convert;

use reifydb_core::{
	interface::{
		catalog::policy::{ColumnPolicyKind, ColumnSaturationPolicy, DEFAULT_COLUMN_SATURATION_POLICY},
		evaluate::TargetColumn,
	},
	value::column::{columns::Columns, data::ColumnData},
};
use reifydb_type::{params::Params, value::r#type::Type};

use crate::vm::stack::SymbolTable;

#[derive(Debug)]
pub struct ColumnEvaluationContext<'a> {
	pub target: Option<TargetColumn>,
	pub columns: Columns,
	pub row_count: usize,
	pub take: Option<usize>,
	pub params: &'a Params,
	pub symbol_table: &'a SymbolTable,
	// TODO: This is a temporary hack to support aggregate functions in StandardColumnEvaluator
	// Should be replaced with proper function detection or separate aggregation methods
	pub is_aggregate_context: bool,
}

impl<'a> ColumnEvaluationContext<'a> {
	pub fn testing() -> Self {
		use std::sync::LazyLock;
		static EMPTY_PARAMS: LazyLock<Params> = LazyLock::new(|| Params::None);
		static EMPTY_SYMBOL_TABLE: LazyLock<SymbolTable> = LazyLock::new(|| SymbolTable::new());
		Self {
			target: None,
			columns: Columns::empty(),
			row_count: 1,
			take: None,
			params: &EMPTY_PARAMS,
			symbol_table: &EMPTY_SYMBOL_TABLE,
			is_aggregate_context: false,
		}
	}

	pub(crate) fn saturation_policy(&self) -> ColumnSaturationPolicy {
		self.target
			.as_ref()
			.and_then(|t| {
				t.policies().into_iter().find_map(|p| match p {
					ColumnPolicyKind::Saturation(policy) => Some(policy),
				})
			})
			.unwrap_or(DEFAULT_COLUMN_SATURATION_POLICY.clone())
	}

	#[inline]
	pub fn pooled(&self, target: Type, capacity: usize) -> ColumnData {
		ColumnData::with_capacity(target, capacity)
	}
}
