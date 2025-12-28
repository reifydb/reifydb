// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod arith;
pub mod column;
pub mod convert;

// Re-export TargetColumn from core
pub use reifydb_core::interface::evaluate::TargetColumn;
use reifydb_core::{
	Row,
	interface::{ColumnPolicyKind, ColumnSaturationPolicy, DEFAULT_COLUMN_SATURATION_POLICY},
	value::column::{ColumnData, Columns},
};
use reifydb_type::{Params, Type};

use crate::stack::Stack;

#[derive(Debug)]
pub struct ColumnEvaluationContext<'a> {
	pub target: Option<TargetColumn>,
	pub columns: Columns,
	pub row_count: usize,
	pub take: Option<usize>,
	pub params: &'a Params,
	pub stack: &'a Stack,
	// TODO: This is a temporary hack to support aggregate functions in StandardColumnEvaluator
	// Should be replaced with proper function detection or separate aggregation methods
	pub is_aggregate_context: bool,
}

impl<'a> ColumnEvaluationContext<'a> {
	pub fn testing() -> Self {
		use std::sync::LazyLock;
		static EMPTY_PARAMS: LazyLock<Params> = LazyLock::new(|| Params::None);
		static EMPTY_STACK: LazyLock<Stack> = LazyLock::new(|| Stack::new());
		Self {
			target: None,
			columns: Columns::empty(),
			row_count: 1,
			take: None,
			params: &EMPTY_PARAMS,
			stack: &EMPTY_STACK,
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

pub struct RowEvaluationContext<'a> {
	pub row: Row,
	pub target: Option<TargetColumn>,
	pub params: &'a Params,
}
