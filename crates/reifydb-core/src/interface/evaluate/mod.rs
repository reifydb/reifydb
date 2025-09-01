// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod arith;
mod convert;
mod demote;
pub mod expression;
mod promote;

pub use convert::Convert;
pub use demote::Demote;
pub use promote::Promote;
use reifydb_type::Type;

use crate::{
	ColumnDescriptor,
	interface::{
		ColumnPolicyKind, ColumnSaturationPolicy,
		DEFAULT_COLUMN_SATURATION_POLICY, Params,
		expression::Expression,
	},
	value::columnar::{Column, ColumnData, Columns},
};

#[derive(Debug)]
pub struct EvaluationContext<'a> {
	pub target_column: Option<ColumnDescriptor<'a>>,
	pub column_policies: Vec<ColumnPolicyKind>,
	pub columns: Columns,
	pub row_count: usize,
	pub take: Option<usize>,
	pub params: &'a Params,
}

impl<'a> EvaluationContext<'a> {
	pub fn testing() -> Self {
		use std::sync::LazyLock;
		static EMPTY_PARAMS: LazyLock<Params> =
			LazyLock::new(|| Params::None);
		Self {
			target_column: None,
			column_policies: Vec::new(),
			columns: Columns::empty(),
			row_count: 1,
			take: None,
			params: &EMPTY_PARAMS,
		}
	}

	pub(crate) fn saturation_policy(&self) -> &ColumnSaturationPolicy {
		self.column_policies
			.iter()
			.find_map(|p| match p {
				ColumnPolicyKind::Saturation(policy) => {
					Some(policy)
				}
			})
			.unwrap_or(&DEFAULT_COLUMN_SATURATION_POLICY)
	}

	#[inline]
	pub fn pooled(&self, target: Type, capacity: usize) -> ColumnData {
		ColumnData::with_capacity(target, capacity)
	}
}

pub trait Evaluator: Send + Sync + 'static {
	fn evaluate(
		&self,
		ctx: &EvaluationContext,
		expr: &Expression,
	) -> crate::Result<Column>;
}
