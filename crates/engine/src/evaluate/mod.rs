// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod arith;
pub mod column;
pub mod convert;
pub mod row;

// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Row,
	interface::{ColumnPolicyKind, ColumnSaturationPolicy, DEFAULT_COLUMN_SATURATION_POLICY, ResolvedColumn},
	value::column::{ColumnData, Columns},
};
use reifydb_type::{Params, Type};

use crate::stack::Stack;
// FIXME this should not be part of core - engine should be sufficient now

/// Represents target column information for evaluation
#[derive(Debug, Clone)]
pub enum TargetColumn<'a> {
	/// Fully resolved column with complete source information
	Resolved(ResolvedColumn<'a>),
	/// Partial column information with type, policies, and optional names for error reporting
	Partial {
		source_name: Option<String>,
		column_name: Option<String>,
		column_type: Type,
		policies: Vec<ColumnPolicyKind>,
	},
}

impl<'a> TargetColumn<'a> {
	/// Get the column type
	pub fn column_type(&self) -> Type {
		match self {
			Self::Resolved(col) => col.column_type(),
			Self::Partial {
				column_type,
				..
			} => *column_type,
		}
	}

	/// Get the column policies
	pub fn policies(&self) -> Vec<ColumnPolicyKind> {
		match self {
			Self::Resolved(col) => col.policies(),
			Self::Partial {
				policies,
				..
			} => policies.clone(),
		}
	}

	// FIXME remove this
	/// Convert to NumberOfRangeColumnDescriptor for error reporting
	pub fn to_number_descriptor(
		&self,
	) -> Option<reifydb_type::diagnostic::number::NumberOfRangeColumnDescriptor<'_>> {
		use reifydb_core::interface::resolved::resolved_column_to_number_descriptor;
		use reifydb_type::diagnostic::number::NumberOfRangeColumnDescriptor;

		match self {
			Self::Resolved(col) => Some(resolved_column_to_number_descriptor(col)),
			Self::Partial {
				column_type,
				source_name,
				column_name,
				..
			} => {
				// Only create descriptor if we have at least some name information
				if source_name.is_some() || column_name.is_some() {
					Some(NumberOfRangeColumnDescriptor {
						namespace: None,
						table: source_name.as_deref(),
						column: column_name.as_deref(),
						column_type: Some(*column_type),
					})
				} else {
					None
				}
			}
		}
	}
}

#[derive(Debug)]
pub struct ColumnEvaluationContext<'a> {
	pub target: Option<TargetColumn<'a>>,
	pub columns: Columns<'a>,
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
	pub target: Option<TargetColumn<'a>>,
	pub params: &'a Params,
}
