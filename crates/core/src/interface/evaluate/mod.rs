// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{error::NumberOutOfRangeDescriptor, value::r#type::Type};

use crate::interface::{
	catalog::policy::ColumnPolicyKind,
	resolved::{ResolvedColumn, resolved_column_to_number_descriptor},
};

/// Represents target column information for evaluation
#[derive(Debug, Clone)]
pub enum TargetColumn {
	/// Fully resolved column with complete source information
	Resolved(ResolvedColumn),
	/// Partial column information with type, policies, and optional names for error reporting
	Partial {
		source_name: Option<String>,
		column_name: Option<String>,
		column_type: Type,
		policies: Vec<ColumnPolicyKind>,
	},
}

impl TargetColumn {
	/// Get the column type
	pub fn column_type(&self) -> Type {
		match self {
			Self::Resolved(col) => col.column_type(),
			Self::Partial {
				column_type,
				..
			} => column_type.clone(),
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

	/// Convert to NumberOutOfRangeDescriptor for error reporting
	pub fn to_number_descriptor(&self) -> Option<NumberOutOfRangeDescriptor> {
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
					Some(NumberOutOfRangeDescriptor {
						namespace: None,
						table: source_name.clone(),
						column: column_name.clone(),
						column_type: Some(column_type.clone()),
					})
				} else {
					None
				}
			}
		}
	}
}
