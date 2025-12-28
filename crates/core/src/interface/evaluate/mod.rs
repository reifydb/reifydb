// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::Type;

use crate::interface::{ColumnPolicyKind, ResolvedColumn};

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

	/// Convert to NumberOfRangeColumnDescriptor for error reporting
	pub fn to_number_descriptor(
		&self,
	) -> Option<reifydb_type::diagnostic::number::NumberOfRangeColumnDescriptor<'_>> {
		use reifydb_type::diagnostic::number::NumberOfRangeColumnDescriptor;

		use crate::interface::resolved::resolved_column_to_number_descriptor;

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
