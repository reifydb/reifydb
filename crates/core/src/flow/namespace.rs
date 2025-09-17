// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use serde::{Deserialize, Serialize};

use crate::interface::ColumnDef;

/// Namespace information for a flow node, including column definitions
/// and source identification for fully qualified column references
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowNodeSchema {
	/// Column definitions for this node's output
	pub columns: Vec<ColumnDef>,
	/// Database namespace name (e.g., "test", "public")
	pub namespace_name: Option<String>,
	/// Table or view name (e.g., "orders", "customers")
	pub source_name: Option<String>,
}

impl FlowNodeSchema {
	/// Create a new FlowNodeSchema
	pub fn new(columns: Vec<ColumnDef>, namespace_name: Option<String>, source_name: Option<String>) -> Self {
		Self {
			columns,
			namespace_name,
			source_name,
		}
	}

	/// Get the fully qualified source name (namespace.table)
	pub fn fully_qualified_name(&self) -> String {
		match (&self.namespace_name, &self.source_name) {
			(Some(namespace), Some(source)) => {
				format!("{}.{}", namespace, source)
			}
			(None, Some(source)) => source.clone(),
			_ => "unknown".to_string(),
		}
	}

	/// Create an empty namespace (for operators that don't have a direct
	/// source)
	pub fn empty() -> Self {
		Self {
			columns: Vec::new(),
			namespace_name: None,
			source_name: None,
		}
	}

	/// Merge two namespaces (for JOIN outputs)
	pub fn merge(left: &Self, right: &Self) -> Self {
		let mut columns = left.columns.clone();
		columns.extend(right.columns.clone());

		Self {
			columns,
			// For merged namespaces, we don't have a single source
			namespace_name: None,
			source_name: None,
		}
	}
}
