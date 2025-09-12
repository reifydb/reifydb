// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::Type;
use serde::{Deserialize, Serialize};

mod columns;
mod data;
pub mod frame;
pub mod layout;
#[allow(dead_code, unused_variables)]
pub mod pool;
pub mod push;
mod qualification;
mod transform;
mod view;

pub use columns::Columns;
pub use data::ColumnData;
pub use view::group_by::{GroupByView, GroupKey};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Column {
	FullyQualified(FullyQualified),
	SourceQualified(SourceQualified),
	ColumnQualified(ColumnQualified),
	Unqualified(Unqualified),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FullyQualified {
	pub namespace: String,
	pub source: String,
	pub name: String,
	pub data: ColumnData,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SourceQualified {
	pub source: String,
	pub name: String,
	pub data: ColumnData,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ColumnQualified {
	pub name: String,
	pub data: ColumnData,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Unqualified {
	pub name: String,
	pub data: ColumnData,
}

impl Column {
	pub fn get_type(&self) -> Type {
		match self {
			Self::FullyQualified(col) => col.data.get_type(),
			Self::SourceQualified(col) => col.data.get_type(),
			Self::ColumnQualified(col) => col.data.get_type(),
			Self::Unqualified(col) => col.data.get_type(),
		}
	}

	pub fn qualified_name(&self) -> String {
		match self {
			Self::FullyQualified(col) => format!(
				"{}.{}.{}",
				col.namespace, col.source, col.name
			),
			Self::SourceQualified(col) => {
				format!("{}.{}", col.source, col.name)
			}
			Self::ColumnQualified(col) => col.name.clone(),
			Self::Unqualified(col) => col.name.clone(),
		}
	}

	pub fn with_new_data(&self, data: ColumnData) -> Column {
		match self {
			Self::FullyQualified(col) => {
				Self::FullyQualified(FullyQualified {
					namespace: col.namespace.clone(),
					source: col.source.clone(),
					name: col.name.clone(),
					data,
				})
			}
			Self::SourceQualified(col) => {
				Self::SourceQualified(SourceQualified {
					source: col.source.clone(),
					name: col.name.clone(),
					data,
				})
			}
			Self::ColumnQualified(col) => {
				Self::ColumnQualified(ColumnQualified {
					name: col.name.clone(),
					data,
				})
			}
			Self::Unqualified(col) => {
				Self::Unqualified(Unqualified {
					name: col.name.clone(),
					data,
				})
			}
		}
	}

	pub fn name(&self) -> &str {
		match self {
			Self::FullyQualified(col) => &col.name,
			Self::SourceQualified(col) => &col.name,
			Self::ColumnQualified(col) => &col.name,
			Self::Unqualified(col) => &col.name,
		}
	}

	pub fn source(&self) -> Option<&str> {
		match self {
			Self::FullyQualified(col) => Some(&col.source),
			Self::SourceQualified(col) => Some(&col.source),
			Self::ColumnQualified(_) => None,
			Self::Unqualified(_) => None,
		}
	}

	// Deprecated: Use source() instead
	pub fn table(&self) -> Option<&str> {
		self.source()
	}

	pub fn namespace(&self) -> Option<&str> {
		match self {
			Self::FullyQualified(col) => Some(&col.namespace),
			Self::SourceQualified(_) => None,
			Self::ColumnQualified(_) => None,
			Self::Unqualified(_) => None,
		}
	}

	pub fn data(&self) -> &ColumnData {
		match self {
			Self::FullyQualified(col) => &col.data,
			Self::SourceQualified(col) => &col.data,
			Self::ColumnQualified(col) => &col.data,
			Self::Unqualified(col) => &col.data,
		}
	}

	pub fn data_mut(&mut self) -> &mut ColumnData {
		match self {
			Self::FullyQualified(col) => &mut col.data,
			Self::SourceQualified(col) => &mut col.data,
			Self::ColumnQualified(col) => &mut col.data,
			Self::Unqualified(col) => &mut col.data,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_source_qualified_column() {
		let column = SourceQualified::int4(
			"test_columns",
			"normal_column",
			[1, 2, 3],
		);
		assert_eq!(
			column.qualified_name(),
			"test_columns.normal_column"
		);
		match column {
			Column::SourceQualified(col) => {
				assert_eq!(col.source, "test_columns");
				assert_eq!(col.name, "normal_column");
			}
			_ => panic!("Expected SourceQualified variant"),
		}
	}

	#[test]
	fn test_fully_qualified_column() {
		let column = FullyQualified::int4(
			"public",
			"users",
			"id",
			[1, 2, 3],
		);
		assert_eq!(column.qualified_name(), "public.users.id");
		match column {
			Column::FullyQualified(col) => {
				assert_eq!(col.namespace, "public");
				assert_eq!(col.source, "users");
				assert_eq!(col.name, "id");
			}
			_ => panic!("Expected FullyQualified variant"),
		}
	}

	#[test]
	fn test_column_qualified() {
		let column = ColumnQualified::int4("expr_result", [1, 2, 3]);
		assert_eq!(column.qualified_name(), "expr_result");
		match column {
			Column::ColumnQualified(col) => {
				assert_eq!(col.name, "expr_result");
			}
			_ => panic!("Expected ColumnQualified variant"),
		}
	}

	#[test]
	fn test_unqualified_expression() {
		let column = Unqualified::int4("sum(a+b)", [1, 2, 3]);
		assert_eq!(column.qualified_name(), "sum(a+b)");
		match column {
			Column::Unqualified(col) => {
				assert_eq!(col.name, "sum(a+b)");
			}
			_ => panic!("Expected Unqualified variant"),
		}
	}
}
