// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::{Fragment, Type};
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
pub enum Column<'a> {
	SourceQualified(SourceQualified<'a>),
	ColumnQualified(ColumnQualified<'a>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SourceQualified<'a> {
	pub source: Fragment<'a>,
	pub name: Fragment<'a>,
	pub data: ColumnData,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ColumnQualified<'a> {
	pub name: Fragment<'a>,
	pub data: ColumnData,
}

impl<'a> Column<'a> {
	pub fn get_type(&self) -> Type {
		match self {
			// Self::FullyQualified(col) => col.data.get_type(),
			Self::SourceQualified(col) => col.data.get_type(),
			Self::ColumnQualified(col) => col.data.get_type(),
		}
	}

	pub fn qualified_name(&self) -> String {
		match self {
			Self::SourceQualified(col) => {
				format!("{}.{}", col.source.text(), col.name.text())
			}
			Self::ColumnQualified(col) => col.name.text().to_string(),
		}
	}

	pub fn with_new_data(&self, data: ColumnData) -> Column<'a> {
		match self {
			Self::SourceQualified(col) => Self::SourceQualified(SourceQualified {
				source: col.source.clone(),
				name: col.name.clone(),
				data,
			}),
			Self::ColumnQualified(col) => Self::ColumnQualified(ColumnQualified {
				name: col.name.clone(),
				data,
			}),
		}
	}

	pub fn name(&self) -> &Fragment<'a> {
		match self {
			Self::SourceQualified(col) => &col.name,
			Self::ColumnQualified(col) => &col.name,
		}
	}

	pub fn name_owned(&self) -> Fragment<'a> {
		self.name().clone()
	}

	pub fn source(&self) -> Option<&Fragment<'a>> {
		match self {
			Self::SourceQualified(col) => Some(&col.source),
			Self::ColumnQualified(_) => None,
		}
	}

	// Deprecated: Use source() instead
	pub fn table(&self) -> Option<&Fragment<'a>> {
		self.source()
	}

	pub fn namespace(&self) -> Option<&Fragment<'a>> {
		match self {
			Self::SourceQualified(_) => None,
			Self::ColumnQualified(_) => None,
		}
	}

	pub fn data(&self) -> &ColumnData {
		match self {
			Self::SourceQualified(col) => &col.data,
			Self::ColumnQualified(col) => &col.data,
		}
	}

	pub fn data_mut(&mut self) -> &mut ColumnData {
		match self {
			Self::SourceQualified(col) => &mut col.data,
			Self::ColumnQualified(col) => &mut col.data,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_source_qualified_column() {
		let column = SourceQualified::int4("test_columns", "normal_column", [1, 2, 3]);
		assert_eq!(column.qualified_name(), "test_columns.normal_column");
		match column {
			Column::SourceQualified(col) => {
				assert_eq!(col.source.text(), "test_columns");
				assert_eq!(col.name.text(), "normal_column");
			}
			_ => panic!("Expected SourceQualified variant"),
		}
	}

	#[test]
	fn test_column_qualified() {
		let column = ColumnQualified::int4("expr_result", [1, 2, 3]);
		assert_eq!(column.qualified_name(), "expr_result");
		match column {
			Column::ColumnQualified(col) => {
				assert_eq!(col.name.text(), "expr_result");
			}
			_ => panic!("Expected ColumnQualified variant"),
		}
	}
}
