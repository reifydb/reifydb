// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::{Fragment, Type};

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

use crate::interface::ResolvedColumn as RColumn;

#[derive(Clone, Debug)]
pub enum Column<'a> {
	Resolved(ResolvedColumn<'a>),
	SourceQualified(SourceQualified<'a>),
	ColumnQualified(ColumnQualified<'a>),
}

#[derive(Clone, Debug)]
pub struct SourceQualified<'a> {
	pub source: Fragment<'a>,
	pub name: Fragment<'a>,
	pub data: ColumnData,
}

#[derive(Clone, Debug)]
pub struct ColumnQualified<'a> {
	pub name: Fragment<'a>,
	pub data: ColumnData,
}

#[derive(Clone, Debug)]
pub struct ResolvedColumn<'a> {
	pub column: RColumn<'a>,
	pub data: ColumnData,
}

impl<'a> ResolvedColumn<'a> {
	/// Create a new ResolvedColumn from a resolved column and data
	pub fn new(column: RColumn<'a>, data: ColumnData) -> Self {
		Self {
			column,
			data,
		}
	}
}

impl<'a> PartialEq for ResolvedColumn<'a> {
	fn eq(&self, other: &Self) -> bool {
		self.column.fully_qualified_name() == other.column.fully_qualified_name() && self.data == other.data
	}
}

impl<'a> PartialEq for SourceQualified<'a> {
	fn eq(&self, other: &Self) -> bool {
		self.source == other.source && self.name == other.name && self.data == other.data
	}
}

impl<'a> PartialEq for ColumnQualified<'a> {
	fn eq(&self, other: &Self) -> bool {
		self.name == other.name && self.data == other.data
	}
}

impl<'a> PartialEq for Column<'a> {
	fn eq(&self, other: &Self) -> bool {
		match (self, other) {
			(Self::Resolved(a), Self::Resolved(b)) => a == b,
			(Self::SourceQualified(a), Self::SourceQualified(b)) => a == b,
			(Self::ColumnQualified(a), Self::ColumnQualified(b)) => a == b,
			_ => false,
		}
	}
}

impl<'a> Column<'a> {
	/// Create a resolved column variant
	pub fn resolved(column: RColumn<'a>, data: ColumnData) -> Self {
		Self::Resolved(ResolvedColumn::new(column, data))
	}

	pub fn get_type(&self) -> Type {
		match self {
			Self::Resolved(col) => col.data.get_type(),
			Self::SourceQualified(col) => col.data.get_type(),
			Self::ColumnQualified(col) => col.data.get_type(),
		}
	}

	pub fn qualified_name(&self) -> String {
		match self {
			Self::Resolved(col) => col.column.fully_qualified_name(),
			Self::SourceQualified(col) => {
				format!("{}.{}", col.source.text(), col.name.text())
			}
			Self::ColumnQualified(col) => col.name.text().to_string(),
		}
	}

	pub fn with_new_data(&self, data: ColumnData) -> Column<'a> {
		match self {
			Self::Resolved(col) => Self::Resolved(ResolvedColumn {
				column: col.column.clone(),
				data,
			}),
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
			Self::Resolved(col) => col.column.fragment(),
			Self::SourceQualified(col) => &col.name,
			Self::ColumnQualified(col) => &col.name,
		}
	}

	pub fn name_owned(&self) -> Fragment<'a> {
		self.name().clone()
	}

	pub fn source(&self) -> Option<&Fragment<'a>> {
		match self {
			Self::Resolved(_) => None, // TODO: could extract source name from ResolvedColumn
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
			Self::Resolved(_) => None, // TODO: could extract namespace from ResolvedColumn
			Self::SourceQualified(_) => None,
			Self::ColumnQualified(_) => None,
		}
	}

	pub fn data(&self) -> &ColumnData {
		match self {
			Self::Resolved(col) => &col.data,
			Self::SourceQualified(col) => &col.data,
			Self::ColumnQualified(col) => &col.data,
		}
	}

	pub fn data_mut(&mut self) -> &mut ColumnData {
		match self {
			Self::Resolved(col) => &mut col.data,
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
