// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::{Fragment, Type};

mod columns;
mod computed;
mod data;
pub mod frame;
pub mod layout;
#[allow(dead_code, unused_variables)]
pub mod pool;
pub mod push;
mod transform;
mod view;

pub use columns::Columns;
pub use data::ColumnData;
pub use view::group_by::{GroupByView, GroupKey};

use crate::interface::ResolvedColumn;

#[derive(Clone, Debug)]
pub enum Column<'a> {
	Resolved(ColumnResolved<'a>),
	SourceQualified(SourceQualified<'a>),
	Computed(ColumnComputed<'a>),
}

#[derive(Clone, Debug)]
pub struct SourceQualified<'a> {
	pub source: Fragment<'a>,
	pub name: Fragment<'a>,
	pub data: ColumnData,
}

#[derive(Clone, Debug)]
pub struct ColumnComputed<'a> {
	pub name: Fragment<'a>,
	pub data: ColumnData,
}

#[derive(Clone, Debug)]
pub struct ColumnResolved<'a> {
	pub column: ResolvedColumn<'a>,
	pub data: ColumnData,
}

impl<'a> ColumnResolved<'a> {
	/// Create a new ResolvedColumn from a resolved column and data
	pub fn new(column: ResolvedColumn<'a>, data: ColumnData) -> Self {
		Self {
			column,
			data,
		}
	}
}

impl<'a> PartialEq for ColumnResolved<'a> {
	fn eq(&self, other: &Self) -> bool {
		self.column.qualified_name() == other.column.qualified_name() && self.data == other.data
	}
}

impl<'a> PartialEq for SourceQualified<'a> {
	fn eq(&self, other: &Self) -> bool {
		self.source == other.source && self.name == other.name && self.data == other.data
	}
}

impl<'a> PartialEq for ColumnComputed<'a> {
	fn eq(&self, other: &Self) -> bool {
		self.name == other.name && self.data == other.data
	}
}

impl<'a> PartialEq for Column<'a> {
	fn eq(&self, other: &Self) -> bool {
		match (self, other) {
			(Self::Resolved(a), Self::Resolved(b)) => a == b,
			(Self::SourceQualified(a), Self::SourceQualified(b)) => a == b,
			(Self::Computed(a), Self::Computed(b)) => a == b,
			_ => false,
		}
	}
}

impl<'a> Column<'a> {
	pub fn get_type(&self) -> Type {
		match self {
			Self::Resolved(col) => col.data.get_type(),
			Self::SourceQualified(col) => col.data.get_type(),
			Self::Computed(col) => col.data.get_type(),
		}
	}

	pub fn qualified_name(&self) -> String {
		match self {
			Self::Resolved(col) => col.column.qualified_name(),
			Self::SourceQualified(col) => {
				format!("{}.{}", col.source.text(), col.name.text())
			}
			Self::Computed(col) => col.name.text().to_string(),
		}
	}

	pub fn with_new_data(&self, data: ColumnData) -> Column<'a> {
		match self {
			Self::Resolved(col) => Self::Resolved(ColumnResolved {
				column: col.column.clone(),
				data,
			}),
			Self::SourceQualified(col) => Self::SourceQualified(SourceQualified {
				source: col.source.clone(),
				name: col.name.clone(),
				data,
			}),
			Self::Computed(col) => Self::Computed(ColumnComputed {
				name: col.name.clone(),
				data,
			}),
		}
	}

	pub fn name(&self) -> &Fragment<'a> {
		match self {
			Self::Resolved(col) => col.column.fragment(),
			Self::SourceQualified(col) => &col.name,
			Self::Computed(col) => &col.name,
		}
	}

	pub fn name_owned(&self) -> Fragment<'a> {
		self.name().clone()
	}

	pub fn source(&self) -> Option<Fragment<'a>> {
		match self {
			Self::Resolved(col) => Some(col.column.source().identifier().clone()),
			Self::SourceQualified(col) => Some(col.source.clone()),
			Self::Computed(_) => None,
		}
	}

	pub fn namespace(&self) -> Option<&Fragment<'a>> {
		match self {
			Self::Resolved(col) => col.column.namespace().map(|ns| ns.fragment()),
			Self::SourceQualified(_) => None,
			Self::Computed(_) => None,
		}
	}

	pub fn data(&self) -> &ColumnData {
		match self {
			Self::Resolved(col) => &col.data,
			Self::SourceQualified(col) => &col.data,
			Self::Computed(col) => &col.data,
		}
	}

	pub fn data_mut(&mut self) -> &mut ColumnData {
		match self {
			Self::Resolved(col) => &mut col.data,
			Self::SourceQualified(col) => &mut col.data,
			Self::Computed(col) => &mut col.data,
		}
	}

	/// Convert to a 'static lifetime version
	pub fn to_static(&self) -> Column<'static> {
		match self {
			Self::Resolved(col) => Column::Resolved(ColumnResolved {
				column: col.column.to_static(),
				data: col.data.clone(),
			}),
			Self::SourceQualified(col) => Column::SourceQualified(SourceQualified {
				source: col.source.clone().to_static(),
				name: col.name.clone().to_static(),
				data: col.data.clone(),
			}),
			Self::Computed(col) => Column::Computed(ColumnComputed {
				name: col.name.clone().to_static(),
				data: col.data.clone(),
			}),
		}
	}
}
