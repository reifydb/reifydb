// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ops::Deref;

use reifydb_type::{Fragment, Type, TypeConstraint, diagnostic::number::NumberOfRangeColumnDescriptor};
use serde::{Deserialize, Serialize};

use super::policy::{ColumnPolicyKind, ColumnSaturationPolicy, DEFAULT_COLUMN_SATURATION_POLICY};
use crate::interface::{ColumnId, ColumnPolicy};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
	pub id: ColumnId,
	pub name: String,
	pub constraint: TypeConstraint,
	pub policies: Vec<ColumnPolicy>,
	pub index: ColumnIndex,
	pub auto_increment: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDescriptor<'a> {
	// Location information
	pub namespace: Option<Fragment<'a>>,
	pub table: Option<Fragment<'a>>,
	pub column: Option<Fragment<'a>>,

	// Column metadata
	pub column_type: Option<Type>,
	pub policies: Vec<ColumnPolicyKind>,
}

impl<'a> ColumnDescriptor<'a> {
	pub fn new() -> Self {
		Self {
			namespace: None,
			table: None,
			column: None,
			column_type: None,
			policies: Vec::new(),
		}
	}

	pub fn with_namespace(mut self, namespace: Fragment<'a>) -> Self {
		self.namespace = Some(namespace);
		self
	}

	pub fn with_table(mut self, table: Fragment<'a>) -> Self {
		self.table = Some(table);
		self
	}

	pub fn with_column(mut self, column: Fragment<'a>) -> Self {
		self.column = Some(column);
		self
	}

	pub fn with_column_type(mut self, column_type: Type) -> Self {
		self.column_type = Some(column_type);
		self
	}

	pub fn with_policies(mut self, policies: Vec<ColumnPolicyKind>) -> Self {
		self.policies = policies;
		self
	}

	// Policy methods
	pub fn saturation_policy(&self) -> &ColumnSaturationPolicy {
		self.policies
			.iter()
			.find_map(|p| match p {
				ColumnPolicyKind::Saturation(policy) => Some(policy),
			})
			.unwrap_or(&DEFAULT_COLUMN_SATURATION_POLICY)
	}

	// Convert to NumberOfRangeColumnDescriptor for error reporting
	// Returns a descriptor with the same lifetime as self
	pub fn to_number_range_descriptor(&self) -> NumberOfRangeColumnDescriptor<'_> {
		let mut descriptor = NumberOfRangeColumnDescriptor::new();

		if let Some(ref namespace) = self.namespace {
			descriptor = descriptor.with_namespace(namespace.text());
		}
		if let Some(ref table) = self.table {
			descriptor = descriptor.with_table(table.text());
		}
		if let Some(ref column) = self.column {
			descriptor = descriptor.with_column(column.text());
		}
		if let Some(column_type) = self.column_type {
			descriptor = descriptor.with_column_type(column_type);
		}
		descriptor
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnIndex(pub u16);

impl Deref for ColumnIndex {
	type Target = u16;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u16> for ColumnIndex {
	fn eq(&self, other: &u16) -> bool {
		self.0.eq(other)
	}
}

impl From<ColumnIndex> for u16 {
	fn from(value: ColumnIndex) -> Self {
		value.0
	}
}
