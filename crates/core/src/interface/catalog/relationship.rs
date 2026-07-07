// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use serde::{Deserialize, Serialize};

use crate::interface::catalog::id::{ColumnId, NamespaceId, RelationshipId, TableId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RelationshipCardinality {
	OneToOne,
	ManyToOne,
	OneToMany,
	ManyToMany,
}

impl RelationshipCardinality {
	pub fn as_str(&self) -> &'static str {
		match self {
			Self::OneToOne => "1:1",
			Self::ManyToOne => "N:1",
			Self::OneToMany => "1:N",
			Self::ManyToMany => "N:M",
		}
	}

	pub fn from_code(code: u8) -> Option<Self> {
		match code {
			0 => Some(Self::OneToOne),
			1 => Some(Self::ManyToOne),
			2 => Some(Self::OneToMany),
			3 => Some(Self::ManyToMany),
			_ => None,
		}
	}

	pub fn as_code(&self) -> u8 {
		match self {
			Self::OneToOne => 0,
			Self::ManyToOne => 1,
			Self::OneToMany => 2,
			Self::ManyToMany => 3,
		}
	}

	pub fn requires_junction(&self) -> bool {
		matches!(self, Self::ManyToMany)
	}

	pub fn is_list(&self) -> bool {
		matches!(self, Self::OneToMany | Self::ManyToMany)
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationshipJunction {
	pub table: TableId,
	pub source_column: ColumnId,
	pub target_column: ColumnId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Relationship {
	pub id: RelationshipId,
	pub namespace: NamespaceId,
	pub name: String,
	pub source_table: TableId,
	pub source_column: ColumnId,
	pub target_table: TableId,
	pub target_column: ColumnId,
	pub junction: Option<RelationshipJunction>,
	pub cardinality: RelationshipCardinality,
}
