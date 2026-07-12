// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use serde::{Deserialize, Serialize};

use crate::interface::catalog::{
	column::Column,
	id::{NamespaceId, SegmentTreeId},
	key::{KeySpec, PrimaryKey},
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SegmentTreeAggregate {
	pub name: String,
	pub monoid: String,
	pub column: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SegmentTree {
	pub id: SegmentTreeId,
	pub namespace: NamespaceId,
	pub name: String,
	pub columns: Vec<Column>,
	pub key: KeySpec,
	pub aggregates: Vec<SegmentTreeAggregate>,
	pub primary_key: Option<PrimaryKey>,
	pub partition_by: Vec<String>,
	pub underlying: bool,
}

impl SegmentTree {
	pub fn name(&self) -> &str {
		&self.name
	}

	pub fn render_aggregates(&self) -> String {
		self.aggregates
			.iter()
			.map(|a| format!("{}: {}({})", a.name, a.monoid, a.column))
			.collect::<Vec<_>>()
			.join(", ")
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SegmentTreeMetadata {
	pub id: SegmentTreeId,
	pub row_count: u64,
	pub oldest_key: u64,
	pub newest_key: u64,
	pub sequence_counter: u64,
}

impl SegmentTreeMetadata {
	pub fn new(segment_tree_id: SegmentTreeId) -> Self {
		Self {
			id: segment_tree_id,
			row_count: 0,
			oldest_key: 0,
			newest_key: 0,
			sequence_counter: 0,
		}
	}
}
