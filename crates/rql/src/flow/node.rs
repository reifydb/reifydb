// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::{JoinType, WindowKind},
	interface::catalog::{
		flow::{FlowEdgeId, FlowId, FlowNodeId},
		id::{RingBufferId, SeriesId, SubscriptionId, TableId, ViewId},
		series::SeriesKey,
		shape::ShapeId,
	},
	row::Ttl,
	sort::SortKey,
};
use serde::{Deserialize, Serialize};

use crate::expression::Expression;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FlowNodeType {
	SourceInlineData {},
	SourceTable {
		table: TableId,
	},
	SourceView {
		view: ViewId,
	},
	SourceFlow {
		flow: FlowId,
	},
	SourceRingBuffer {
		ringbuffer: RingBufferId,
	},
	SourceSeries {
		series: SeriesId,
	},
	Filter {
		conditions: Vec<Expression>,
	},
	Gate {
		conditions: Vec<Expression>,
	},
	Map {
		expressions: Vec<Expression>,
	},
	Extend {
		expressions: Vec<Expression>,
	},
	Join {
		join_type: JoinType,
		left: Vec<Expression>,
		right: Vec<Expression>,
		alias: Option<String>,
		#[serde(default)]
		ttl: Option<Ttl>,
	},
	Aggregate {
		by: Vec<Expression>,
		map: Vec<Expression>,
	},
	Append,
	Sort {
		by: Vec<SortKey>,
	},
	Take {
		limit: usize,
	},
	Distinct {
		expressions: Vec<Expression>,
		#[serde(default)]
		ttl: Option<Ttl>,
	},
	Apply {
		operator: String,
		expressions: Vec<Expression>,
		#[serde(default)]
		ttl: Option<Ttl>,
	},
	SinkTableView {
		view: ViewId,
		table: TableId,
	},
	SinkRingBufferView {
		view: ViewId,
		ringbuffer: RingBufferId,
		capacity: u64,
		propagate_evictions: bool,
	},
	SinkSeriesView {
		view: ViewId,
		series: SeriesId,
		key: SeriesKey,
	},
	SinkSubscription {
		subscription: SubscriptionId,
	},
	Window {
		kind: WindowKind,
		group_by: Vec<Expression>,
		aggregations: Vec<Expression>,
		ts: Option<String>,
	},
}

impl FlowNodeType {
	/// Returns a discriminator value for this node type variant.
	/// Must match indices in FLOW_NODE_TYPE_NAMES in catalog/vtable/system/flow_node_types.rs
	pub fn discriminator(&self) -> u8 {
		match self {
			FlowNodeType::SourceInlineData {
				..
			} => 0,
			FlowNodeType::SourceTable {
				..
			} => 1,
			FlowNodeType::SourceView {
				..
			} => 2,
			FlowNodeType::SourceFlow {
				..
			} => 3,
			FlowNodeType::Filter {
				..
			} => 4,
			FlowNodeType::Map {
				..
			} => 5,
			FlowNodeType::Extend {
				..
			} => 6,
			FlowNodeType::Join {
				..
			} => 7,
			FlowNodeType::Aggregate {
				..
			} => 8,
			FlowNodeType::Append => 9,
			FlowNodeType::Sort {
				..
			} => 10,
			FlowNodeType::Take {
				..
			} => 11,
			FlowNodeType::Distinct {
				..
			} => 12,
			FlowNodeType::Apply {
				..
			} => 13,
			FlowNodeType::SinkSubscription {
				..
			} => 14,
			FlowNodeType::Window {
				..
			} => 15,
			FlowNodeType::SourceRingBuffer {
				..
			} => 16,
			FlowNodeType::SourceSeries {
				..
			} => 17,
			FlowNodeType::Gate {
				..
			} => 18,
			FlowNodeType::SinkTableView {
				..
			} => 19,
			FlowNodeType::SinkRingBufferView {
				..
			} => 20,
			FlowNodeType::SinkSeriesView {
				..
			} => 21,
		}
	}

	/// If this node is a primitive data source (table, ring buffer, or series),
	/// returns its [`ShapeId`]. Returns `None` for all other node types.
	///
	/// Uses an exhaustive match so that adding a new variant to [`FlowNodeType`]
	/// produces a compiler error, forcing the author to decide whether the new
	/// variant is a primitive source.
	pub fn primitive_source_shape_id(&self) -> Option<ShapeId> {
		match self {
			FlowNodeType::SourceTable {
				table,
			} => Some(ShapeId::table(*table)),
			FlowNodeType::SourceRingBuffer {
				ringbuffer,
			} => Some(ShapeId::ringbuffer(*ringbuffer)),
			FlowNodeType::SourceSeries {
				series,
			} => Some(ShapeId::series(*series)),
			FlowNodeType::SourceInlineData {
				..
			}
			| FlowNodeType::SourceView {
				..
			}
			| FlowNodeType::SourceFlow {
				..
			}
			| FlowNodeType::Filter {
				..
			}
			| FlowNodeType::Gate {
				..
			}
			| FlowNodeType::Map {
				..
			}
			| FlowNodeType::Extend {
				..
			}
			| FlowNodeType::Join {
				..
			}
			| FlowNodeType::Aggregate {
				..
			}
			| FlowNodeType::Append
			| FlowNodeType::Sort {
				..
			}
			| FlowNodeType::Take {
				..
			}
			| FlowNodeType::Distinct {
				..
			}
			| FlowNodeType::Apply {
				..
			}
			| FlowNodeType::SinkTableView {
				..
			}
			| FlowNodeType::SinkRingBufferView {
				..
			}
			| FlowNodeType::SinkSeriesView {
				..
			}
			| FlowNodeType::SinkSubscription {
				..
			}
			| FlowNodeType::Window {
				..
			} => None,
		}
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowNode {
	pub id: FlowNodeId,
	pub ty: FlowNodeType,
	pub inputs: Vec<FlowNodeId>,
	pub outputs: Vec<FlowNodeId>,
}

impl FlowNode {
	pub fn new(id: impl Into<FlowNodeId>, ty: FlowNodeType) -> Self {
		Self {
			id: id.into(),
			ty,
			inputs: Vec::new(),
			outputs: Vec::new(),
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FlowEdge {
	pub id: FlowEdgeId,
	pub source: FlowNodeId,
	pub target: FlowNodeId,
}

impl FlowEdge {
	pub fn new(id: impl Into<FlowEdgeId>, source: impl Into<FlowNodeId>, target: impl Into<FlowNodeId>) -> Self {
		Self {
			id: id.into(),
			source: source.into(),
			target: target.into(),
		}
	}
}
