// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::{JoinType, WindowKind},
	interface::catalog::{
		flow::{FlowEdgeId, FlowId, FlowNodeId},
		id::{RingBufferId, SeriesId, SubscriptionId, TableId, ViewId},
		series::SeriesKey,
		shape::ShapeId,
	},
	sort::SortKey,
};
use reifydb_value::value::{dictionary::DictionaryId, duration::Duration};
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
		snapshot: bool,
		#[serde(default)]
		natural: bool,
		#[serde(default)]
		latest: bool,
	},
	Aggregate {
		by: Vec<Expression>,
		map: Vec<Expression>,
	},
	Append {},
	Sort {
		by: Vec<SortKey>,
	},
	Take {
		limit: usize,
	},
	Distinct {
		expressions: Vec<Expression>,
	},
	Apply {
		operator: String,
		expressions: Vec<Expression>,
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
		lateness: Option<Duration>,
	},
	SourceDictionary {
		dictionary: DictionaryId,
	},
}

impl FlowNodeType {
	pub fn ticks(&self) -> bool {
		matches!(
			self,
			FlowNodeType::Append { .. }
				| FlowNodeType::Distinct { .. }
				| FlowNodeType::Window { .. }
				| FlowNodeType::Apply { .. } | FlowNodeType::Join { .. }
		)
	}

	pub fn label(&self) -> String {
		match self {
			FlowNodeType::SourceInlineData {
				..
			} => "SourceInlineData".into(),
			FlowNodeType::SourceTable {
				..
			} => "SourceTable".into(),
			FlowNodeType::SourceView {
				..
			} => "SourceView".into(),
			FlowNodeType::SourceFlow {
				..
			} => "SourceFlow".into(),
			FlowNodeType::SourceRingBuffer {
				..
			} => "SourceRingBuffer".into(),
			FlowNodeType::SourceSeries {
				..
			} => "SourceSeries".into(),
			FlowNodeType::SourceDictionary {
				..
			} => "SourceDictionary".into(),
			FlowNodeType::Filter {
				..
			} => "Filter".into(),
			FlowNodeType::Gate {
				..
			} => "Gate".into(),
			FlowNodeType::Map {
				..
			} => "Map".into(),
			FlowNodeType::Extend {
				..
			} => "Extend".into(),
			FlowNodeType::Join {
				..
			} => "Join".into(),
			FlowNodeType::Aggregate {
				..
			} => "Aggregate".into(),
			FlowNodeType::Append {
				..
			} => "Append".into(),
			FlowNodeType::Sort {
				..
			} => "Sort".into(),
			FlowNodeType::Take {
				..
			} => "Take".into(),
			FlowNodeType::Distinct {
				..
			} => "Distinct".into(),
			FlowNodeType::Apply {
				operator,
				..
			} => format!("Apply({})", operator),
			FlowNodeType::SinkTableView {
				..
			} => "SinkTableView".into(),
			FlowNodeType::SinkRingBufferView {
				..
			} => "SinkRingBufferView".into(),
			FlowNodeType::SinkSeriesView {
				..
			} => "SinkSeriesView".into(),
			FlowNodeType::SinkSubscription {
				..
			} => "SinkSubscription".into(),
			FlowNodeType::Window {
				..
			} => "Window".into(),
		}
	}

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
			FlowNodeType::Append {
				..
			} => 9,
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
			FlowNodeType::SourceDictionary {
				..
			} => 22,
		}
	}

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
			FlowNodeType::SourceDictionary {
				dictionary,
			} => Some(ShapeId::dictionary(*dictionary)),
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
			| FlowNodeType::Append {
				..
			}
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

#[cfg(test)]
mod tests {
	use reifydb_core::common::JoinType;

	use super::FlowNodeType;

	fn join() -> FlowNodeType {
		FlowNodeType::Join {
			join_type: JoinType::Inner,
			left: vec![],
			right: vec![],
			alias: None,
			snapshot: false,
			natural: false,
			latest: false,
		}
	}

	#[test]
	fn join_always_requests_ticks() {
		// Join state TTL is reclaimed by the background operator GC actor (per-side, via
		// OperatorSettings), not on the flow tick path - so a Join node never requests ticks.
		assert!(join().ticks());
	}

	#[test]
	fn apply_always_requests_ticks() {
		// Apply nodes always register for flow ticks, regardless of the underlying operator's
		// tick capability. The graph-level gate cannot see the runtime operator, so it
		// registers unconditionally; the runtime operator then decides whether tick() actually
		// runs (an FFI operator without CAPABILITY_TICK reports no interval and is skipped).
		// Registering here is what lets a tick-capable custom operator be ticked at all.
		let apply = FlowNodeType::Apply {
			operator: "compute_swap_volumes".to_string(),
			expressions: vec![],
		};
		assert!(apply.ticks());
	}

	#[test]
	fn append_and_distinct_always_request_ticks() {
		// Their TTL now lives in OperatorSettings (not the node) and is reclaimed on tick when
		// configured; the graph-level gate cannot see it, so they request ticks unconditionally and
		// the runtime operator decides whether tick() actually runs.
		assert!(FlowNodeType::Append {}.ticks());
		assert!(FlowNodeType::Distinct {
			expressions: vec![]
		}
		.ticks());
	}

	#[test]
	fn stateless_nodes_do_not_request_ticks() {
		assert!(!FlowNodeType::Map {
			expressions: vec![]
		}
		.ticks());
		assert!(!FlowNodeType::Filter {
			conditions: vec![]
		}
		.ticks());
	}
}
