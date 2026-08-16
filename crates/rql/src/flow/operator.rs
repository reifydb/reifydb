// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::{JoinType, TimeDomain, WindowKind},
	interface::catalog::{
		flow::{FlowEdgeId, OperatorId},
		id::{RingBufferId, SeriesId, SubscriptionId, TableId, ViewId},
		object::ObjectId,
		series::SeriesKey,
	},
	sort::SortKey,
};
use reifydb_value::value::duration::Duration;
use serde::{Deserialize, Serialize};

use crate::expression::Expression;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperatorDef {
	SourceInlineData {},
	SourceTable {
		table: TableId,
		time_domain: TimeDomain,
	},
	SourceView {
		view: ViewId,
	},
	SourceRingBuffer {
		ringbuffer: RingBufferId,
		time_domain: TimeDomain,
	},
	SourceSeries {
		series: SeriesId,
		time_domain: TimeDomain,
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
		lateness: Duration,
		immutable: Option<Duration>,
	},
}

impl OperatorDef {
	pub fn is_source(&self) -> bool {
		matches!(
			self,
			OperatorDef::SourceInlineData {}
				| OperatorDef::SourceTable { .. }
				| OperatorDef::SourceView { .. }
				| OperatorDef::SourceRingBuffer { .. }
				| OperatorDef::SourceSeries { .. }
		)
	}

	pub fn declares_time(&self) -> bool {
		match self {
			OperatorDef::SourceTable {
				time_domain,
				..
			}
			| OperatorDef::SourceRingBuffer {
				time_domain,
				..
			}
			| OperatorDef::SourceSeries {
				time_domain,
				..
			} => *time_domain != TimeDomain::None,
			OperatorDef::SourceView {
				..
			} => true,
			_ => false,
		}
	}

	pub fn ticks(&self) -> bool {
		matches!(
			self,
			OperatorDef::Append { .. }
				| OperatorDef::Distinct { .. }
				| OperatorDef::Window { .. } | OperatorDef::Apply { .. }
				| OperatorDef::Join { .. } | OperatorDef::Aggregate { .. }
				| OperatorDef::SinkRingBufferView { .. }
		)
	}

	pub fn label(&self) -> String {
		match self {
			OperatorDef::SourceInlineData {
				..
			} => "SourceInlineData".into(),
			OperatorDef::SourceTable {
				..
			} => "SourceTable".into(),
			OperatorDef::SourceView {
				..
			} => "SourceView".into(),
			OperatorDef::SourceRingBuffer {
				..
			} => "SourceRingBuffer".into(),
			OperatorDef::SourceSeries {
				..
			} => "SourceSeries".into(),
			OperatorDef::Filter {
				..
			} => "Filter".into(),
			OperatorDef::Gate {
				..
			} => "Gate".into(),
			OperatorDef::Map {
				..
			} => "Map".into(),
			OperatorDef::Extend {
				..
			} => "Extend".into(),
			OperatorDef::Join {
				..
			} => "Join".into(),
			OperatorDef::Aggregate {
				..
			} => "Aggregate".into(),
			OperatorDef::Append {
				..
			} => "Append".into(),
			OperatorDef::Sort {
				..
			} => "Sort".into(),
			OperatorDef::Take {
				..
			} => "Take".into(),
			OperatorDef::Distinct {
				..
			} => "Distinct".into(),
			OperatorDef::Apply {
				operator,
				..
			} => format!("Apply({})", operator),
			OperatorDef::SinkTableView {
				..
			} => "SinkTableView".into(),
			OperatorDef::SinkRingBufferView {
				..
			} => "SinkRingBufferView".into(),
			OperatorDef::SinkSeriesView {
				..
			} => "SinkSeriesView".into(),
			OperatorDef::SinkSubscription {
				..
			} => "SinkSubscription".into(),
			OperatorDef::Window {
				..
			} => "Window".into(),
		}
	}

	pub fn discriminator(&self) -> u8 {
		match self {
			OperatorDef::SourceInlineData {
				..
			} => 0,
			OperatorDef::SourceTable {
				..
			} => 1,
			OperatorDef::SourceView {
				..
			} => 2,
			OperatorDef::Filter {
				..
			} => 4,
			OperatorDef::Map {
				..
			} => 5,
			OperatorDef::Extend {
				..
			} => 6,
			OperatorDef::Join {
				..
			} => 7,
			OperatorDef::Aggregate {
				..
			} => 8,
			OperatorDef::Append {
				..
			} => 9,
			OperatorDef::Sort {
				..
			} => 10,
			OperatorDef::Take {
				..
			} => 11,
			OperatorDef::Distinct {
				..
			} => 12,
			OperatorDef::Apply {
				..
			} => 13,
			OperatorDef::SinkSubscription {
				..
			} => 14,
			OperatorDef::Window {
				..
			} => 15,
			OperatorDef::SourceRingBuffer {
				..
			} => 16,
			OperatorDef::SourceSeries {
				..
			} => 17,
			OperatorDef::Gate {
				..
			} => 18,
			OperatorDef::SinkTableView {
				..
			} => 19,
			OperatorDef::SinkRingBufferView {
				..
			} => 20,
			OperatorDef::SinkSeriesView {
				..
			} => 21,
		}
	}

	pub fn source_object_id(&self) -> Option<ObjectId> {
		match self {
			OperatorDef::SourceTable {
				table,
				..
			} => Some(ObjectId::table(*table)),
			OperatorDef::SourceRingBuffer {
				ringbuffer,
				..
			} => Some(ObjectId::ringbuffer(*ringbuffer)),
			OperatorDef::SourceSeries {
				series,
				..
			} => Some(ObjectId::series(*series)),
			OperatorDef::SourceInlineData {
				..
			}
			| OperatorDef::SourceView {
				..
			}
			| OperatorDef::Filter {
				..
			}
			| OperatorDef::Gate {
				..
			}
			| OperatorDef::Map {
				..
			}
			| OperatorDef::Extend {
				..
			}
			| OperatorDef::Join {
				..
			}
			| OperatorDef::Aggregate {
				..
			}
			| OperatorDef::Append {
				..
			}
			| OperatorDef::Sort {
				..
			}
			| OperatorDef::Take {
				..
			}
			| OperatorDef::Distinct {
				..
			}
			| OperatorDef::Apply {
				..
			}
			| OperatorDef::SinkTableView {
				..
			}
			| OperatorDef::SinkRingBufferView {
				..
			}
			| OperatorDef::SinkSeriesView {
				..
			}
			| OperatorDef::SinkSubscription {
				..
			}
			| OperatorDef::Window {
				..
			} => None,
		}
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowNode {
	pub id: OperatorId,
	pub ty: OperatorDef,
	pub inputs: Vec<OperatorId>,
	pub outputs: Vec<OperatorId>,
}

impl FlowNode {
	pub fn new(id: impl Into<OperatorId>, ty: OperatorDef) -> Self {
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
	pub source: OperatorId,
	pub target: OperatorId,
}

impl FlowEdge {
	pub fn new(id: impl Into<FlowEdgeId>, source: impl Into<OperatorId>, target: impl Into<OperatorId>) -> Self {
		Self {
			id: id.into(),
			source: source.into(),
			target: target.into(),
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		common::JoinType,
		interface::catalog::id::{RingBufferId, ViewId},
	};

	use super::OperatorDef;

	fn join() -> OperatorDef {
		OperatorDef::Join {
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
		// A join's per-side TTL lives in OperatorSettings, where the graph-level gate cannot see it, so the
		// node requests ticks unconditionally and the runtime operator decides.
		assert!(join().ticks());
	}

	#[test]
	fn apply_always_requests_ticks() {
		// The graph-level gate cannot see the runtime operator, so it must register unconditionally and let
		// the operator decide; without that a tick-capable custom operator could never be ticked at all.
		let apply = OperatorDef::Apply {
			operator: "compute_swap_volumes".to_string(),
			expressions: vec![],
		};
		assert!(apply.ticks());
	}

	#[test]
	fn append_and_distinct_always_request_ticks() {
		// Their TTL lives in OperatorSettings rather than the node, where the graph-level gate cannot see it,
		// so they have to request ticks unconditionally and let the runtime operator decide.
		assert!(OperatorDef::Append {}.ticks());
		assert!(OperatorDef::Distinct {
			expressions: vec![]
		}
		.ticks());
	}

	#[test]
	fn sink_ringbuffer_view_always_requests_ticks() {
		// The row TTL lives in row settings, not the node, so the graph-level gate cannot see it. Without an
		// unconditional request the flow is never scheduled to tick and quiet partitions leak forever.
		assert!(OperatorDef::SinkRingBufferView {
			view: ViewId(1),
			ringbuffer: RingBufferId(1),
			capacity: 1,
		}
		.ticks());
	}

	#[test]
	fn stateless_nodes_do_not_request_ticks() {
		assert!(!OperatorDef::Map {
			expressions: vec![]
		}
		.ticks());
		assert!(!OperatorDef::Filter {
			conditions: vec![]
		}
		.ticks());
	}
}
