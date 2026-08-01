// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::{JoinType, WindowKind},
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
	},
	SourceView {
		view: ViewId,
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
		grace: Duration,
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

	pub fn holds_state(&self) -> bool {
		matches!(
			self,
			OperatorDef::Join { .. }
				| OperatorDef::Distinct { .. }
				| OperatorDef::Append { .. } | OperatorDef::Apply { .. }
				| OperatorDef::Aggregate { .. }
				| OperatorDef::Window { .. }
		)
	}

	pub fn consults_declared_span(&self) -> bool {
		matches!(
			self,
			OperatorDef::Join { .. }
				| OperatorDef::Distinct { .. }
				| OperatorDef::Append { .. } | OperatorDef::Apply { .. }
				| OperatorDef::Aggregate { .. }
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
			} => Some(ObjectId::table(*table)),
			OperatorDef::SourceRingBuffer {
				ringbuffer,
			} => Some(ObjectId::ringbuffer(*ringbuffer)),
			OperatorDef::SourceSeries {
				series,
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
		common::{JoinType, WindowKind, WindowSize},
		interface::catalog::id::{RingBufferId, ViewId},
		row::{JoinTtl, OperatorSettings, OperatorTtl},
	};
	use reifydb_value::value::duration::Duration;

	use super::OperatorDef;

	fn ms(milliseconds: i64) -> Duration {
		Duration::from_milliseconds(milliseconds).expect("test duration must be representable")
	}

	fn window(kind: WindowKind, grace: Duration) -> OperatorDef {
		OperatorDef::Window {
			kind,
			group_by: vec![],
			aggregations: vec![],
			grace,
		}
	}

	fn apply() -> OperatorDef {
		OperatorDef::Apply {
			operator: "custom".into(),
			expressions: vec![],
		}
	}

	#[test]
	fn a_window_node_consults_no_declared_span_because_its_operator_owns_that_answer() {
		// A window's retention is intrinsic (span + grace) and the operator computes it. Two sources for one
		// fact is how a node stamps activity in one domain while the substrate ages it in another, which is
		// silent in release and reclaims live state.
		let node = window(
			WindowKind::Tumbling {
				size: WindowSize::Duration(ms(60_000)),
			},
			ms(5_000),
		);

		assert!(!node.consults_declared_span(), "a declared ttl must never reach a window");
	}

	#[test]
	fn the_node_types_that_accept_a_declared_span_are_exactly_those_that_keep_keyed_state() {
		// The substrate cannot know what keyed state means or how it ages, so the ttl clause is the only
		// source. An aggregate is the case worth naming: its groups are reclaimable, but any future row with
		// the same key folds into the accumulator, so nothing can derive when a key is done.
		for node in [
			apply(),
			join(),
			OperatorDef::Aggregate {
				by: vec![],
				map: vec![],
			},
			OperatorDef::Distinct {
				expressions: vec![],
			},
			OperatorDef::Append {},
		] {
			assert!(node.consults_declared_span(), "{node:?} keeps keyed state and must accept a span");
		}

		// Filter and map hold nothing per group, so accepting a span here takes a declaration the substrate
		// then silently ignores.
		for node in [
			OperatorDef::Filter {
				conditions: vec![],
			},
			OperatorDef::Map {
				expressions: vec![],
			},
		] {
			assert!(!node.consults_declared_span(), "{node:?} keeps no keyed state");
		}
	}

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
	fn every_node_that_can_reclaim_also_requests_ticks() {
		// Reclamation runs only on the tick path, and a flow ticks only if some node asks for it. A node that
		// derives a horizon but answers false to ticks() accumulates state nothing ever scans, and the
		// retention report calls it healthy because the driver never ran for it.
		let ttl = OperatorSettings {
			ttl: Some(OperatorTtl {
				duration: ms(60_000),
			}),
			join: None,
		};
		// Both sides must be declared: reclaiming one side while the other still holds the group changes the
		// join's output rather than just freeing memory.
		let join_ttl = OperatorSettings {
			ttl: None,
			join: Some(JoinTtl {
				left: Some(OperatorTtl {
					duration: ms(60_000),
				}),
				right: Some(OperatorTtl {
					duration: ms(60_000),
				}),
			}),
		};

		// This list is the assertion's whole reach, so it has to name every node type consults_declared_span
		// accepts; a type missing from both this list and ticks() cancels out and reclaims nothing forever
		// while system::flow_nodes still reports it bounded.
		let reclaimable: Vec<(OperatorDef, Option<&OperatorSettings>)> = vec![
			(join(), Some(&join_ttl)),
			(
				OperatorDef::Distinct {
					expressions: vec![],
				},
				Some(&ttl),
			),
			(OperatorDef::Append {}, Some(&ttl)),
			(apply(), Some(&ttl)),
			(
				OperatorDef::Aggregate {
					by: vec![],
					map: vec![],
				},
				Some(&ttl),
			),
		];

		for (node, settings) in reclaimable {
			assert!(
				node.consults_declared_span() && settings.is_some(),
				"precondition: this node must accept a declared span: {node:?}"
			);
			assert!(node.ticks(), "a reclaimable node that never ticks is never reclaimed: {node:?}");
		}

		// A window reclaims through its operator's seal span rather than a declared one, so the pairing has to
		// hold for it without going through declared_horizon.
		let window_node = window(
			WindowKind::Tumbling {
				size: WindowSize::Duration(ms(60_000)),
			},
			ms(0),
		);
		assert!(window_node.ticks(), "a window reclaims on tick, so it must request one: {window_node:?}");
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

	#[test]
	fn every_node_that_can_reclaim_is_also_reported_as_holding_state() {
		// holds_state drives the system::flow_nodes listing used to find unbounded retention, so a node that
		// reclaims but answers false here is absent from that listing in both directions - never reported as
		// perpetual, never reported when it declares a span.
		let ttl = OperatorSettings {
			ttl: Some(OperatorTtl {
				duration: ms(60_000),
			}),
			join: None,
		};
		let join_ttl = OperatorSettings {
			ttl: None,
			join: Some(JoinTtl {
				left: Some(OperatorTtl {
					duration: ms(60_000),
				}),
				right: Some(OperatorTtl {
					duration: ms(60_000),
				}),
			}),
		};

		let reclaimable: Vec<(OperatorDef, Option<&OperatorSettings>)> = vec![
			(join(), Some(&join_ttl)),
			(
				OperatorDef::Distinct {
					expressions: vec![],
				},
				Some(&ttl),
			),
			(OperatorDef::Append {}, Some(&ttl)),
			(apply(), Some(&ttl)),
			(
				OperatorDef::Aggregate {
					by: vec![],
					map: vec![],
				},
				Some(&ttl),
			),
		];

		for (node, settings) in reclaimable {
			assert!(
				node.consults_declared_span() && settings.is_some(),
				"precondition: this node must accept a declared span: {node:?}"
			);
			assert!(node.holds_state(), "a node that can reclaim must be listed as stateful: {node:?}");
		}

		// A window derives its horizon from its operator's seal, so it never reaches the loop above, but it
		// holds group state and has to be listed all the same.
		assert!(
			window(
				WindowKind::Tumbling {
					size: WindowSize::Duration(ms(60_000)),
				},
				ms(0),
			)
			.holds_state(),
			"a window keeps per-group accumulators and must be listed as stateful"
		);

		assert!(
			!OperatorDef::Map {
				expressions: vec![],
			}
			.holds_state(),
			"a map keeps nothing between rows and must never appear in the retention listing"
		);
	}
}
