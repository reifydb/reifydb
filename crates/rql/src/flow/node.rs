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
	row::OperatorSettings,
	sort::SortKey,
	state::horizon::{Horizon, keyed_horizon, window_horizon},
};
use reifydb_value::value::duration::Duration;
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
		grace: Duration,
		lateness: Duration,
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
				| FlowNodeType::SinkRingBufferView { .. }
		)
	}

	pub fn horizon(&self, settings: Option<&OperatorSettings>) -> Horizon {
		match self {
			FlowNodeType::Window {
				kind,
				grace,
				lateness,
				..
			} => window_horizon(kind, *grace, *lateness),
			FlowNodeType::Join {
				..
			}
			| FlowNodeType::Distinct {
				..
			}
			| FlowNodeType::Append {
				..
			}
			| FlowNodeType::Apply {
				..
			} => keyed_horizon(settings),
			_ => Horizon::Perpetual,
		}
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
	use reifydb_core::{
		common::{JoinType, TimeDomain, WindowKind, WindowSize},
		interface::catalog::id::{RingBufferId, ViewId},
		row::{JoinTtl, OperatorSettings, OperatorTtl},
		state::horizon::Horizon,
	};
	use reifydb_value::value::duration::Duration;

	use super::FlowNodeType;

	fn ms(milliseconds: i64) -> Duration {
		Duration::from_milliseconds(milliseconds).expect("test duration must be representable")
	}

	fn window(kind: WindowKind, grace: Duration, lateness: Duration) -> FlowNodeType {
		FlowNodeType::Window {
			kind,
			group_by: vec![],
			aggregations: vec![],
			ts: None,
			grace,
			lateness,
		}
	}

	fn apply() -> FlowNodeType {
		FlowNodeType::Apply {
			operator: "custom".into(),
			expressions: vec![],
		}
	}

	#[test]
	fn a_window_derives_its_horizon_from_its_own_declaration_not_from_settings() {
		// A window's retention is intrinsic: span, grace and lateness are compile-time properties of
		// the node, so there is no knob to misdeclare and no settings row to consult. Passing
		// settings must not change the answer, or two sources of truth would drift.
		let node = window(
			WindowKind::Tumbling {
				size: WindowSize::Duration(ms(60_000)),
				time: TimeDomain::Event,
			},
			ms(5_000),
			ms(0),
		);

		let without = node.horizon(None);
		let with = node.horizon(Some(&OperatorSettings {
			ttl: Some(OperatorTtl {
				duration: ms(1),
			}),
			join: None,
		}));

		assert_eq!(without.span_ms(), Some(65_000));
		assert_eq!(with, without, "a settings ttl must not shorten a window's intrinsic seal horizon");
	}

	#[test]
	fn a_custom_apply_operator_gets_a_horizon_only_when_its_author_declared_one() {
		// The substrate cannot know what an extension operator's state means or how it ages, so it
		// must never invent a horizon for one. The existing ttl clause on apply is the declaration
		// channel: with it the group ages out, without it the operator is honestly perpetual and gets
		// named in the report rather than silently reclaimed on a schedule nobody agreed to.
		let declared = apply().horizon(Some(&OperatorSettings {
			ttl: Some(OperatorTtl {
				duration: ms(3_600_000),
			}),
			join: None,
		}));

		assert_eq!(declared.idle_span(), Some(ms(3_600_000)));
		assert_eq!(apply().horizon(None), Horizon::Perpetual);
	}

	#[test]
	fn a_join_reads_both_sides_of_its_settings() {
		// Join sides share one group range, so the node horizon must come from the JoinTtl pair
		// rather than the single-ttl field, which a join never writes.
		let node = join().horizon(Some(&OperatorSettings {
			ttl: None,
			join: Some(JoinTtl {
				left: Some(OperatorTtl {
					duration: ms(60_000),
				}),
				right: Some(OperatorTtl {
					duration: ms(120_000),
				}),
			}),
		}));

		assert_eq!(node.span_ms(), Some(120_000));
	}

	#[test]
	fn nodes_that_keep_no_keyed_state_are_never_reclaimed() {
		// Filter and map hold nothing per group, so there is nothing for the driver to scan. They
		// answer perpetual so a future driver cannot be handed a cutoff for a node with no groups.
		assert_eq!(
			FlowNodeType::Filter {
				conditions: vec![]
			}
			.horizon(None),
			Horizon::Perpetual
		);
		assert_eq!(
			FlowNodeType::Map {
				expressions: vec![]
			}
			.horizon(None),
			Horizon::Perpetual
		);
	}

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
	fn sink_ringbuffer_view_always_requests_ticks() {
		// A ring buffer view sink owns its per-partition operator-state TTL eviction on the flow
		// tick. The graph-level gate cannot see the row TTL (it lives in row settings, not the
		// node), so the sink requests ticks unconditionally; the runtime operator reports no tick
		// interval when no TTL is configured and is then skipped. Without this the flow would never
		// be scheduled to tick and quiet partitions' state would leak forever.
		assert!(FlowNodeType::SinkRingBufferView {
			view: ViewId(1),
			ringbuffer: RingBufferId(1),
			capacity: 1,
		}
		.ticks());
	}

	#[test]
	fn every_node_that_can_reclaim_also_requests_ticks() {
		// Group reclamation runs only on the flow tick path, and a flow is scheduled to tick only if
		// at least one of its nodes requests ticks. A node type that derives a reclaiming horizon
		// while answering false to ticks() would therefore accumulate group state that nothing ever
		// scans, and the retention report would call it healthy because the driver never ran for it.
		// These two functions must be kept in step; this is the assertion that notices when they are
		// not.
		let ttl = OperatorSettings {
			ttl: Some(OperatorTtl {
				duration: ms(60_000),
			}),
			join: None,
		};
		// Both sides must be declared: a join whose other side retains forever is perpetual as a
		// whole, because reclaiming one side's rows while the other still holds the group would
		// silently change the join's output rather than just free memory.
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

		let reclaimable: Vec<(FlowNodeType, Option<&OperatorSettings>)> = vec![
			(
				window(
					WindowKind::Tumbling {
						size: WindowSize::Duration(ms(60_000)),
						time: TimeDomain::Event,
					},
					ms(0),
					ms(0),
				),
				None,
			),
			(join(), Some(&join_ttl)),
			(
				FlowNodeType::Distinct {
					expressions: vec![],
				},
				Some(&ttl),
			),
			(FlowNodeType::Append {}, Some(&ttl)),
			(apply(), Some(&ttl)),
		];

		for (node, settings) in reclaimable {
			assert!(
				node.horizon(settings).reclaims(),
				"precondition: this node must derive a reclaiming horizon: {node:?}"
			);
			assert!(node.ticks(), "a reclaimable node that never ticks is never reclaimed: {node:?}");
		}
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
