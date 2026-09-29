// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use serde::{Deserialize, Serialize};

use crate::{
	common::{JoinType, TimeDomain},
	expression::Expression,
	interface::catalog::{
		flow::OperatorId,
		id::{RingBufferId, SeriesId, SubscriptionId, TableId, ViewId},
		object::ObjectId,
		series::SeriesKey,
	},
	operator_with::{AggregateWith, ApplyWith, DistinctWith, JoinWith, LookupWith, WindowWith},
	sort::SortKey,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
		natural: bool,
		with: JoinWith,
	},
	Aggregate {
		by: Vec<Expression>,
		map: Vec<Expression>,
		with: AggregateWith,
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
		with: DistinctWith,
	},
	Apply {
		operator: String,
		params: Vec<Expression>,
		with: ApplyWith,
	},
	SinkTableView {
		view: ViewId,
	},
	SinkRingBufferView {
		view: ViewId,
		capacity: u64,
	},
	SinkSeriesView {
		view: ViewId,
		key: SeriesKey,
	},
	SinkSubscription {
		subscription: SubscriptionId,
	},
	Window {
		group_by: Vec<Expression>,
		aggregations: Vec<Expression>,
		with: WindowWith,
	},
	Lookup {
		join_type: JoinType,
		right: LookupObject,
		left: Vec<Expression>,
		alias: Option<String>,
		with: LookupWith,
	},
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LookupObject {
	Table(TableId),
	View(ViewId),
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
				| OperatorDef::Window { .. }
				| OperatorDef::Apply { .. }
				| OperatorDef::Join { .. }
				| OperatorDef::Lookup { .. }
				| OperatorDef::Aggregate { .. }
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
			OperatorDef::Lookup {
				..
			} => "Lookup".into(),
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
			OperatorDef::Lookup {
				..
			} => 22,
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
			}
			| OperatorDef::Lookup {
				..
			} => None,
		}
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[cfg(test)]
mod tests {
	use reifydb_value::value::duration::Duration;

	use super::{LookupObject, OperatorDef};
	use crate::{
		common::JoinType,
		interface::catalog::id::{TableId, ViewId},
		operator_with::{ApplyWith, DistinctWith, JoinWith, LookupWith},
		row::OperatorRetention,
	};

	fn join() -> OperatorDef {
		OperatorDef::Join {
			join_type: JoinType::Inner,
			left: vec![],
			right: vec![],
			alias: None,
			natural: false,
			with: JoinWith::default(),
		}
	}

	#[test]
	fn join_always_requests_ticks() {
		// The graph-level gate never reads the join's per-side TTL, so only the runtime operator can decide.
		assert!(join().ticks());
	}

	#[test]
	fn apply_always_requests_ticks() {
		// The graph-level gate cannot see the runtime operator, so it must register unconditionally and let
		// the operator decide; without that a tick-capable custom operator could never be ticked at all.
		let apply = OperatorDef::Apply {
			operator: "compute_swap_volumes".to_string(),
			params: vec![],
			with: ApplyWith::default(),
		};
		assert!(apply.ticks());
	}

	#[test]
	fn append_and_distinct_always_request_ticks() {
		// The graph-level gate cannot see their TTL, so only the runtime operator can decide.
		assert!(OperatorDef::Append {}.ticks());
		assert!(OperatorDef::Distinct {
			expressions: vec![],
			with: DistinctWith {},
		}
		.ticks());
	}

	#[test]
	fn sink_ringbuffer_view_always_requests_ticks() {
		// The row TTL lives in row settings, not the node, so the graph-level gate cannot see it. Without an
		// unconditional request the flow is never scheduled to tick and quiet partitions leak forever.
		assert!(OperatorDef::SinkRingBufferView {
			view: ViewId(1),
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

	fn lookup(right: LookupObject) -> OperatorDef {
		OperatorDef::Lookup {
			join_type: JoinType::Left,
			right,
			left: vec![],
			alias: Some("price".to_string()),
			with: LookupWith {
				retention: Some(OperatorRetention {
					duration: Duration::from_seconds(10).unwrap(),
				}),
			},
		}
	}

	#[test]
	fn lookup_always_requests_ticks() {
		// Left retention frees rows on ticks; without them the read-version lease never moves.
		assert!(lookup(LookupObject::Table(TableId(1))).ticks());
	}

	#[test]
	fn lookup_is_not_a_source_and_has_no_source_object() {
		// A source flag would route the right side's CDC into the flow, which the lookup must never read.
		let node = lookup(LookupObject::View(ViewId(7)));
		assert!(!node.is_source());
		assert!(node.source_object_id().is_none());
	}

	#[test]
	fn lookup_discriminator_is_appended_after_every_existing_operator() {
		// Stored operator rows keep their type byte, so a lookup must not reuse or shift an existing number.
		let node = lookup(LookupObject::Table(TableId(1)));
		assert_eq!(node.discriminator(), 22);
		assert_eq!(node.label(), "Lookup");
	}

	#[test]
	fn lookup_survives_the_persisted_encoding_with_its_right_object_and_retention() {
		// The DAG is stored with postcard, so a lost field registers a different lookup after restart.
		for right in [LookupObject::Table(TableId(3)), LookupObject::View(ViewId(4))] {
			let node = lookup(right);
			let bytes = postcard::to_stdvec(&node).expect("a lookup node must encode");
			let decoded: OperatorDef = postcard::from_bytes(&bytes).expect("a lookup node must decode");
			let OperatorDef::Lookup {
				join_type,
				right: decoded_right,
				alias,
				with,
				..
			} = decoded
			else {
				panic!("a lookup must decode as a lookup, got {decoded:?}");
			};
			assert_eq!(join_type, JoinType::Left);
			assert_eq!(decoded_right, right);
			assert_eq!(alias.as_deref(), Some("price"));
			assert_eq!(with.retention.map(|r| r.duration), Some(Duration::from_seconds(10).unwrap()));
		}
	}
}
