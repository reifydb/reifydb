// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	interface::catalog::{
		flow::{FlowId, OperatorId},
		storage::StorageId,
	},
	key::operator_group_state::{GroupId, Keyspace},
	state::horizon::Cutoff,
};
use reifydb_flow::{operator::Operator, transaction::FlowTransaction};
use reifydb_rql::flow::{flow::FlowDag, operator::OperatorDef};
use reifydb_store_operator::{CompactionOutcome, OperatorStore};
use reifydb_value::{
	Result,
	value::{datetime::DateTime, duration::Duration},
};
use tracing::instrument;

use crate::engine::FlowEngineInner;

const COMPACTION_CADENCE_DIVISOR: u64 = 2;

const MAPPING_ROWS_PER_TICK: usize = 1_024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OperatorCompaction {
	pub outcome: CompactionOutcome,
	pub floor: Option<DateTime>,
	pub mapping_rows: usize,
}

pub fn compact_operator(
	txn: &mut FlowTransaction,
	store: &OperatorStore,
	operator: &dyn Operator,
	watermark: DateTime,
	identity: Option<DateTime>,
	mapping_cursor: &mut Option<EncodedKey>,
) -> Result<OperatorCompaction> {
	let spec = operator.floors(txn, watermark)?;

	let mut mapping_rows = 0;
	if let Some(cutoff) = mapping_cutoff(spec.cutoff(Keyspace::ROW_NUMBER_MAPPING), identity) {
		mapping_rows = txn.evict_row_numbers(
			operator.id(),
			GroupId::NODE_SCOPE,
			Cutoff(cutoff),
			mapping_cursor,
			MAPPING_ROWS_PER_TICK,
		)?;
	}

	let floor = spec.max_cutoff();
	if spec.is_empty() {
		return Ok(OperatorCompaction {
			outcome: CompactionOutcome::default(),
			floor,
			mapping_rows,
		});
	}
	let outcome = store.compact(operator.id(), &spec);
	operator.on_compacted(&outcome);
	Ok(OperatorCompaction {
		outcome,
		floor,
		mapping_rows,
	})
}

fn compaction_due(last: Option<DateTime>, watermark: DateTime, floor: Option<DateTime>) -> bool {
	let Some(floor) = floor else {
		return false;
	};
	let span = watermark.to_nanos().saturating_sub(floor.to_nanos());
	match last {
		Some(last) => watermark.to_nanos().saturating_sub(last.to_nanos()) >= span / COMPACTION_CADENCE_DIVISOR,
		None => true,
	}
}

pub fn mapping_cutoff(declared: Option<DateTime>, identity: Option<DateTime>) -> Option<DateTime> {
	match identity {
		Some(identity) => declared.map(|declared| declared.min(identity)),
		None => None,
	}
}

pub fn identity_cutoff(identity_span: Option<Duration>, watermark: DateTime) -> Option<DateTime> {
	identity_span.map(|span| watermark.saturating_sub(span))
}

pub fn identity_span(flow: &FlowDag, row_ttl: impl Fn(StorageId) -> Option<Duration>) -> Option<Duration> {
	flow.get_operator_ids()
		.filter_map(|id| flow.get_operator(&id))
		.find_map(|operator| sink_storage(&operator.ty))
		.and_then(row_ttl)
}

fn sink_storage(ty: &OperatorDef) -> Option<StorageId> {
	match ty {
		OperatorDef::SinkTableView {
			table,
			..
		} => Some(StorageId::Table(*table)),
		OperatorDef::SinkRingBufferView {
			ringbuffer,
			..
		} => Some(StorageId::RingBuffer(*ringbuffer)),
		OperatorDef::SinkSeriesView {
			series,
			..
		} => Some(StorageId::Series(*series)),
		_ => None,
	}
}

impl FlowEngineInner {
	#[instrument(name = "flow::engine::compact", level = "debug", skip(self, txn), fields(flow_id = ?flow_id))]
	pub fn compact_flow(&self, txn: &mut FlowTransaction, flow_id: FlowId) -> Result<()> {
		let Some(flow) = self.flows.get(&flow_id) else {
			return Ok(());
		};
		let watermark = self.flow_watermark(txn, flow)?;
		let identity = identity_cutoff(identity_span(flow, |storage| self.row_ttl(storage)), watermark);

		for operator_id in flow.get_operator_ids() {
			let Some(operator) = self.operators.get(&operator_id) else {
				continue;
			};

			let spec = operator.floors(txn, watermark)?;
			let floor = spec.max_cutoff();
			self.executor.services().node_retention_store.set_frontier(operator_id, floor);

			let mut mapping_rows = 0;
			if let Some(cutoff) = mapping_cutoff(spec.cutoff(Keyspace::ROW_NUMBER_MAPPING), identity) {
				let mut cursor = self.mapping_cursors.entry(operator_id).or_default().clone();
				mapping_rows = txn.evict_row_numbers(
					operator_id,
					GroupId::NODE_SCOPE,
					Cutoff(cutoff),
					&mut cursor,
					MAPPING_ROWS_PER_TICK,
				)?;
				*self.mapping_cursors.entry(operator_id).or_default() = cursor;
			}

			let mut outcome = CompactionOutcome::default();
			if !spec.is_empty() && self.compaction_due(operator_id, watermark, floor) {
				outcome = self.substrate.operators.compact(operator_id, &spec);
				operator.on_compacted(&outcome);
			}

			self.operator_samples.record_compaction(
				operator_id,
				outcome.dropped,
				outcome.reclaimed_bytes,
				mapping_rows as u64,
			);
		}
		Ok(())
	}

	fn compaction_due(&self, operator: OperatorId, watermark: DateTime, floor: Option<DateTime>) -> bool {
		let last = self.compacted_at.get(&operator).map(|entry| *entry.value());
		let due = compaction_due(last, watermark, floor);
		if due {
			self.compacted_at.insert(operator, watermark);
		}
		due
	}

	fn flow_watermark(&self, txn: &mut FlowTransaction, flow: &FlowDag) -> Result<DateTime> {
		let sources: Vec<OperatorId> = flow
			.get_operator_ids()
			.filter(|id| flow.get_operator(id).is_some_and(|operator| operator.ty.is_source()))
			.collect();
		txn.source_watermarks().flow_watermark(&sources, txn)
	}

	fn row_ttl(&self, storage: StorageId) -> Option<Duration> {
		self.catalog.find_row_settings_latest(storage).and_then(|settings| settings.ttl).map(|ttl| ttl.duration)
	}
}

#[cfg(test)]
mod cadence_tests {
	use reifydb_value::factory::at_nanos;

	use super::*;

	const SECOND: u64 = 1_000_000_000;

	#[test]
	fn an_operator_is_not_recompacted_until_the_watermark_advances_half_its_span() {
		// Compaction merges the whole arena to find the few rows that just expired, so its wasted
		// work is proportional to span/interval. Merging on every flow tick (~1s against a 10s ttl)
		// rewrites live state ten times per expiry; harvesting half the window at a time bounds
		// that at ~2x. Mutation falsified against: dropping the cadence (always due) and inverting
		// the comparison (never due at exactly half the span).
		let watermark = at_nanos(100 * SECOND);
		let floor = Some(at_nanos(90 * SECOND));

		assert!(
			!compaction_due(Some(at_nanos(96 * SECOND)), watermark, floor),
			"4s into a 10s span is under the 5s cadence and must not re-compact"
		);
		assert!(
			compaction_due(Some(at_nanos(95 * SECOND)), watermark, floor),
			"exactly half the span must be due, or the cadence drifts a tick later every pass"
		);
		assert!(
			compaction_due(None, watermark, floor),
			"an operator never compacted must not wait for a cadence it has no baseline for"
		);
	}

	#[test]
	fn an_operator_with_no_floor_is_never_due() {
		// A floorless operator has nothing merge-time cancellation could drop, so a merge would be
		// pure rewrite cost. Mutation falsified against: treating None as always-due.
		assert!(!compaction_due(None, at_nanos(100 * SECOND), None));
	}

	#[test]
	fn a_floor_at_the_watermark_is_always_due() {
		// A zero span means everything below the watermark is already expired; dividing by the
		// cadence must not produce an interval that defers the merge forever.
		let watermark = at_nanos(100 * SECOND);
		assert!(compaction_due(Some(watermark), watermark, Some(watermark)));
	}
}

#[cfg(test)]
mod mapping_cutoff_tests {
	use super::*;

	fn ms(milliseconds: i64) -> Duration {
		Duration::from_milliseconds(milliseconds).expect("test duration must be representable")
	}

	#[test]
	fn the_sink_row_ttl_bounds_identity_from_the_same_watermark_the_operator_answered_against() {
		// Identity belongs to the SINK, not the operator: a mapping must outlive the published row
		// naming it, and that row lives exactly the sink's row ttl.
		let watermark = DateTime::from_millis(1_000_000);

		assert_eq!(
			identity_cutoff(Some(ms(60_000)), watermark),
			Some(DateTime::from_millis(940_000)),
			"watermark minus the sink row ttl"
		);
	}

	#[test]
	fn a_forever_sink_keeps_identity_entirely() {
		// A sink row with no ttl lives forever, so the mapping it names has to as well.
		assert_eq!(identity_cutoff(None, DateTime::from_millis(1_000_000)), None);

		assert_eq!(
			mapping_cutoff(Some(DateTime::from_millis(500)), None),
			None,
			"a declared mapping horizon is inert until the sink bounds its rows"
		);
	}

	#[test]
	fn the_mapping_dies_at_the_earlier_of_its_declared_horizon_and_the_sink_horizon() {
		// The declared horizon says when the operator stops needing the mapping; the sink horizon
		// says when the published row stops naming it. Evicting at the later of the two would keep
		// a mapping past both reasons to exist; at the earlier, neither is ever violated because
		// eviction is only ever a space decision once both horizons have passed the row's stamp.
		let declared = Some(DateTime::from_millis(400));
		let identity = Some(DateTime::from_millis(700));

		assert_eq!(mapping_cutoff(declared, identity), Some(DateTime::from_millis(400)));
		assert_eq!(
			mapping_cutoff(Some(DateTime::from_millis(900)), identity),
			Some(DateTime::from_millis(700)),
			"the sink horizon caps a looser declaration"
		);
		assert_eq!(mapping_cutoff(None, identity), None, "no declaration, no mapping eviction");
	}
}

#[cfg(test)]
mod sink_storage_tests {
	use reifydb_core::interface::catalog::{
		id::{RingBufferId, SeriesId, TableId, ViewId},
		series::{SeriesKey, TimestampPrecision},
		storage::StorageId,
	};
	use reifydb_rql::flow::operator::OperatorDef;

	use super::sink_storage;

	#[test]
	fn a_sink_resolves_to_the_storage_it_writes_not_the_view_it_presents() {
		// Row settings are recorded against the storage, never the view: returning the view is
		// well-typed but the lookup misses, the flow reads as perpetual, and its mappings are never
		// evicted. The ids are distinct so returning the wrong half cannot pass by accident.
		assert_eq!(
			sink_storage(&OperatorDef::SinkTableView {
				view: ViewId(1),
				table: TableId(2),
			}),
			Some(StorageId::Table(TableId(2)))
		);

		assert_eq!(
			sink_storage(&OperatorDef::SinkRingBufferView {
				view: ViewId(3),
				ringbuffer: RingBufferId(4),
				capacity: 16,
			}),
			Some(StorageId::RingBuffer(RingBufferId(4)))
		);

		assert_eq!(
			sink_storage(&OperatorDef::SinkSeriesView {
				view: ViewId(5),
				series: SeriesId(6),
				key: SeriesKey::DateTime {
					column: "ts".to_string(),
					precision: TimestampPrecision::Millisecond,
				},
			}),
			Some(StorageId::Series(SeriesId(6)))
		);
	}

	#[test]
	fn a_node_that_owns_no_storage_resolves_to_nothing() {
		// Only sinks own storage; resolving one here would attribute a row ttl to a operator that
		// never writes rows.
		assert_eq!(
			sink_storage(&OperatorDef::SourceTable {
				table: TableId(9)
			}),
			None
		);
	}
}

#[cfg(test)]
mod identity_span_tests {
	use reifydb_core::interface::catalog::{
		flow::{FlowId, OperatorId},
		id::{SubscriptionId, TableId, ViewId},
		storage::StorageId,
	};
	use reifydb_rql::flow::{
		flow::FlowDag,
		operator::{FlowEdge, FlowNode, OperatorDef},
	};
	use reifydb_value::value::duration::Duration;

	use super::identity_span;

	fn ms(milliseconds: i64) -> Duration {
		Duration::from_milliseconds(milliseconds).expect("test duration must be representable")
	}

	fn dag(operators: &[(u64, OperatorDef)], edges: &[(u64, u64)]) -> FlowDag {
		// The edges are wired even though the resolver scans operators rather than walking them, so
		// the fixture keeps the shape of a real flow.
		let mut builder = FlowDag::builder(FlowId(1));
		for (id, ty) in operators {
			builder.add_node(FlowNode::new(OperatorId(*id), ty.clone()));
		}
		for (index, (source, target)) in edges.iter().enumerate() {
			builder.add_edge(FlowEdge::new(index as u64 + 1, *source, *target)).expect("edge");
		}
		builder.build()
	}

	fn source() -> OperatorDef {
		OperatorDef::SourceTable {
			table: TableId(1),
		}
	}

	fn operator() -> OperatorDef {
		OperatorDef::Append {}
	}

	#[test]
	fn a_flows_identity_is_bounded_by_its_sinks_row_ttl() {
		// The mapping has to outlive the row naming it, and the row lives exactly the sink's row
		// ttl. Anything shorter retires the mapping under a live row, and the next event on that
		// key mints a second row over it.
		let flow = dag(
			&[
				(1, source()),
				(2, operator()),
				(
					3,
					OperatorDef::SinkTableView {
						view: ViewId(10),
						table: TableId(20),
					},
				),
			],
			&[(1, 2), (2, 3)],
		);

		let span = identity_span(&flow, |storage| match storage {
			StorageId::Table(TableId(20)) => Some(ms(60_000)),
			_ => None,
		});

		assert_eq!(span, Some(ms(60_000)));
	}

	#[test]
	fn a_sink_that_never_expires_its_rows_leaves_identity_perpetual() {
		// A sink with no declared row ttl keeps its rows forever, so any duration here would
		// eventually retire a mapping while the row still points at it.
		let flow = dag(
			&[
				(1, operator()),
				(
					2,
					OperatorDef::SinkTableView {
						view: ViewId(10),
						table: TableId(20),
					},
				),
			],
			&[(1, 2)],
		);

		assert_eq!(identity_span(&flow, |_| None), None);
	}

	#[test]
	fn a_subscription_flow_bounds_nothing() {
		// Subscription flows tick too, and a subscription owns no storage, so its rows are not
		// ours to age: the resolver must find no sink rather than mistake it for one.
		let flow = dag(
			&[
				(1, operator()),
				(
					2,
					OperatorDef::SinkSubscription {
						subscription: SubscriptionId(7),
					},
				),
			],
			&[(1, 2)],
		);

		assert_eq!(identity_span(&flow, |_| Some(ms(60_000))), None);
	}
}
