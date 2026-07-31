// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_catalog::catalog::Catalog;
use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		config::{ConfigKey, GetConfig},
		flow::{FlowId, FlowNodeId},
		storage::StorageId,
	},
	key::operator_state::{GroupId, GroupSet, Keyspace},
	lifecycle::class::{Floor, FloorTerm, RetentionClass},
	state::horizon::{Cutoff, Horizon},
};
use reifydb_flow::{transaction::FlowTransaction, window::ledger::read_sealed_through};
use reifydb_rql::flow::{flow::FlowDag, node::FlowNodeType};
use reifydb_value::{
	Result,
	value::{datetime::DateTime, duration::Duration},
};
use tracing::instrument;

use crate::engine::FlowEngineInner;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReclaimBudget {
	pub groups: usize,
	pub rows: usize,
}

impl ReclaimBudget {
	pub fn from_config(catalog: &Catalog) -> Self {
		Self {
			groups: catalog.get_config_uint8(ConfigKey::OperatorReclaimGroupsPerTick) as usize,
			rows: catalog.get_config_uint8(ConfigKey::OperatorReclaimRowsPerTick) as usize,
		}
	}

	fn exhausted(&self) -> bool {
		self.groups == 0 || self.rows == 0
	}
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReclaimReport {
	pub data_groups: usize,
	pub identity_groups: usize,
	pub keyspace_groups: usize,
	pub rows: usize,
	pub backlog: usize,
	pub perpetual_nodes: usize,
	pub data_floor: Option<(Floor, FloorTerm)>,
	pub identity_floor: Option<(Floor, FloorTerm)>,
}

impl ReclaimReport {
	fn bind(&mut self, cutoffs: &Cutoffs) {
		self.data_floor = lowest(self.data_floor, Some(cutoffs.data_floor));
		self.identity_floor = lowest(self.identity_floor, cutoffs.identity_floor);
	}
}

fn lowest(current: Option<(Floor, FloorTerm)>, candidate: Option<(Floor, FloorTerm)>) -> Option<(Floor, FloorTerm)> {
	match (current, candidate) {
		(Some(current), Some(candidate)) => Some(if current.0.monotonic_key() <= candidate.0.monotonic_key() {
			current
		} else {
			candidate
		}),
		(Some(only), None) | (None, Some(only)) => Some(only),
		(None, None) => None,
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Cutoffs {
	data: Cutoff,
	identity: Option<Cutoff>,
	watermark: DateTime,
	slack: Duration,
	data_floor: (Floor, FloorTerm),
	identity_floor: Option<(Floor, FloorTerm)>,
}

impl FlowEngineInner {
	#[instrument(name = "lifecycle::operator::group::scan", level = "debug", skip(self, txn), fields(flow_id = ?flow_id))]
	pub fn reclaim_flow(
		&self,
		txn: &mut FlowTransaction,
		flow_id: FlowId,
		checkpoint: CommitVersion,
		budget: ReclaimBudget,
	) -> Result<ReclaimReport> {
		let mut report = ReclaimReport::default();
		let Some(flow) = self.flows.get(&flow_id) else {
			return Ok(report);
		};
		let identity_span = identity_span(flow, |storage| self.row_ttl(storage));
		let mut remaining = budget;

		let watermark = self.flow_watermark(txn, flow)?;

		let mut inputs = Vec::new();
		for node_id in flow.get_node_ids() {
			let Some(node) = flow.get_node(&node_id) else {
				continue;
			};
			let Some(operator) = self.operators.get(&node_id) else {
				continue;
			};
			if !operator.capabilities().contains(&OperatorCapability::Reclaim) {
				report.perpetual_nodes += 1;
				continue;
			}
			let horizon = self.node_horizon(node);
			let keyspace_spans = operator.keyspace_spans();
			let mapping_span = operator.node_mapping_span();
			if !horizon.reclaims() && keyspace_spans.is_empty() && mapping_span.is_none() {
				report.perpetual_nodes += 1;
				continue;
			}
			let Some(grid) = self.substrate.group.buckets(node_id).event_grid() else {
				continue;
			};
			let slack = grid.width();
			let sealed_through = if seals_on_timer(&node.ty) {
				read_sealed_through(txn, node_id)?.map(|sealed| sealed.at())
			} else {
				None
			};
			let cutoffs =
				seal_cutoffs(horizon, watermark, identity_span, sealed_through, slack, checkpoint);
			if let Some(cutoffs) = &cutoffs {
				report.bind(cutoffs);
			}
			inputs.push(SweepInputs {
				node: node_id,
				data: cutoffs.as_ref().map(|cutoffs| cutoffs.data),
				identity: cutoffs.as_ref().and_then(|cutoffs| cutoffs.identity),
				keyspaces: keyspace_spans
					.into_iter()
					.map(|(keyspace, span)| {
						(keyspace, Cutoff(watermark.saturating_sub(span).saturating_sub(slack)))
					})
					.collect(),
				mapping: mapping_span
					.map(|span| Cutoff(watermark.saturating_sub(span).saturating_sub(slack))),
				mapping_cursor: self.mapping_cursors.entry(node_id).or_default().clone(),
			});
		}

		let outcome = reclaim_nodes(inputs, txn, &mut remaining, &mut report, &mut |node, groups| {
			if let Some(operator) = self.operators.get(&node) {
				operator.invalidate_groups(&groups);
			}
		})?;
		for (node, cursor) in outcome.cursors {
			*self.mapping_cursors.entry(node).or_default() = cursor;
		}

		self.record(&report, remaining);
		Ok(report)
	}

	fn record(&self, report: &ReclaimReport, remaining: ReclaimBudget) {
		let metrics = &self.retention_metrics;
		metrics.record_liveness(RetentionClass::OperatorGroupData);
		metrics.record_liveness(RetentionClass::OperatorGroupIdentity);
		metrics.record_reclamation(
			RetentionClass::OperatorGroupData,
			report.data_floor,
			report.data_groups as u64,
			report.backlog as u64,
		);
		metrics.record_reclamation(
			RetentionClass::OperatorGroupIdentity,
			report.identity_floor,
			report.identity_groups as u64,
			report.backlog as u64,
		);
		if remaining.exhausted() {
			metrics.record_budget_exhausted(RetentionClass::OperatorGroupData);
			metrics.record_budget_exhausted(RetentionClass::OperatorGroupIdentity);
		}
	}

	fn flow_watermark(&self, txn: &mut FlowTransaction, flow: &FlowDag) -> Result<DateTime> {
		let sources: Vec<FlowNodeId> = flow
			.get_node_ids()
			.filter(|id| flow.get_node(id).is_some_and(|node| node.ty.is_source()))
			.collect();
		txn.source_watermarks().flow_watermark(flow.time_domain(), &sources, txn)
	}

	fn row_ttl(&self, storage: StorageId) -> Option<Duration> {
		self.catalog.find_row_settings_latest(storage).and_then(|settings| settings.ttl).map(|ttl| ttl.duration)
	}
}

fn identity_span(flow: &FlowDag, row_ttl: impl Fn(StorageId) -> Option<Duration>) -> Option<Duration> {
	flow.get_node_ids()
		.filter_map(|id| flow.get_node(&id))
		.find_map(|node| sink_storage(&node.ty))
		.and_then(row_ttl)
}

pub struct SweepInputs {
	pub node: FlowNodeId,
	pub data: Option<Cutoff>,
	pub identity: Option<Cutoff>,

	pub keyspaces: Vec<(Keyspace, Cutoff)>,

	pub mapping: Option<Cutoff>,

	pub mapping_cursor: Option<EncodedKey>,
}

pub struct SweepOutcome {
	pub cursors: Vec<(FlowNodeId, Option<EncodedKey>)>,
}

pub fn reclaim_nodes(
	inputs: Vec<SweepInputs>,
	txn: &mut FlowTransaction,
	remaining: &mut ReclaimBudget,
	report: &mut ReclaimReport,
	invalidate: &mut dyn FnMut(FlowNodeId, GroupSet),
) -> Result<SweepOutcome> {
	let mut cursors = Vec::new();
	for input in inputs {
		if remaining.exhausted() {
			break;
		}
		let node = input.node;

		if let Some(cutoff) = input.identity {
			reclaim_identity(txn, node, cutoff, remaining, report)?;
		}

		for (keyspace, cutoff) in input.keyspaces {
			if remaining.exhausted() {
				break;
			}
			let retired = reclaim_keyspace(txn, node, keyspace, cutoff, remaining, report)?;
			if !retired.is_empty() {
				invalidate(node, GroupSet::new(retired));
			}
		}

		let mut cursor = input.mapping_cursor;
		if let Some(cutoff) = input.mapping
			&& !remaining.exhausted()
		{
			let removed =
				txn.evict_row_numbers(node, GroupId::NODE_SCOPE, cutoff, &mut cursor, remaining.rows)?;
			remaining.rows -= removed;
			report.rows += removed;
			if cursor.is_some() {
				report.backlog += 1;
			}
		}
		cursors.push((node, cursor));

		if let Some(cutoff) = input.data {
			let released = reclaim_data(txn, node, cutoff, remaining, report)?;
			if !released.is_empty() {
				invalidate(node, GroupSet::new(released));
			}
		}
	}
	Ok(SweepOutcome {
		cursors,
	})
}

#[instrument(name = "lifecycle::operator::group::data", level = "debug", skip_all, fields(node = node.0))]
fn reclaim_data(
	txn: &mut FlowTransaction,
	node: FlowNodeId,
	cutoff: Cutoff,
	remaining: &mut ReclaimBudget,
	report: &mut ReclaimReport,
) -> Result<Vec<GroupId>> {
	let due = txn.due_groups(node, cutoff, remaining.groups)?;
	let mut released = Vec::new();
	for group in due {
		if remaining.exhausted() {
			report.backlog += 1;
			continue;
		}
		remaining.groups -= 1;
		let outcome = txn.reclaim_group_data(node, group, remaining.rows)?;
		remaining.rows -= outcome.removed;
		report.rows += outcome.removed;
		if outcome.more {
			report.backlog += 1;
			continue;
		}
		txn.defer_group(node, group)?;
		released.push(group);
		report.data_groups += 1;
	}
	Ok(released)
}

#[instrument(name = "lifecycle::operator::group::keyspace", level = "debug", skip_all, fields(node = node.0, keyspace = keyspace.0))]
fn reclaim_keyspace(
	txn: &mut FlowTransaction,
	node: FlowNodeId,
	keyspace: Keyspace,
	cutoff: Cutoff,
	remaining: &mut ReclaimBudget,
	report: &mut ReclaimReport,
) -> Result<Vec<GroupId>> {
	let due = txn.due_side_groups(node, keyspace, cutoff, remaining.groups)?;
	let mut retired = Vec::new();
	for group in due {
		if remaining.exhausted() {
			report.backlog += 1;
			continue;
		}
		remaining.groups -= 1;
		let outcome = txn.reclaim_group_keyspace(node, group, keyspace, remaining.rows)?;
		remaining.rows -= outcome.removed;
		report.rows += outcome.removed;
		if outcome.more {
			report.backlog += 1;
			continue;
		}
		txn.forget_side(node, group, keyspace)?;
		retired.push(group);
		report.keyspace_groups += 1;
	}
	Ok(retired)
}

#[instrument(name = "lifecycle::operator::group::identity", level = "debug", skip_all, fields(node = node.0))]
fn reclaim_identity(
	txn: &mut FlowTransaction,
	node: FlowNodeId,
	cutoff: Cutoff,
	remaining: &mut ReclaimBudget,
	report: &mut ReclaimReport,
) -> Result<()> {
	let due = txn.due_identity_groups(node, cutoff, remaining.groups)?;
	let mut reclaimed = Vec::new();
	for group in due {
		if remaining.exhausted() {
			report.backlog += 1;
			continue;
		}
		remaining.groups -= 1;
		let outcome = txn.reclaim_group_identity(node, group, remaining.rows)?;
		remaining.rows -= outcome.removed;
		report.rows += outcome.removed;
		if outcome.more {
			report.backlog += 1;
			continue;
		}
		reclaimed.push(group);
		report.identity_groups += 1;
	}
	if !reclaimed.is_empty() {
		txn.invalidate_row_number_groups(node, &GroupSet::new(reclaimed));
	}
	Ok(())
}

fn seal_cutoffs(
	horizon: Horizon,
	watermark: DateTime,
	identity_span: Option<Duration>,
	sealed_through: Option<DateTime>,
	slack: Duration,
	checkpoint: CommitVersion,
) -> Option<Cutoffs> {
	horizon.cutoff(watermark).map(|data| Cutoffs {
		data: Cutoff(sealed_through.map_or(data, |sealed| data.min(sealed))),
		identity: identity_span.map(|span| Cutoff(watermark.saturating_sub(span).saturating_sub(slack))),
		watermark,
		slack,
		data_floor: (Floor::Version(checkpoint), FloorTerm::OwningFlowCheckpoint),
		identity_floor: Some((Floor::Version(checkpoint), FloorTerm::OwningFlowCheckpoint)),
	})
}

fn seals_on_timer(ty: &FlowNodeType) -> bool {
	matches!(ty, FlowNodeType::Window { kind, .. } if !kind.size().is_some_and(|size| size.is_count()))
}

fn sink_storage(ty: &FlowNodeType) -> Option<StorageId> {
	match ty {
		FlowNodeType::SinkTableView {
			table,
			..
		} => Some(StorageId::Table(*table)),
		FlowNodeType::SinkRingBufferView {
			ringbuffer,
			..
		} => Some(StorageId::RingBuffer(*ringbuffer)),
		FlowNodeType::SinkSeriesView {
			series,
			..
		} => Some(StorageId::Series(*series)),
		_ => None,
	}
}

#[cfg(test)]
mod tests {
	use reifydb_catalog::catalog::Catalog;
	use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey, state::OperatorState};
	use reifydb_core::key::operator_state::{Keyspace, OperatorStateKey, group_inner_range, keyspace_inner_range};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_flow::transaction::ChangeCoordinate;
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_value::value::identity::IdentityId;

	use super::*;

	const NODE: FlowNodeId = FlowNodeId(1);
	const WIDTH: u64 = 100;

	fn ms(milliseconds: i64) -> Duration {
		Duration::from_milliseconds(milliseconds).expect("test duration must be representable")
	}

	fn budget(groups: usize, rows: usize) -> ReclaimBudget {
		ReclaimBudget {
			groups,
			rows,
		}
	}

	fn deferred(engine: &TestEngine) -> FlowTransaction {
		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		let txn = FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(0)),
		);
		// The width is never set on its own: it is always the one the node's horizon derives, and a
		// 1600ms seal horizon derives exactly WIDTH. Stamping therefore happens in the event domain.
		txn.group_interner().set_activity_grid(NODE, Horizon::of(ms(1_600)));
		txn
	}

	fn payload() -> EncodedRow {
		1u64.encode_state(DateTime::EPOCH).unwrap().into_row()
	}

	// A group with two data rows and a row-number mapping, interned at `position_ms`. The node is
	// event-domain (seal horizon above), so the substrate stamps Event(coordinate.at).
	fn seed(txn: &mut FlowTransaction, name: &str, position_ms: u64) -> GroupId {
		txn.set_change_coordinate(ChangeCoordinate {
			at: DateTime::from_millis(position_ms),
			version: CommitVersion(0),
		});
		let (id, _) = txn.intern_group(NODE, &EncodedKey::new(name.as_bytes())).unwrap();
		for suffix in [1u8, 2] {
			let key = OperatorStateKey::inner_encoded(id, Keyspace::ACCUMULATOR, vec![suffix]);
			txn.state_set(NODE, &key, payload()).unwrap();
		}
		let mapping = OperatorStateKey::inner_encoded(id, Keyspace::ROW_NUMBER_MAPPING, vec![1]);
		txn.state_set(NODE, &mapping, payload()).unwrap();
		id
	}

	fn rows(txn: &mut FlowTransaction, id: GroupId) -> usize {
		txn.state_range(NODE, group_inner_range(id), None).unwrap().items.len()
	}

	fn node_deferred(engine: &TestEngine, nodes: &[FlowNodeId]) -> FlowTransaction {
		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		let txn = FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(0)),
		);
		for node in nodes {
			txn.group_interner().set_activity_grid(*node, Horizon::of(ms(1_600)));
		}
		txn
	}

	// The same shape as `seed`, but for an arbitrary node so a sweep can be given more than one.
	fn seed_node(txn: &mut FlowTransaction, node: FlowNodeId, name: &str, position_ms: u64) -> GroupId {
		txn.set_change_coordinate(ChangeCoordinate {
			at: DateTime::from_millis(position_ms),
			version: CommitVersion(0),
		});
		let (id, _) = txn.intern_group(node, &EncodedKey::new(name.as_bytes())).unwrap();
		for suffix in [1u8, 2] {
			let key = OperatorStateKey::inner_encoded(id, Keyspace::ACCUMULATOR, vec![suffix]);
			txn.state_set(node, &key, payload()).unwrap();
		}
		id
	}

	// Only the accumulators, not the whole group range: the GROUP_RECORD lives in that range too and
	// deliberately survives the data phase, so counting the range would conflate "erased" with "left
	// exactly the record the second phase still needs".
	fn node_accumulators(txn: &mut FlowTransaction, node: FlowNodeId, id: GroupId) -> usize {
		txn.state_range(node, keyspace_inner_range(id, Keyspace::ACCUMULATOR), None).unwrap().items.len()
	}

	fn data_only(node: FlowNodeId, cutoff_ms: u64) -> SweepInputs {
		SweepInputs {
			node,
			data: Some(Cutoff(DateTime::from_millis(cutoff_ms))),
			identity: None,
			keyspaces: Vec::new(),
			mapping: None,
			mapping_cursor: None,
		}
	}

	#[test]
	fn a_node_that_exhausts_the_budget_starves_every_node_after_it() {
		// The loop shares one budget across nodes and stops dead when it runs out, and node order is
		// fixed ascending by id, so a high-cardinality low-id node can starve its successors forever.
		// Nothing in the report distinguishes "nothing was due" from "never reached", which is what
		// makes the starvation silent. This is the behaviour as it stands, pinned so that adding
		// fairness later is a deliberate change rather than an accident.
		let engine = TestEngine::new();
		let nodes = [FlowNodeId(1), FlowNodeId(2), FlowNodeId(3)];
		let mut txn = node_deferred(&engine, &nodes);
		let seeded: Vec<GroupId> = nodes.iter().map(|node| seed_node(&mut txn, *node, "idle", 50)).collect();

		let mut remaining = budget(1, 100);
		let mut report = ReclaimReport::default();
		let mut invalidated: Vec<FlowNodeId> = Vec::new();
		reclaim_nodes(
			nodes.iter().map(|node| data_only(*node, 1_000)).collect(),
			&mut txn,
			&mut remaining,
			&mut report,
			&mut |node, _| invalidated.push(node),
		)
		.unwrap();

		assert_eq!(report.data_groups, 1, "a one-group budget reclaims exactly one group");
		assert_eq!(invalidated, vec![nodes[0]], "and only the first node is ever reached");
		assert_eq!(node_accumulators(&mut txn, nodes[0], seeded[0]), 0);
		assert_eq!(node_accumulators(&mut txn, nodes[1], seeded[1]), 2, "node 2 was never scanned");
		assert_eq!(node_accumulators(&mut txn, nodes[2], seeded[2]), 2, "node 3 likewise");
	}

	#[test]
	fn a_node_with_no_data_cutoff_still_sweeps_its_keyspaces() {
		// The phases are independently gated, and this is the ordinary shape of a join declaring a ttl
		// on one side only: `later_of` returns perpetual unless BOTH sides declare a span, so the group
		// horizon is absent while the per-side spans are present. Gating the keyspace phase on the data
		// cutoff would leave exactly that configuration retaining forever while reporting a ttl.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let id = seed_sides(&mut txn, "keyed", 500, 900);

		let mut remaining = budget(10, 100);
		let mut report = ReclaimReport::default();
		reclaim_nodes(
			vec![SweepInputs {
				node: NODE,
				data: None,
				identity: None,
				keyspaces: vec![(Keyspace::JOIN_LEFT, Cutoff(DateTime::from_millis(800)))],
				mapping: None,
				mapping_cursor: None,
			}],
			&mut txn,
			&mut remaining,
			&mut report,
			&mut |_, _| {},
		)
		.unwrap();

		assert_eq!(report.keyspace_groups, 1, "a perpetual horizon must not disable the side sweep");
		assert_eq!(side_rows(&mut txn, id, Keyspace::JOIN_LEFT), 0);
		assert_eq!(side_rows(&mut txn, id, Keyspace::JOIN_RIGHT), 2, "the longer-lived side survives");
	}

	#[test]
	fn the_keyspace_order_decides_what_a_truncated_budget_reaches() {
		// The keyspace list is walked under the shared budget with an early break, so a keyspace listed
		// second is only swept if the budget survives the first. That is why a join declares JOIN_LEFT
		// before the ledger keyspaces describing what those left rows published: under truncation the
		// left rows go first and the record of them outlives the sweep, never the reverse.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let id = seed_sides(&mut txn, "keyed", 500, 500);
		let cutoff = Cutoff(DateTime::from_millis(800));

		// One group of budget: enough for exactly the first keyspace named.
		let mut remaining = budget(1, 100);
		let mut report = ReclaimReport::default();
		reclaim_nodes(
			vec![SweepInputs {
				node: NODE,
				data: None,
				identity: None,
				keyspaces: vec![(Keyspace::JOIN_LEFT, cutoff), (Keyspace::JOIN_RIGHT, cutoff)],
				mapping: None,
				mapping_cursor: None,
			}],
			&mut txn,
			&mut remaining,
			&mut report,
			&mut |_, _| {},
		)
		.unwrap();

		assert_eq!(side_rows(&mut txn, id, Keyspace::JOIN_LEFT), 0, "the first keyspace named is swept");
		assert_eq!(
			side_rows(&mut txn, id, Keyspace::JOIN_RIGHT),
			2,
			"the second is not reached, which is the whole reason the order is declared"
		);
	}

	#[test]
	fn the_mapping_cursor_is_carried_in_and_handed_back() {
		// The sweep keeps no state between ticks: a half-finished node-mapping scan hands its position
		// back to the caller, who feeds it in again next tick. Holding it inside the sweep would make
		// the function unusable outside the one engine that owns the map.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let mut remaining = budget(10, 100);
		let mut report = ReclaimReport::default();

		let outcome = reclaim_nodes(
			vec![data_only(NODE, 1_000)],
			&mut txn,
			&mut remaining,
			&mut report,
			&mut |_, _| {},
		)
		.unwrap();

		assert_eq!(outcome.cursors.len(), 1, "every swept node reports its cursor, even an unset one");
		assert_eq!(outcome.cursors[0].0, NODE);
	}

	#[test]
	fn a_group_is_handed_to_invalidation_only_once_it_is_fully_drained() {
		// Invalidation revokes RAM caches that mirror the store, so handing back a half-drained group
		// would drop a filter that is still correct for the rows it has left. The row budget here can
		// only take one of the group's two rows.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		seed(&mut txn, "big", 50);

		let mut remaining = budget(10, 1);
		let mut report = ReclaimReport::default();
		let mut invalidated: Vec<FlowNodeId> = Vec::new();
		reclaim_nodes(vec![data_only(NODE, 1_000)], &mut txn, &mut remaining, &mut report, &mut |node, _| {
			invalidated.push(node)
		})
		.unwrap();

		assert!(invalidated.is_empty(), "a partially drained group must not be handed back");
		assert_eq!(report.backlog, 1, "and the caller must be told there is work left");
	}

	#[test]
	fn the_data_phase_erases_state_and_leaves_identity_for_the_second_phase() {
		// The whole point of splitting the phases: a sink row can still name the row-number mapping long
		// after the accumulators behind it are worthless. Taking both at once means the next
		// event on that group mints a second row number for a row that already exists.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let id = seed(&mut txn, "idle", 50);
		let mut remaining = budget(10, 100);
		let mut report = ReclaimReport::default();

		let released =
			reclaim_data(&mut txn, NODE, Cutoff(DateTime::from_millis(1_000)), &mut remaining, &mut report)
				.unwrap();

		assert_eq!(released, vec![id], "the caller needs the group ids to drop their cached rows");
		assert_eq!(report.data_groups, 1);
		assert_eq!(report.rows, 2, "both accumulator rows, and nothing from the identity keyspaces");
		assert_eq!(rows(&mut txn, id), 2, "the mapping and the group record must survive the data phase");
	}

	fn seed_sides(txn: &mut FlowTransaction, name: &str, left_ms: u64, right_ms: u64) -> GroupId {
		txn.set_change_coordinate(ChangeCoordinate {
			at: DateTime::from_millis(left_ms),
			version: CommitVersion(0),
		});
		let (id, _) = txn.intern_group(NODE, &EncodedKey::new(name.as_bytes())).unwrap();
		for (keyspace, at) in [(Keyspace::JOIN_LEFT, left_ms), (Keyspace::JOIN_RIGHT, right_ms)] {
			txn.set_change_coordinate(ChangeCoordinate {
				at: DateTime::from_millis(at),
				version: CommitVersion(0),
			});
			for suffix in [1u8, 2] {
				let key = OperatorStateKey::inner_encoded(id, keyspace, vec![suffix]);
				txn.state_set(NODE, &key, payload()).unwrap();
			}
			txn.stamp_side(NODE, id, keyspace).unwrap();
		}
		id
	}

	fn side_rows(txn: &mut FlowTransaction, id: GroupId, keyspace: Keyspace) -> usize {
		txn.state_range(NODE, keyspace_inner_range(id, keyspace), None).unwrap().items.len()
	}

	#[test]
	fn the_keyspace_phase_retires_one_side_of_a_group_and_spares_the_other() {
		// The join's reason for existing in this sweep. Both sides share a group, so the group-level
		// phases can only offer them one horizon; a join declaring a 60s left ttl against an hour-long
		// right ttl needs the left rows gone while the right rows are still being probed. Here the left
		// side was last active 500ms in and the right 900ms in, and a cutoff between them must take
		// exactly one.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let id = seed_sides(&mut txn, "keyed", 500, 900);
		let mut remaining = budget(10, 100);
		let mut report = ReclaimReport::default();

		reclaim_keyspace(
			&mut txn,
			NODE,
			Keyspace::JOIN_LEFT,
			Cutoff(DateTime::from_millis(800)),
			&mut remaining,
			&mut report,
		)
		.unwrap();

		assert_eq!(report.keyspace_groups, 1);
		assert_eq!(report.rows, 2, "both left rows");
		assert_eq!(side_rows(&mut txn, id, Keyspace::JOIN_LEFT), 0);
		assert_eq!(
			side_rows(&mut txn, id, Keyspace::JOIN_RIGHT),
			2,
			"the right side was active later and its ttl still covers it"
		);
	}

	#[test]
	fn a_retired_side_hands_its_groups_back_so_ram_state_can_drop_them() {
		// The group-level phase returns released ids so the operator can invalidate the RAM state
		// that mirrors them; the side phase has to do the same or the join's membership filter keeps
		// claiming keys whose rows the sweep just deleted, and every probe of them pays a store read
		// that can only ever miss. A half-drained side must NOT be handed back - its rows are still
		// there and invalidating early would drop a filter that is still correct.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let id = seed_sides(&mut txn, "keyed", 500, 900);
		let cutoff = Cutoff(DateTime::from_millis(800));

		let mut remaining = budget(10, 1);
		let mut report = ReclaimReport::default();
		let partial =
			reclaim_keyspace(&mut txn, NODE, Keyspace::JOIN_LEFT, cutoff, &mut remaining, &mut report)
				.unwrap();
		assert!(partial.is_empty(), "an unfinished side must not be handed back");

		let mut remaining = budget(10, 100);
		let mut report = ReclaimReport::default();
		let retired =
			reclaim_keyspace(&mut txn, NODE, Keyspace::JOIN_LEFT, cutoff, &mut remaining, &mut report)
				.unwrap();

		assert_eq!(retired, vec![id], "a drained side hands its group back for RAM invalidation");
	}

	#[test]
	fn a_retired_side_is_not_offered_again() {
		// forget_side is what stops the sweep re-reclaiming an empty keyspace on every pass. Without
		// it the side stays due forever and burns the group budget that live groups need.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		seed_sides(&mut txn, "keyed", 500, 900);
		let mut remaining = budget(10, 100);
		let mut report = ReclaimReport::default();
		let cutoff = Cutoff(DateTime::from_millis(800));
		reclaim_keyspace(&mut txn, NODE, Keyspace::JOIN_LEFT, cutoff, &mut remaining, &mut report).unwrap();

		let mut second = ReclaimReport::default();
		reclaim_keyspace(&mut txn, NODE, Keyspace::JOIN_LEFT, cutoff, &mut remaining, &mut second).unwrap();

		assert_eq!(second.keyspace_groups, 0, "a drained side must not come back");
		assert_eq!(second.rows, 0);
	}

	#[test]
	fn retiring_a_side_leaves_the_groups_identity_alone() {
		// The side sweep deliberately does not defer the group: identity keeps ageing on the per-group
		// index at the later of the two ttls, because a sink row can still name the row-number mapping
		// after one side of the join is gone. Deferring here would drop that mapping early and the next
		// event on the key would mint a duplicate row.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let name = EncodedKey::new(b"keyed");
		let id = seed_sides(&mut txn, "keyed", 500, 900);
		let mut remaining = budget(10, 100);
		let mut report = ReclaimReport::default();

		reclaim_keyspace(
			&mut txn,
			NODE,
			Keyspace::JOIN_LEFT,
			Cutoff(DateTime::from_millis(800)),
			&mut remaining,
			&mut report,
		)
		.unwrap();

		assert_eq!(
			txn.lookup_group(NODE, &name).unwrap(),
			Some(id),
			"the group must still resolve: only one of its keyspaces retired"
		);
		assert!(
			txn.due_identity_groups(NODE, Cutoff(DateTime::from_millis(100_000)), 10).unwrap().is_empty(),
			"a side sweep must not enrol the group in the identity phase"
		);
		assert_eq!(
			txn.due_groups(NODE, Cutoff(DateTime::from_millis(100_000)), 10).unwrap(),
			vec![id],
			"the group still ages on its own index, at its own horizon"
		);
	}

	#[test]
	fn a_side_the_row_budget_cannot_drain_stays_due_until_it_is_finished() {
		// On the per-side path a high-cardinality side must not be dropped in one
		// unbounded delete. The half-drained side has to remain due, or the rows it still holds are
		// stranded until the group's own horizon - which for a short left side against a long right
		// ttl could be hours.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let id = seed_sides(&mut txn, "keyed", 500, 900);
		let cutoff = Cutoff(DateTime::from_millis(800));
		let mut remaining = budget(10, 1);
		let mut report = ReclaimReport::default();

		reclaim_keyspace(&mut txn, NODE, Keyspace::JOIN_LEFT, cutoff, &mut remaining, &mut report).unwrap();

		assert_eq!(report.rows, 1, "the budget allowed exactly one row");
		assert_eq!(report.keyspace_groups, 0, "an unfinished side is not a retired side");
		assert_eq!(report.backlog, 1, "the caller must be told to come back");
		assert_eq!(side_rows(&mut txn, id, Keyspace::JOIN_LEFT), 1);

		let mut remaining = budget(10, 100);
		let mut report = ReclaimReport::default();
		reclaim_keyspace(&mut txn, NODE, Keyspace::JOIN_LEFT, cutoff, &mut remaining, &mut report).unwrap();

		assert_eq!(report.keyspace_groups, 1, "the second pass finishes it");
		assert_eq!(side_rows(&mut txn, id, Keyspace::JOIN_LEFT), 0);
	}

	#[test]
	fn a_side_span_is_measured_from_the_watermark_with_the_same_slack_identity_gets() {
		// The side cutoff is derived in the driver from cutoffs.watermark and cutoffs.slack, so those
		// have to survive on the struct with the meaning the identity cutoff already gives them:
		// bucketed activity means a group stamped anywhere inside a bucket reads as active at the
		// bucket start, and subtracting one width is what stops a side being retired mid-bucket.
		let cutoffs = seal_cutoffs(
			Horizon::of(ms(1_600)),
			DateTime::from_millis(1_000_000),
			Some(ms(60_000)),
			None,
			ms(WIDTH as i64),
			CommitVersion(7),
		)
		.unwrap();

		assert_eq!(cutoffs.watermark, DateTime::from_millis(1_000_000));
		assert_eq!(cutoffs.slack, ms(WIDTH as i64));
		assert_eq!(
			Cutoff(cutoffs.watermark.saturating_sub(ms(60_000)).saturating_sub(cutoffs.slack)),
			cutoffs.identity.unwrap(),
			"a side span of the same length must land exactly where the identity span does"
		);
	}

	#[test]
	fn the_identity_phase_only_takes_groups_the_data_phase_already_finished() {
		// The identity scan reads a different index precisely so it can never reach a live group. If it
		// could, a group merely idle enough for its data horizon would lose its mapping at the same
		// moment - collapsing the two horizons into one.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let id = seed(&mut txn, "idle", 50);
		let mut remaining = budget(10, 100);
		let mut report = ReclaimReport::default();

		reclaim_identity(&mut txn, NODE, Cutoff(DateTime::from_millis(1_000)), &mut remaining, &mut report)
			.unwrap();
		assert_eq!(report.identity_groups, 0, "a group that still holds data is not an identity candidate");
		assert_eq!(rows(&mut txn, id), 4);

		reclaim_data(&mut txn, NODE, Cutoff(DateTime::from_millis(1_000)), &mut remaining, &mut report)
			.unwrap();
		reclaim_identity(&mut txn, NODE, Cutoff(DateTime::from_millis(1_000)), &mut remaining, &mut report)
			.unwrap();

		assert_eq!(report.identity_groups, 1);
		assert_eq!(rows(&mut txn, id), 0, "after both phases the group's range must be empty");
		assert_eq!(
			txn.lookup_group(NODE, &EncodedKey::new(b"idle")).unwrap(),
			None,
			"and the dictionary entry must go with it"
		);
	}

	#[test]
	fn the_identity_phase_drops_the_row_number_cache_so_no_ghost_survives() {
		// Phase 2 deletes a group's mapping rows from the store, but the row-number provider still
		// holds (id, key) -> row number in memory. A reborn group is handed a fresh id so the entry is
		// never queried again, yet it must be dropped or the provider cache grows without bound as
		// groups reclaim - and a query on the reclaimed id must never serve the stale number.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		txn.set_change_coordinate(ChangeCoordinate {
			at: DateTime::from_millis(50),
			version: CommitVersion(0),
		});
		let (id, _) = txn.intern_group(NODE, &EncodedKey::new(b"idle")).unwrap();
		let key = EncodedKey::new(b"sink");
		txn.get_or_create_row_number(NODE, id, &key).unwrap();
		for suffix in [1u8, 2] {
			let data = OperatorStateKey::inner_encoded(id, Keyspace::ACCUMULATOR, vec![suffix]);
			txn.state_set(NODE, &data, payload()).unwrap();
		}
		let mut remaining = budget(10, 100);
		let mut report = ReclaimReport::default();

		reclaim_data(&mut txn, NODE, Cutoff(DateTime::from_millis(1_000)), &mut remaining, &mut report)
			.unwrap();
		reclaim_identity(&mut txn, NODE, Cutoff(DateTime::from_millis(1_000)), &mut remaining, &mut report)
			.unwrap();

		assert_eq!(rows(&mut txn, id), 0, "both phases must empty the group's range");
		assert_eq!(
			txn.get_row_number(NODE, id, &key).unwrap(),
			None,
			"the reclaimed group's mapping must not survive in the provider cache as a ghost"
		);
	}

	#[test]
	fn a_reclaimed_group_is_not_offered_to_the_data_phase_again() {
		// Between the two horizons a group has nothing left to erase but is still due by the data
		// cutoff. If it kept coming back, every tick would spend its group budget rediscovering
		// leftovers and the groups that still hold state would starve behind them.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		seed(&mut txn, "idle", 50);
		let mut remaining = budget(10, 100);
		let mut report = ReclaimReport::default();
		reclaim_data(&mut txn, NODE, Cutoff(DateTime::from_millis(1_000)), &mut remaining, &mut report)
			.unwrap();

		let again =
			reclaim_data(&mut txn, NODE, Cutoff(DateTime::from_millis(1_000)), &mut remaining, &mut report)
				.unwrap();

		assert!(again.is_empty());
		assert_eq!(report.data_groups, 1, "the second pass must find nothing to do");
		assert_eq!(remaining.groups, 9, "and must not spend group budget rediscovering it");
	}

	#[test]
	fn a_group_the_row_budget_cannot_drain_stays_live_until_it_is_finished() {
		// Marking a half-erased group as reclaimed would strand its remaining rows: the data scan would
		// never offer it again, and the identity phase would delete the record that addresses them. A
		// partial group must stay exactly where it is and be reported as backlog.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let id = seed(&mut txn, "big", 50);
		let mut remaining = budget(10, 1);
		let mut report = ReclaimReport::default();

		let released =
			reclaim_data(&mut txn, NODE, Cutoff(DateTime::from_millis(1_000)), &mut remaining, &mut report)
				.unwrap();

		assert!(released.is_empty(), "a group that is not drained must not be handed to the operator");
		assert_eq!(report.data_groups, 0);
		assert_eq!(report.backlog, 1, "the caller must learn there is work left");
		assert_eq!(rows(&mut txn, id), 3, "one data row went; the other and the mapping remain");

		let mut remaining = budget(10, 100);
		let released =
			reclaim_data(&mut txn, NODE, Cutoff(DateTime::from_millis(1_000)), &mut remaining, &mut report)
				.unwrap();

		assert_eq!(released, vec![id], "the next tick resumes the same group and finishes it");
		assert_eq!(report.data_groups, 1);
	}

	#[test]
	fn the_group_budget_bounds_how_many_groups_one_tick_touches() {
		// Every reclaimed row is a tombstone write on the single write mutex, so a tick
		// that took every due group at once would be a latency incident on the first high-cardinality
		// node to go idle.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		for i in 0..5 {
			seed(&mut txn, &format!("g{i}"), 50);
		}
		let mut remaining = budget(2, 100);
		let mut report = ReclaimReport::default();

		let released =
			reclaim_data(&mut txn, NODE, Cutoff(DateTime::from_millis(1_000)), &mut remaining, &mut report)
				.unwrap();

		assert_eq!(released.len(), 2);
		assert_eq!(report.data_groups, 2);
		assert_eq!(remaining.groups, 0);
	}

	#[test]
	fn a_seal_node_measures_both_phases_in_event_time() {
		// Seal spans are window coordinates and idle spans are wall-clock; there is no exchange rate. A
		// seal node's identity horizon therefore has to be subtracted from the same watermark its data
		// horizon is, or the two phases would be ordered by comparing milliseconds against commit
		// versions and could fire in either order.
		let horizon = Horizon::of(ms(1_600));
		let cutoffs = seal_cutoffs(
			horizon,
			DateTime::from_millis(1_000_000),
			Some(ms(60_000)),
			None,
			ms(WIDTH as i64),
			CommitVersion(7),
		)
		.unwrap();

		assert_eq!(cutoffs.data, Cutoff(DateTime::from_millis(998_400)), "watermark minus the seal span");
		assert_eq!(
			cutoffs.identity,
			Some(Cutoff(DateTime::from_millis(1_000_000 - 60_000 - WIDTH))),
			"watermark minus the sink row ttl, minus one bucket of slack"
		);
		assert!(
			cutoffs.identity.unwrap().raw() < cutoffs.data.raw(),
			"identity must always trail data, or the mapping dies before the state does"
		);
	}

	#[test]
	fn a_seal_node_with_a_forever_sink_reclaims_data_and_keeps_identity() {
		// A sink row with no ttl lives forever, so the mapping it names has to as well. This is the
		// honest perpetual case: the data phase still bounds the accumulators, and the identity residue
		// is what the step-8 report is for.
		let cutoffs = seal_cutoffs(
			Horizon::of(ms(1_600)),
			DateTime::from_millis(1_000_000),
			None,
			None,
			ms(WIDTH as i64),
			CommitVersion(7),
		)
		.unwrap();

		assert_eq!(cutoffs.data, Cutoff(DateTime::from_millis(998_400)));
		assert_eq!(cutoffs.identity, None);
	}

	#[test]
	fn reclaim_never_advances_past_what_the_operator_has_sealed() {
		// A live defect, not a hypothetical. Once reclaim reads the FLOW watermark, a processing
		// domain flow's watermark is the wall clock and advances forever - so an idle node's state
		// was reclaimed roughly one span after its last write, which beat that node's own Seal timer.
		// The seal then fired, found the accumulator already erased, emitted NO withdrawal, and the
		// row it had published stayed in the view forever while its window result was lost. Silent
		// in every log.
		// The reclaim cutoff must therefore never pass the operator's seal ledger: state that has not
		// been sealed is not reclaimable, whatever the clock says.
		let horizon = Horizon::of(ms(1_600));
		let watermark = DateTime::from_millis(1_000_000);
		let sealed = DateTime::from_millis(990_000);

		let clamped = seal_cutoffs(horizon, watermark, None, Some(sealed), ms(WIDTH as i64), CommitVersion(7))
			.unwrap();
		assert_eq!(
			clamped.data,
			Cutoff(sealed),
			"the seal ledger is behind the watermark derived cutoff, so it must be the one that binds"
		);

		// An operator that has sealed further than the horizon reaches must not be dragged FORWARD by
		// the clamp - the horizon still bounds it.
		let ahead = seal_cutoffs(
			horizon,
			watermark,
			None,
			Some(DateTime::from_millis(999_999)),
			ms(WIDTH as i64),
			CommitVersion(7),
		)
		.unwrap();
		assert_eq!(
			ahead.data,
			Cutoff(DateTime::from_millis(998_400)),
			"the clamp is a floor on aggressiveness, never a licence to reclaim further"
		);

		// An operator that seals nothing (count based, or any non sealing operator) is unconstrained.
		let unsealed =
			seal_cutoffs(horizon, watermark, None, None, ms(WIDTH as i64), CommitVersion(7)).unwrap();
		assert_eq!(unsealed.data, Cutoff(DateTime::from_millis(998_400)));
	}

	#[test]
	fn the_owning_flow_is_always_the_binding_term_for_both_floors() {
		// What survives of a_lagging_flow_holds_both_idle_cutoffs_down: a flow parked below the
		// cutoff has input it has not applied yet, and reclaiming above its checkpoint would erase
		// state its own unprocessed changes still refer to. The floor therefore binds to the flow's
		// checkpoint, and the binding has to NAME the flow or a stalled node is indistinguishable
		// from an idle one in the report.
		// The cutoff itself is event-domain and no longer clamped by the checkpoint, so the
		// checkpoint shows up exclusively as the reported floor. That is what this pins.
		let cutoffs = seal_cutoffs(
			Horizon::of(ms(1_600)),
			DateTime::from_millis(1_000_000),
			Some(ms(60_000)),
			None,
			ms(WIDTH as i64),
			CommitVersion(10),
		)
		.unwrap();

		assert_eq!(cutoffs.data_floor, (Floor::Version(CommitVersion(10)), FloorTerm::OwningFlowCheckpoint));
		assert_eq!(
			cutoffs.identity_floor,
			Some((Floor::Version(CommitVersion(10)), FloorTerm::OwningFlowCheckpoint))
		);
	}

	#[test]
	fn a_perpetual_node_produces_no_cutoff() {
		// Nothing derivable means nothing reclaimable. A cutoff of zero here would be equally safe, but
		// it would still cost a scan per tick per node forever.
		assert_eq!(
			seal_cutoffs(
				Horizon::Perpetual,
				DateTime::from_millis(1_000_000),
				Some(ms(60_000)),
				None,
				ms(WIDTH as i64),
				CommitVersion(7)
			),
			None
		);
	}

	#[test]
	fn the_reported_floor_is_the_lowest_across_every_node_in_the_flow() {
		// The plane reports one floor per class, and a class is only as free as its most constrained
		// node. Reporting a later node's healthier floor would hide the one node actually holding
		// reclamation back.
		let at = |checkpoint: u64| {
			seal_cutoffs(
				Horizon::of(ms(1_600)),
				DateTime::from_millis(1_000_000),
				None,
				None,
				ms(WIDTH as i64),
				CommitVersion(checkpoint),
			)
			.unwrap()
		};
		let mut report = ReclaimReport::default();
		report.bind(&at(9_000));
		report.bind(&at(400));
		report.bind(&at(7_000));

		assert_eq!(
			report.data_floor,
			Some((Floor::Version(CommitVersion(400)), FloorTerm::OwningFlowCheckpoint))
		);
	}

	#[test]
	fn the_slack_is_the_width_the_interner_actually_stamps_with() {
		// The slack exists because a group's recorded last-activity is only accurate to one bucket, so it
		// has to be the width the stamping side really used. Deriving it independently from the horizon
		// would silently diverge whenever the two disagreed, and an identity cutoff that is one bucket
		// too high retires a mapping while the sink row naming it is still inside its own ttl.
		let horizon = Horizon::of(ms(1_600));

		assert_eq!(
			horizon.buckets().event_grid().expect("a seal horizon buckets in event time").width(),
			ms(WIDTH as i64),
			"the width registration derives for this horizon"
		);
		assert_eq!(
			seal_cutoffs(
				horizon,
				DateTime::from_millis(1_000_000),
				Some(ms(60_000)),
				None,
				ms(WIDTH as i64),
				CommitVersion(7)
			)
			.unwrap()
			.identity,
			Some(Cutoff(DateTime::from_millis(939_900)))
		);
		assert_eq!(
			seal_cutoffs(
				horizon,
				DateTime::from_millis(1_000_000),
				Some(ms(60_000)),
				None,
				ms(4_096),
				CommitVersion(7)
			)
			.unwrap()
			.identity,
			Some(Cutoff(DateTime::from_millis(935_904))),
			"a node stamping with a wider bucket must get correspondingly wider slack"
		);
	}
}

#[cfg(test)]
mod sink_storage_tests {
	use reifydb_core::interface::catalog::{
		id::{RingBufferId, SeriesId, TableId, ViewId},
		series::{SeriesKey, TimestampPrecision},
		storage::StorageId,
	};
	use reifydb_rql::flow::node::FlowNodeType;

	use super::sink_storage;

	/// A sink node carries BOTH the view id and the id of the storage it materialises into, and row
	/// settings are only ever recorded against the storage. Returning the view here is well-typed but
	/// wrong: the lookup misses, the flow reads as perpetual, and its rows are never reclaimed. The
	/// ids are deliberately distinct so returning the wrong half cannot accidentally pass.
	#[test]
	fn a_sink_resolves_to_the_storage_it_writes_not_the_view_it_presents() {
		assert_eq!(
			sink_storage(&FlowNodeType::SinkTableView {
				view: ViewId(1),
				table: TableId(2),
			}),
			Some(StorageId::Table(TableId(2)))
		);

		assert_eq!(
			sink_storage(&FlowNodeType::SinkRingBufferView {
				view: ViewId(3),
				ringbuffer: RingBufferId(4),
				capacity: 16,
			}),
			Some(StorageId::RingBuffer(RingBufferId(4)))
		);

		assert_eq!(
			sink_storage(&FlowNodeType::SinkSeriesView {
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

	/// Only sinks own storage. A source or operator node must not resolve to one, otherwise retention
	/// would attribute a row TTL to a node that never writes rows.
	#[test]
	fn a_node_that_owns_no_storage_resolves_to_nothing() {
		assert_eq!(
			sink_storage(&FlowNodeType::SourceTable {
				table: TableId(9)
			}),
			None
		);
	}
}

#[cfg(test)]
mod identity_span_tests {
	use reifydb_core::interface::catalog::{
		flow::{FlowId, FlowNodeId},
		id::{SubscriptionId, TableId, ViewId},
		storage::StorageId,
	};
	use reifydb_rql::flow::{
		flow::FlowDag,
		node::{FlowEdge, FlowNode, FlowNodeType},
	};
	use reifydb_value::value::duration::Duration;

	use crate::execution::reclaim::identity_span;

	fn ms(milliseconds: i64) -> Duration {
		Duration::from_milliseconds(milliseconds).expect("test duration must be representable")
	}

	/// A view compiles to one source, one operator and one sink. The edges are wired even though the
	/// resolver scans nodes rather than walking them, so the fixture stays the shape of a real flow.
	fn dag(nodes: &[(u64, FlowNodeType)], edges: &[(u64, u64)]) -> FlowDag {
		let mut builder = FlowDag::builder(FlowId(1));
		for (id, ty) in nodes {
			builder.add_node(FlowNode::new(FlowNodeId(*id), ty.clone()));
		}
		for (index, (source, target)) in edges.iter().enumerate() {
			builder.add_edge(FlowEdge::new(index as u64 + 1, *source, *target)).expect("edge");
		}
		builder.build()
	}

	fn source() -> FlowNodeType {
		FlowNodeType::SourceTable {
			table: TableId(1),
		}
	}

	fn operator() -> FlowNodeType {
		FlowNodeType::Append {}
	}

	#[test]
	fn a_flows_identity_is_bounded_by_its_sinks_row_ttl() {
		// The sink's row ttl is the only thing that can bound identity: the mapping has to outlive the
		// row naming it, and the row lives exactly that long. Resolving to anything shorter retires the
		// mapping under a live row, and the next event on that key mints a second row over it.
		let flow = dag(
			&[
				(1, source()),
				(2, operator()),
				(
					3,
					FlowNodeType::SinkTableView {
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
		// A sink with no declared row ttl keeps its rows forever, so no mapping one of those rows names
		// may ever be reclaimed. None is the safe answer here and the only safe answer: any duration
		// would eventually retire a mapping while the row still points at it.
		let flow = dag(
			&[
				(1, operator()),
				(
					2,
					FlowNodeType::SinkTableView {
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
		// Subscription flows do reach the sweep, and a subscription owns no storage, so there are no
		// row settings to consult and its rows are not ours to age. The resolver must find no sink at
		// all rather than mistaking the subscription for one, and identity stays perpetual.
		let flow = dag(
			&[
				(1, operator()),
				(
					2,
					FlowNodeType::SinkSubscription {
						subscription: SubscriptionId(7),
					},
				),
			],
			&[(1, 2)],
		);

		assert_eq!(identity_span(&flow, |_| Some(ms(60_000))), None);
	}
}
