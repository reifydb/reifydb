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
	state::horizon::Cutoff,
};
use reifydb_flow::transaction::FlowTransaction;
use reifydb_rql::flow::{flow::FlowDag, node::FlowNodeType};
use reifydb_value::{
	Result,
	value::{datetime::DateTime, duration::Duration},
};
use tracing::{Span, field, instrument};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhaseReclaim {
	pub cutoff: Cutoff,
	pub groups: Vec<GroupId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyspaceReclaim {
	pub keyspace: Keyspace,
	pub cutoff: Cutoff,
	pub groups: Vec<GroupId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MappingReclaim {
	pub cutoff: Cutoff,
	pub rows: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeReclaim {
	pub node: FlowNodeId,
	pub data: Option<PhaseReclaim>,
	pub identity: Option<PhaseReclaim>,
	pub keyspaces: Vec<KeyspaceReclaim>,
	pub mapping: Option<MappingReclaim>,
}

impl NodeReclaim {
	fn new(node: FlowNodeId) -> Self {
		Self {
			node,
			data: None,
			identity: None,
			keyspaces: Vec::new(),
			mapping: None,
		}
	}
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReclaimReport {
	pub data_groups: usize,
	pub identity_groups: usize,
	pub keyspace_groups: usize,
	pub rows: usize,
	pub backlog: usize,
	pub perpetual_nodes: usize,
	pub ungridded_nodes: usize,
	pub data_floor: Option<(Floor, FloorTerm)>,
	pub identity_floor: Option<(Floor, FloorTerm)>,
	pub nodes: Vec<NodeReclaim>,
}

impl ReclaimReport {
	fn bind(&mut self, data: Option<Cutoff>, checkpoint: CommitVersion) {
		if data.is_none() {
			return;
		}
		let floor = (Floor::Version(checkpoint), FloorTerm::OwningFlowCheckpoint);
		self.data_floor = lowest(self.data_floor, Some(floor));
		self.identity_floor = lowest(self.identity_floor, Some(floor));
	}

	pub fn node(&self, node: FlowNodeId) -> Option<&NodeReclaim> {
		self.nodes.iter().find(|reclaim| reclaim.node == node)
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

impl FlowEngineInner {
	#[instrument(
		name = "lifecycle::operator::group::scan",
		level = "debug",
		skip(self, txn),
		fields(flow_id = ?flow_id, perpetual_nodes = field::Empty, ungridded_nodes = field::Empty)
	)]
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

		let identity = identity_cutoff(identity_span, watermark);

		let mut inputs = Vec::new();
		for node_id in flow.get_node_ids() {
			let Some(_) = flow.get_node(&node_id) else {
				continue;
			};
			let Some(operator) = self.operators.get(&node_id) else {
				continue;
			};
			if !operator.capabilities().contains(&OperatorCapability::Reclaim) {
				report.perpetual_nodes += 1;
				continue;
			}
			let reclaimable = operator.reclaimable_through(txn, watermark)?;
			self.executor.services().node_retention_store.set_frontier(node_id, reclaimable.data);
			if reclaimable.is_empty() {
				report.perpetual_nodes += 1;
				continue;
			}
			if self.substrate.group.buckets(node_id).event_grid().is_none() {
				report.ungridded_nodes += 1;
				continue;
			}
			report.bind(reclaimable.data.map(Cutoff), checkpoint);
			inputs.push(SweepInputs {
				node: node_id,
				data: reclaimable.data.map(Cutoff),
				identity,
				keyspaces: reclaimable
					.keyspaces
					.into_iter()
					.map(|(keyspace, at)| (keyspace, Cutoff(at)))
					.collect(),
				mapping: reclaimable.mapping.map(Cutoff),
				mapping_cursor: self.mapping_cursors.entry(node_id).or_default().clone(),
			});
		}

		let span = Span::current();
		span.record("perpetual_nodes", report.perpetual_nodes);
		span.record("ungridded_nodes", report.ungridded_nodes);

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
		let mut reclaimed = NodeReclaim::new(node);

		if let Some(cutoff) = input.identity {
			let groups = reclaim_identity(txn, node, cutoff, remaining, report)?;
			reclaimed.identity = Some(PhaseReclaim {
				cutoff,
				groups,
			});
		}

		for (keyspace, cutoff) in input.keyspaces {
			if remaining.exhausted() {
				break;
			}
			let retired = reclaim_keyspace(txn, node, keyspace, cutoff, remaining, report)?;
			if !retired.is_empty() {
				invalidate(node, GroupSet::new(retired.clone()));
			}
			reclaimed.keyspaces.push(KeyspaceReclaim {
				keyspace,
				cutoff,
				groups: retired,
			});
		}

		let mut cursor = input.mapping_cursor;
		if let Some(cutoff) = mapping_cutoff(input.mapping, input.identity)
			&& !remaining.exhausted()
		{
			let removed =
				txn.evict_row_numbers(node, GroupId::NODE_SCOPE, cutoff, &mut cursor, remaining.rows)?;
			remaining.rows -= removed;
			report.rows += removed;
			if cursor.is_some() {
				report.backlog += 1;
			}
			reclaimed.mapping = Some(MappingReclaim {
				cutoff,
				rows: removed,
			});
		}
		cursors.push((node, cursor));

		if let Some(cutoff) = input.data {
			let released = reclaim_data(txn, node, cutoff, remaining, report)?;
			if !released.is_empty() {
				invalidate(node, GroupSet::new(released.clone()));
			}
			reclaimed.data = Some(PhaseReclaim {
				cutoff,
				groups: released,
			});
		}

		report.nodes.push(reclaimed);
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

fn mapping_cutoff(declared: Option<Cutoff>, identity: Option<Cutoff>) -> Option<Cutoff> {
	match identity {
		Some(identity) => declared.map(|declared| Cutoff(declared.instant().min(identity.instant()))),
		None => None,
	}
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
) -> Result<Vec<GroupId>> {
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
		txn.invalidate_row_number_groups(node, &GroupSet::new(reclaimed.clone()));
	}
	Ok(reclaimed)
}

fn identity_cutoff(identity_span: Option<Duration>, watermark: DateTime) -> Option<Cutoff> {
	identity_span.map(|span| Cutoff(watermark.saturating_sub(span)))
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
		txn.group_interner().set_activity_grid(NODE, Some(ms(1_600)));
		txn
	}

	fn payload() -> EncodedRow {
		1u64.encode_state(DateTime::EPOCH).unwrap().into_row()
	}

	fn seed(txn: &mut FlowTransaction, name: &str, position_ms: u64) -> GroupId {
		// Two data rows and a row-number mapping, interned at `position_ms`: the node is
		// event-domain, so the substrate stamps Event(coordinate.at).
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
			txn.group_interner().set_activity_grid(*node, Some(ms(1_600)));
		}
		txn
	}

	fn seed_node(txn: &mut FlowTransaction, node: FlowNodeId, name: &str, position_ms: u64) -> GroupId {
		// The same shape as `seed`, but for an arbitrary node so a sweep can be given more
		// than one.
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

	fn node_accumulators(txn: &mut FlowTransaction, node: FlowNodeId, id: GroupId) -> usize {
		// Not the whole group range: the GROUP_RECORD survives the data phase, so counting the
		// range would conflate "erased" with "left the record the second phase still needs".
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
		// One budget shared across nodes in fixed ascending order, so a high-cardinality low-id
		// node starves its successors and the report cannot tell "nothing was due" from "never
		// reached". Pinned so that adding fairness later is a deliberate change.
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
		// `later_of` is perpetual unless BOTH sides declare a span, so a join with a ttl on one
		// side only has no group horizon. Gating the keyspace phase on the data cutoff would
		// leave exactly that shape retaining forever while reporting a ttl.
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
		// The keyspace list is walked under one budget with an early break, so declaration order
		// decides what a truncated sweep reaches: a join names JOIN_LEFT before its ledger
		// keyspaces so the left rows go first and the record of them outlives the sweep.
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
		// The sweep keeps no state between ticks: holding the scan position inside it would make
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
		// Invalidation revokes RAM caches that mirror the store, so handing back a half-drained
		// group would drop a filter that is still correct for the rows it has left.
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
		// A sink row can still name the row-number mapping long after the accumulators behind it
		// are worthless. Taking both at once means the next event on that group mints a second
		// row number for a row that already exists.
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
		// Both sides share a group, so the group-level phases can only offer them one horizon; a
		// join with a 60s left ttl against an hour-long right ttl needs the left rows gone while
		// the right rows are still being probed.
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
		// Without the released ids the join's membership filter keeps claiming keys whose rows
		// the sweep deleted, and every probe pays a store read that can only miss. A half-drained
		// side must NOT be handed back - invalidating early drops a filter that is still correct.
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
		// A side that stayed due after being drained would burn on every pass the group budget
		// that live groups need.
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
		// Identity keeps ageing on the per-group index at the later of the two ttls: a sink row
		// can still name the row-number mapping after one side of the join is gone, and dropping
		// it early would let the next event on the key mint a duplicate row.
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
		// A high-cardinality side must not be dropped in one unbounded delete, and a half-drained
		// side has to stay due, or its remaining rows are stranded until the group's own horizon
		// - hours, for a short left side against a long right ttl.
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
	fn the_identity_phase_only_takes_groups_the_data_phase_already_finished() {
		// The identity scan reads a different index so it can never reach a live group: otherwise
		// a group merely idle enough for its data horizon would lose its mapping at the same
		// moment, collapsing the two horizons into one.
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
		// The store rows go, but the row-number provider still holds (id, key) -> row number in
		// memory. A reborn group gets a fresh id, so the entry is never queried again, yet
		// leaving it grows the cache without bound and can still serve a stale number.
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
		// cutoff; if it kept coming back, every tick would starve live groups behind it.
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
		// Marking a half-erased group reclaimed strands its remaining rows: the data scan never
		// offers it again, and the identity phase deletes the record that addresses them.
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
		// Every reclaimed row is a tombstone write on the single write mutex, so a tick that took
		// every due group at once is a latency incident on the first big node to go idle.
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
	fn the_sink_row_ttl_bounds_identity_from_the_same_watermark_the_operator_answered_against() {
		// Identity belongs to the SINK, not the operator: a mapping must outlive the published row
		// naming it, and that row lives exactly the sink's row ttl. Measuring it off a different
		// clock would order the two phases by comparing milliseconds against commit versions.
		let watermark = DateTime::from_millis(1_000_000);

		assert_eq!(
			identity_cutoff(Some(ms(60_000)), watermark),
			Some(Cutoff(DateTime::from_millis(940_000))),
			"watermark minus the sink row ttl"
		);
	}

	#[test]
	fn a_forever_sink_keeps_identity_entirely() {
		// A sink row with no ttl lives forever, so the mapping it names has to as well; the data
		// phase still bounds the accumulators from the operator's own frontier.
		assert_eq!(identity_cutoff(None, DateTime::from_millis(1_000_000)), None);
	}

	#[test]
	fn both_floors_bind_to_the_owning_flows_checkpoint_and_only_when_data_is_reclaimable() {
		// A flow parked below the cutoff has input it has not applied, so reclaiming above its
		// checkpoint erases state those changes still refer to. The floor must NAME the flow, and
		// a node with no data frontier contributes none or it would look like the blocker.
		let mut report = ReclaimReport::default();
		report.bind(Some(Cutoff(DateTime::from_millis(998_400))), CommitVersion(10));

		assert_eq!(
			report.data_floor,
			Some((Floor::Version(CommitVersion(10)), FloorTerm::OwningFlowCheckpoint))
		);
		assert_eq!(
			report.identity_floor,
			Some((Floor::Version(CommitVersion(10)), FloorTerm::OwningFlowCheckpoint))
		);

		let mut perpetual = ReclaimReport::default();
		perpetual.bind(None, CommitVersion(10));
		assert_eq!(perpetual.data_floor, None, "a node with no frontier holds nothing back");
		assert_eq!(perpetual.identity_floor, None);
	}

	#[test]
	fn the_reported_floor_is_the_lowest_across_every_node_in_the_flow() {
		// A class is only as free as its most constrained node; reporting a later node's healthier
		// floor would hide the one actually holding reclamation back.
		let data = Some(Cutoff(DateTime::from_millis(998_400)));
		let mut report = ReclaimReport::default();
		report.bind(data, CommitVersion(9_000));
		report.bind(data, CommitVersion(400));
		report.bind(data, CommitVersion(7_000));

		assert_eq!(
			report.data_floor,
			Some((Floor::Version(CommitVersion(400)), FloorTerm::OwningFlowCheckpoint))
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

	#[test]
	fn a_sink_resolves_to_the_storage_it_writes_not_the_view_it_presents() {
		// Row settings are recorded against the storage, never the view: returning the view is
		// well-typed but the lookup misses, the flow reads as perpetual, and its rows are never
		// reclaimed. The ids are distinct so returning the wrong half cannot pass by accident.
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

	#[test]
	fn a_node_that_owns_no_storage_resolves_to_nothing() {
		// Only sinks own storage; resolving one here would attribute a row ttl to a node that
		// never writes rows.
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

	fn dag(nodes: &[(u64, FlowNodeType)], edges: &[(u64, u64)]) -> FlowDag {
		// The edges are wired even though the resolver scans nodes rather than walking them, so
		// the fixture keeps the shape of a real flow.
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
		// The mapping has to outlive the row naming it, and the row lives exactly the sink's row
		// ttl. Anything shorter retires the mapping under a live row, and the next event on that
		// key mints a second row over it.
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
		// A sink with no declared row ttl keeps its rows forever, so any duration here would
		// eventually retire a mapping while the row still points at it.
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
		// Subscription flows do reach the sweep, and a subscription owns no storage, so its rows
		// are not ours to age: the resolver must find no sink rather than mistake it for one.
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
