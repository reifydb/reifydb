// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		config::{ConfigKey, GetConfig},
		flow::{FlowId, FlowNodeId},
		storage::StorageId,
	},
	key::operator_state::{GroupId, GroupSet},
	lifecycle::class::{FloorTerm, RetentionClass},
	state::horizon::{Cutoff, Horizon},
};
use reifydb_flow::transaction::FlowTransaction;
use reifydb_rql::flow::{flow::FlowDag, node::FlowNodeType};
use reifydb_runtime::version_epoch::EpochSeconds;
use reifydb_value::{
	Result,
	value::{datetime::DateTime, duration::Duration},
};
use tracing::{instrument, warn};

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
	pub rows: usize,
	pub backlog: usize,
	pub perpetual_nodes: usize,
	pub data_floor: Option<(CommitVersion, FloorTerm)>,
	pub identity_floor: Option<(CommitVersion, FloorTerm)>,
}

impl ReclaimReport {
	fn bind(&mut self, cutoffs: &Cutoffs) {
		self.data_floor = lowest(self.data_floor, Some(cutoffs.data_floor));
		self.identity_floor = lowest(self.identity_floor, cutoffs.identity_floor);
	}
}

fn lowest(
	current: Option<(CommitVersion, FloorTerm)>,
	candidate: Option<(CommitVersion, FloorTerm)>,
) -> Option<(CommitVersion, FloorTerm)> {
	match (current, candidate) {
		(Some(current), Some(candidate)) => Some(if current.0 <= candidate.0 {
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
	data_floor: (CommitVersion, FloorTerm),
	identity_floor: Option<(CommitVersion, FloorTerm)>,
}

impl FlowEngineInner {
	#[instrument(name = "lifecycle::operator::group::scan", level = "debug", skip(self, txn), fields(flow_id = ?flow_id))]
	pub fn reclaim_flow(
		&self,
		txn: &mut FlowTransaction,
		flow_id: FlowId,
		now: DateTime,
		checkpoint: CommitVersion,
		budget: ReclaimBudget,
	) -> Result<ReclaimReport> {
		let mut report = ReclaimReport::default();
		let Some(flow) = self.flows.get(&flow_id) else {
			return Ok(report);
		};
		let identity_span = self.identity_span(flow);
		let mut remaining = budget;

		for node_id in flow.get_node_ids() {
			if remaining.exhausted() {
				break;
			}
			let Some(node) = flow.get_node(&node_id) else {
				continue;
			};
			let horizon = self.node_horizon(node);
			if !horizon.reclaims() {
				report.perpetual_nodes += 1;
				continue;
			}
			let Some(operator) = self.operators.get(&node_id) else {
				continue;
			};
			if !operator.capabilities().contains(&OperatorCapability::Reclaim) {
				report.perpetual_nodes += 1;
				continue;
			}
			let Some(cutoffs) = self.cutoffs(txn, node_id, horizon, identity_span, now, checkpoint)? else {
				continue;
			};
			report.bind(&cutoffs);

			if let Some(cutoff) = cutoffs.identity {
				reclaim_identity(txn, node_id, cutoff, &mut remaining, &mut report)?;
			}
			let released = reclaim_data(txn, node_id, cutoffs.data, &mut remaining, &mut report)?;
			if !released.is_empty() {
				operator.invalidate_groups(&GroupSet::new(released));
			}
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

	fn cutoffs(
		&self,
		txn: &mut FlowTransaction,
		node: FlowNodeId,
		horizon: Horizon,
		identity_span: Option<Duration>,
		now: DateTime,
		checkpoint: CommitVersion,
	) -> Result<Option<Cutoffs>> {
		let buckets = self.substrate.group.buckets(node);
		Ok(match horizon {
			Horizon::Seal {
				..
			} => {
				let (Some(slack), Some(position)) =
					(buckets.event_grid(), txn.node_position(node)?.event())
				else {
					return Ok(None);
				};
				seal_cutoffs(horizon, position, identity_span, slack.width(), checkpoint)
			}
			Horizon::Idle {
				..
			} => {
				let Some(slack) = buckets.version_grid() else {
					return Ok(None);
				};
				idle_cutoffs(
					horizon.idle_span().and_then(|span| self.expiry_version(now, span)),
					identity_span.and_then(|span| self.expiry_version(now, span)),
					slack.width(),
					checkpoint,
				)
			}
			Horizon::Perpetual => None,
		})
	}

	fn expiry_version(&self, now: DateTime, span: Duration) -> Option<u64> {
		let expires_before = now.checked_sub(span)?;
		self.runtime_context.version_epoch.floor_version_at(EpochSeconds::from_datetime(expires_before))
	}

	fn identity_span(&self, flow: &FlowDag) -> Option<Duration> {
		let mut longest: Option<Duration> = None;
		for storage in flow
			.get_node_ids()
			.filter_map(|id| flow.get_node(&id))
			.filter_map(|node| sink_storage(&node.ty))
		{
			let Some(settings) = self.catalog.find_row_settings_latest(storage) else {
				warn!(
					?storage,
					"sink has no row settings; treating it as declaring no row TTL for this flow"
				);
				continue;
			};
			let Some(ttl) = settings.ttl else {
				continue;
			};
			longest = Some(match longest {
				Some(current) if current >= ttl.duration => current,
				_ => ttl.duration,
			});
		}
		longest
	}
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
	position: DateTime,
	identity_span: Option<Duration>,
	slack: Duration,
	checkpoint: CommitVersion,
) -> Option<Cutoffs> {
	horizon.seal_cutoff(position).map(|data| Cutoffs {
		data: Cutoff::Event(data),
		identity: identity_span.map(|span| Cutoff::Event(position.saturating_sub(span).saturating_sub(slack))),
		data_floor: (checkpoint, FloorTerm::OwningFlowCheckpoint),
		identity_floor: Some((checkpoint, FloorTerm::OwningFlowCheckpoint)),
	})
}

fn idle_cutoffs(
	expiry: Option<u64>,
	identity_expiry: Option<u64>,
	slack: u64,
	checkpoint: CommitVersion,
) -> Option<Cutoffs> {
	expiry.map(|expiry| {
		let identity = identity_expiry.map(|version| version.saturating_sub(slack));
		Cutoffs {
			data: Cutoff::Version(expiry.min(checkpoint.0)),
			identity: identity.map(|version| Cutoff::Version(version.min(checkpoint.0))),
			data_floor: floor_of(expiry, checkpoint, FloorTerm::OperatorExpiry),
			identity_floor: identity.map(|version| floor_of(version, checkpoint, FloorTerm::RowExpiry)),
		}
	})
}

fn floor_of(expiry: u64, checkpoint: CommitVersion, declared: FloorTerm) -> (CommitVersion, FloorTerm) {
	if checkpoint.0 <= expiry {
		(checkpoint, FloorTerm::OwningFlowCheckpoint)
	} else {
		(CommitVersion(expiry), declared)
	}
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
	use reifydb_core::key::operator_state::{Keyspace, OperatorStateKey, group_inner_range};
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
		txn.group_interner().set_horizon(NODE, Horizon::seal(ms(1_600)));
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

	#[test]
	fn the_data_phase_erases_state_and_leaves_identity_for_the_second_phase() {
		// The whole point of splitting the phases: a sink row can still name the row-number mapping long
		// after the accumulators behind it are worthless. Taking both at once is landmine L2 - the next
		// event on that group mints a second row number for a row that already exists.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let id = seed(&mut txn, "idle", 50);
		let mut remaining = budget(10, 100);
		let mut report = ReclaimReport::default();

		let released = reclaim_data(
			&mut txn,
			NODE,
			Cutoff::Event(DateTime::from_millis(1_000)),
			&mut remaining,
			&mut report,
		)
		.unwrap();

		assert_eq!(released, vec![id], "the caller needs the group ids to drop their cached rows");
		assert_eq!(report.data_groups, 1);
		assert_eq!(report.rows, 2, "both accumulator rows, and nothing from the identity keyspaces");
		assert_eq!(rows(&mut txn, id), 2, "the mapping and the group record must survive the data phase");
	}

	#[test]
	fn the_identity_phase_only_takes_groups_the_data_phase_already_finished() {
		// The identity scan reads a different index precisely so it can never reach a live group. If it
		// could, a group merely idle enough for its data horizon would lose its mapping at the same
		// moment - collapsing the two horizons into one and reopening L2.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let id = seed(&mut txn, "idle", 50);
		let mut remaining = budget(10, 100);
		let mut report = ReclaimReport::default();

		reclaim_identity(
			&mut txn,
			NODE,
			Cutoff::Event(DateTime::from_millis(1_000)),
			&mut remaining,
			&mut report,
		)
		.unwrap();
		assert_eq!(report.identity_groups, 0, "a group that still holds data is not an identity candidate");
		assert_eq!(rows(&mut txn, id), 4);

		reclaim_data(&mut txn, NODE, Cutoff::Event(DateTime::from_millis(1_000)), &mut remaining, &mut report)
			.unwrap();
		reclaim_identity(
			&mut txn,
			NODE,
			Cutoff::Event(DateTime::from_millis(1_000)),
			&mut remaining,
			&mut report,
		)
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
		// groups reclaim - and a query on the reclaimed id must never serve the stale number (L5).
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

		reclaim_data(&mut txn, NODE, Cutoff::Event(DateTime::from_millis(1_000)), &mut remaining, &mut report)
			.unwrap();
		reclaim_identity(
			&mut txn,
			NODE,
			Cutoff::Event(DateTime::from_millis(1_000)),
			&mut remaining,
			&mut report,
		)
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
		reclaim_data(&mut txn, NODE, Cutoff::Event(DateTime::from_millis(1_000)), &mut remaining, &mut report)
			.unwrap();

		let again = reclaim_data(
			&mut txn,
			NODE,
			Cutoff::Event(DateTime::from_millis(1_000)),
			&mut remaining,
			&mut report,
		)
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

		let released = reclaim_data(
			&mut txn,
			NODE,
			Cutoff::Event(DateTime::from_millis(1_000)),
			&mut remaining,
			&mut report,
		)
		.unwrap();

		assert!(released.is_empty(), "a group that is not drained must not be handed to the operator");
		assert_eq!(report.data_groups, 0);
		assert_eq!(report.backlog, 1, "the caller must learn there is work left");
		assert_eq!(rows(&mut txn, id), 3, "one data row went; the other and the mapping remain");

		let mut remaining = budget(10, 100);
		let released = reclaim_data(
			&mut txn,
			NODE,
			Cutoff::Event(DateTime::from_millis(1_000)),
			&mut remaining,
			&mut report,
		)
		.unwrap();

		assert_eq!(released, vec![id], "the next tick resumes the same group and finishes it");
		assert_eq!(report.data_groups, 1);
	}

	#[test]
	fn the_group_budget_bounds_how_many_groups_one_tick_touches() {
		// Landmine L10: every reclaimed row is a tombstone write on the single write mutex, so a tick
		// that took every due group at once would be a latency incident on the first high-cardinality
		// node to go idle.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		for i in 0..5 {
			seed(&mut txn, &format!("g{i}"), 50);
		}
		let mut remaining = budget(2, 100);
		let mut report = ReclaimReport::default();

		let released = reclaim_data(
			&mut txn,
			NODE,
			Cutoff::Event(DateTime::from_millis(1_000)),
			&mut remaining,
			&mut report,
		)
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
		let horizon = Horizon::seal(ms(1_600));
		let cutoffs = seal_cutoffs(
			horizon,
			DateTime::from_millis(1_000_000),
			Some(ms(60_000)),
			ms(WIDTH as i64),
			CommitVersion(7),
		)
		.unwrap();

		assert_eq!(
			cutoffs.data,
			Cutoff::Event(DateTime::from_millis(998_400)),
			"watermark minus the seal span"
		);
		assert_eq!(
			cutoffs.identity,
			Some(Cutoff::Event(DateTime::from_millis(1_000_000 - 60_000 - WIDTH))),
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
			Horizon::seal(ms(1_600)),
			DateTime::from_millis(1_000_000),
			None,
			ms(WIDTH as i64),
			CommitVersion(7),
		)
		.unwrap();

		assert_eq!(cutoffs.data, Cutoff::Event(DateTime::from_millis(998_400)));
		assert_eq!(cutoffs.identity, None);
	}

	#[test]
	fn an_idle_node_resolves_both_phases_into_commit_versions() {
		// Version-anchored expiry is what makes the idle rule replay-safe (L3). Both cutoffs come from
		// the epoch, and identity trails data by the longer sink horizon plus a bucket of slack.
		let cutoffs = idle_cutoffs(Some(9_000), Some(5_000), WIDTH, CommitVersion(u64::MAX)).unwrap();

		assert_eq!(cutoffs.data, Cutoff::Version(9_000));
		assert_eq!(cutoffs.identity, Some(Cutoff::Version(5_000 - WIDTH)));
		assert_eq!(cutoffs.data_floor, (CommitVersion(9_000), FloorTerm::OperatorExpiry));
		assert_eq!(cutoffs.identity_floor, Some((CommitVersion(5_000 - WIDTH), FloorTerm::RowExpiry)));
	}

	#[test]
	fn a_lagging_flow_holds_both_idle_cutoffs_down_and_is_named_as_the_reason() {
		// A flow parked below the expiry cutoff has input it has not applied yet; reclaiming above its
		// checkpoint would erase state its own unprocessed changes still refer to. The binding has to
		// name the flow too, or a stalled node is indistinguishable from an idle one in the report.
		let cutoffs = idle_cutoffs(Some(9_000), Some(5_000), WIDTH, CommitVersion(10)).unwrap();

		assert_eq!(
			cutoffs.data,
			Cutoff::Version(10),
			"the flow, not the declared horizon, is the binding term"
		);
		assert_eq!(cutoffs.identity, Some(Cutoff::Version(10)));
		assert_eq!(cutoffs.data_floor, (CommitVersion(10), FloorTerm::OwningFlowCheckpoint));
		assert_eq!(cutoffs.identity_floor, Some((CommitVersion(10), FloorTerm::OwningFlowCheckpoint)));
	}

	#[test]
	fn an_epoch_that_cannot_place_the_horizon_reclaims_nothing() {
		// Early in a process's life the epoch has no sample old enough to answer. Treating that as
		// version zero would be harmless, but treating it as "no floor" would let the class reclaim from
		// the present. The only safe answer is to skip the node this tick.
		assert_eq!(idle_cutoffs(None, Some(5_000), WIDTH, CommitVersion(u64::MAX)), None);
	}

	#[test]
	fn a_perpetual_node_produces_no_cutoff_in_either_domain() {
		// Nothing derivable means nothing reclaimable. A cutoff of zero here would be equally safe, but
		// it would still cost a scan per tick per node forever.
		assert_eq!(
			seal_cutoffs(
				Horizon::Perpetual,
				DateTime::from_millis(1_000_000),
				Some(ms(60_000)),
				ms(WIDTH as i64),
				CommitVersion(7)
			),
			None
		);
		assert_eq!(idle_cutoffs(None, None, WIDTH, CommitVersion(7)), None);
	}

	#[test]
	fn the_reported_floor_is_the_lowest_across_every_node_in_the_flow() {
		// The plane reports one floor per class, and a class is only as free as its most constrained
		// node. Reporting a later node's healthier floor would hide the one node actually holding
		// reclamation back.
		let mut report = ReclaimReport::default();
		report.bind(&idle_cutoffs(Some(9_000), None, WIDTH, CommitVersion(u64::MAX)).unwrap());
		report.bind(&idle_cutoffs(Some(400), None, WIDTH, CommitVersion(u64::MAX)).unwrap());
		report.bind(&idle_cutoffs(Some(7_000), None, WIDTH, CommitVersion(u64::MAX)).unwrap());

		assert_eq!(report.data_floor, Some((CommitVersion(400), FloorTerm::OperatorExpiry)));
	}

	#[test]
	fn the_slack_is_the_width_the_interner_actually_stamps_with() {
		// The slack exists because a group's recorded last-activity is only accurate to one bucket, so it
		// has to be the width the stamping side really used. Deriving it independently from the horizon
		// would silently diverge whenever the two disagreed, and an identity cutoff that is one bucket
		// too high retires a mapping while the sink row naming it is still inside its own ttl.
		let horizon = Horizon::seal(ms(1_600));

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
				ms(WIDTH as i64),
				CommitVersion(7)
			)
			.unwrap()
			.identity,
			Some(Cutoff::Event(DateTime::from_millis(939_900)))
		);
		assert_eq!(
			seal_cutoffs(
				horizon,
				DateTime::from_millis(1_000_000),
				Some(ms(60_000)),
				ms(4_096),
				CommitVersion(7)
			)
			.unwrap()
			.identity,
			Some(Cutoff::Event(DateTime::from_millis(935_904))),
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
