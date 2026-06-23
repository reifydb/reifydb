// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, mem::take};

use reifydb_core::{
	actors::operator_ttl::OperatorTtlMessage as Message,
	common::CommitVersion,
	event::row::OperatorRowsExpiredEvent,
	interface::{
		catalog::{config::ConfigKey, flow::FlowNodeId},
		store::EntryKind,
	},
	key::flow_node_state::FlowNodeStateKey,
	row::{Ttl, TtlCleanupMode},
};
use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::ActorRef,
		system::{ActorConfig, ActorSpawner},
		timers::TimerHandle,
		traits::{Actor as ActorTrait, Directive},
	},
	version_epoch::VersionEpoch,
};
use reifydb_value::{reifydb_assertions, value::datetime::DateTime};
use tracing::{debug, trace, warn};

use super::{ListOperatorSettings, OperatorScanStats, scanner};
use crate::{
	gc::row::scanner::ScanResult,
	store::StandardMultiStore,
	tier::{RangeCursor, commit::buffer::MultiCommitBufferTier, persistent::MultiPersistentTier},
};

#[derive(Default)]
pub struct ScannerState {
	cursors: HashMap<FlowNodeId, RangeCursor>,
}

pub struct ActorState {
	_timer_handle: Option<TimerHandle>,
	scanning: bool,
	scanner: ScannerState,
}

pub struct Actor<P: ListOperatorSettings> {
	store: StandardMultiStore,
	provider: P,
	epoch: VersionEpoch,
}

impl<P: ListOperatorSettings> Actor<P> {
	pub fn new(store: StandardMultiStore, provider: P, epoch: VersionEpoch) -> Self {
		Self {
			store,
			provider,
			epoch,
		}
	}

	pub fn spawn(
		spawner: &ActorSpawner,
		store: StandardMultiStore,
		provider: P,
		epoch: VersionEpoch,
	) -> ActorRef<Message> {
		let actor = Self::new(store, provider, epoch);
		spawner.spawn_background("operator-row", actor).actor_ref().clone()
	}

	fn run_scan(&self, state: &mut ActorState, now: DateTime) {
		if state.scanning {
			debug!("Operator TTL scan already in progress, skipping tick");
			return;
		}

		let buffer = self.store.commit();
		let persistent = self.store.persistent();
		if buffer.is_none() && persistent.is_none() {
			warn!("Operator TTL scan skipped: no storage tier is configured");
			return;
		}

		state.scanning = true;

		let now_nanos = now.to_nanos();
		trace!(now_nanos, "Starting operator TTL scan");

		let (mut stats, persistent_rows_deleted) =
			self.scan_all_operators(&mut state.scanner, buffer, persistent, now_nanos);

		self.run_maintenance(buffer, persistent, &stats);
		self.report_scan(&stats, persistent_rows_deleted);
		self.emit_expired_event(&mut stats);

		state.scanning = false;
	}

	#[inline]
	fn scan_all_operators(
		&self,
		scan_state: &mut ScannerState,
		buffer: Option<&MultiCommitBufferTier>,
		persistent: Option<&MultiPersistentTier>,
		now_nanos: u64,
	) -> (OperatorScanStats, u64) {
		let entries = self.provider.list_operator_settings();
		let config = self.provider.config();
		let mut stats = OperatorScanStats::default();
		let mut persistent_rows_deleted: u64 = 0;

		let batch_size = config.get_config_uint8(ConfigKey::OperatorTtlScanBatchSize) as usize;

		for (node_id, settings) in &entries {
			if let Some(join) = settings.join.as_ref() {
				let left = join.left.as_ref();
				let right = join.right.as_ref();
				if left.is_none() && right.is_none() {
					continue;
				}

				self.scan_join_entry(
					scan_state,
					buffer,
					persistent,
					*node_id,
					left,
					right,
					now_nanos,
					batch_size,
					&mut stats,
					&mut persistent_rows_deleted,
				);
				continue;
			}

			let Some(ttl) = settings.ttl.as_ref() else {
				continue;
			};
			trace!(?node_id, ?ttl, "Evaluating TTL config for operator");
			if ttl.cleanup_mode == TtlCleanupMode::Delete {
				debug!(?node_id, "Skipping operator with TtlCleanupMode::Delete (not supported in V1)");
				stats.operators_skipped += 1;
				continue;
			}

			self.scan_ttl_entry(
				scan_state,
				buffer,
				persistent,
				*node_id,
				ttl,
				now_nanos,
				batch_size,
				&mut stats,
				&mut persistent_rows_deleted,
			);
		}

		(stats, persistent_rows_deleted)
	}

	#[allow(clippy::too_many_arguments)]
	fn scan_join_entry(
		&self,
		scan_state: &mut ScannerState,
		buffer: Option<&MultiCommitBufferTier>,
		persistent: Option<&MultiPersistentTier>,
		node_id: FlowNodeId,
		left: Option<&Ttl>,
		right: Option<&Ttl>,
		now_nanos: u64,
		batch_size: usize,
		stats: &mut OperatorScanStats,
		persistent_rows_deleted: &mut u64,
	) {
		reifydb_assertions! {
			let both_none = left.is_none() && right.is_none();
			assert!(
				!both_none,
				"scan_join_entry was called for node {node_id:?} with neither join side configured; \
				 the caller's left/right guard let an idle join through, which wastes a buffer range \
				 scan and a persistent delete_below_version on a node that can never expire rows"
			);
		}

		let left_cutoff = left
			.and_then(|ttl| DateTime::from_nanos(now_nanos).checked_sub(ttl.duration))
			.and_then(|cutoff| self.epoch.floor_version_at(cutoff.to_nanos()))
			.map(CommitVersion);
		let right_cutoff = right
			.and_then(|ttl| DateTime::from_nanos(now_nanos).checked_sub(ttl.duration))
			.and_then(|cutoff| self.epoch.floor_version_at(cutoff.to_nanos()))
			.map(CommitVersion);

		if let Some(buffer) = buffer {
			let mut cursor = scan_state.cursors.remove(&node_id).unwrap_or_default();
			match scanner::scan_operator_join(
				buffer,
				node_id,
				left_cutoff,
				right_cutoff,
				batch_size,
				&mut cursor,
			) {
				Ok((expired, result)) => {
					stats.operators_scanned += 1;
					if !expired.is_empty() {
						stats.rows_expired += expired.len() as u64;
						for row in &expired {
							*stats.bytes_discovered.entry(row.node_id).or_insert(0) +=
								row.scanned_bytes;
							self.store.invalidate_read_key(&row.key);
						}
						if let Err(e) =
							scanner::drop_expired_operator_keys(buffer, &expired, stats)
						{
							warn!(?node_id, error = %e, "Failed to drop expired join-state keys");
						}
					}
					if let ScanResult::Yielded = result {
						scan_state.cursors.insert(node_id, cursor);
					}
				}
				Err(e) => {
					warn!(?node_id, error = %e, "Failed to scan join operator state for expired rows");
				}
			}
		}

		if let Some(persistent) = persistent {
			for (side_cutoff, side_prefix) in
				[(left_cutoff, scanner::JOIN_LEFT_PREFIX), (right_cutoff, scanner::JOIN_RIGHT_PREFIX)]
			{
				let Some(cutoff) = side_cutoff else {
					continue;
				};
				let prefix = FlowNodeStateKey::encoded(node_id, vec![side_prefix]);
				match persistent.delete_below_version(
					EntryKind::Operator(node_id),
					cutoff,
					Some(prefix.as_ref()),
				) {
					Ok(keys) => {
						*persistent_rows_deleted += keys.len() as u64;
						for key in &keys {
							self.store.invalidate_read_key(key);
						}
					}
					Err(e) => {
						warn!(?node_id, error = %e, "Failed to evict expired persistent join rows");
					}
				}
			}
		}
	}

	#[allow(clippy::too_many_arguments)]
	fn scan_ttl_entry(
		&self,
		scan_state: &mut ScannerState,
		buffer: Option<&MultiCommitBufferTier>,
		persistent: Option<&MultiPersistentTier>,
		node_id: FlowNodeId,
		ttl: &Ttl,
		now_nanos: u64,
		batch_size: usize,
		stats: &mut OperatorScanStats,
		persistent_rows_deleted: &mut u64,
	) {
		reifydb_assertions! {
			let is_delete = ttl.cleanup_mode == TtlCleanupMode::Delete;
			assert!(
				!is_delete,
				"scan_ttl_entry was called for node {node_id:?} with TtlCleanupMode::Delete, which \
				 the caller is supposed to skip and count as operators_skipped; dropping such rows here \
				 would silently apply unsupported Delete semantics instead of the intended Drop"
			);
		}

		let Some(cutoff) = DateTime::from_nanos(now_nanos).checked_sub(ttl.duration) else {
			return;
		};
		let cutoff_version = self.epoch.floor_version_at(cutoff.to_nanos()).map(CommitVersion);

		if let (Some(buffer), Some(cutoff_version)) = (buffer, cutoff_version) {
			let mut cursor = scan_state.cursors.remove(&node_id).unwrap_or_default();

			let scan_result = scanner::scan_operator_expired(
				buffer,
				node_id,
				cutoff_version,
				batch_size,
				&mut cursor,
			);

			match scan_result {
				Ok((expired, result)) => {
					stats.operators_scanned += 1;

					if !expired.is_empty() {
						stats.rows_expired += expired.len() as u64;
						for row in &expired {
							*stats.bytes_discovered.entry(row.node_id).or_insert(0) +=
								row.scanned_bytes;
							self.store.invalidate_read_key(&row.key);
						}

						if let Err(e) =
							scanner::drop_expired_operator_keys(buffer, &expired, stats)
						{
							warn!(?node_id, error = %e, "Failed to drop expired operator-state keys");
						}
					}

					match result {
						ScanResult::Yielded => {
							scan_state.cursors.insert(node_id, cursor);
						}
						ScanResult::Exhausted => {}
					}
				}
				Err(e) => {
					warn!(?node_id, error = %e, "Failed to scan operator state for expired rows");
				}
			}
		}

		if let (Some(persistent), Some(cutoff_version)) = (persistent, cutoff_version) {
			match persistent.delete_below_version(EntryKind::Operator(node_id), cutoff_version, None) {
				Ok(keys) => {
					*persistent_rows_deleted += keys.len() as u64;
					if !keys.is_empty() {
						for key in &keys {
							self.store.invalidate_read_key(key);
						}
						debug!(
							?node_id,
							deleted = keys.len(),
							"Evicted expired operator rows from persistent tier"
						);
					}
				}
				Err(e) => {
					warn!(?node_id, error = %e, "Failed to evict expired persistent operator rows");
				}
			}
		}
	}

	#[inline]
	fn run_maintenance(
		&self,
		buffer: Option<&MultiCommitBufferTier>,
		persistent: Option<&MultiPersistentTier>,
		stats: &OperatorScanStats,
	) {
		if let Some(buffer) = buffer
			&& stats.rows_expired > 0
		{
			buffer.maintenance();
		}

		if buffer.is_none()
			&& let Some(persistent) = persistent
			&& let Err(e) = persistent.maybe_checkpoint()
		{
			warn!(error = %e, "persistent WAL checkpoint failed");
		}
	}

	#[inline]
	fn report_scan(&self, stats: &OperatorScanStats, persistent_rows_deleted: u64) {
		if stats.rows_expired > 0 || persistent_rows_deleted > 0 {
			debug!(
				operators_scanned = stats.operators_scanned,
				operators_skipped = stats.operators_skipped,
				rows_expired = stats.rows_expired,
				versions_dropped = stats.versions_dropped,
				persistent_rows_deleted,
				"Operator TTL scan completed"
			);
		} else {
			debug!(
				operators_scanned = stats.operators_scanned,
				operators_skipped = stats.operators_skipped,
				"Operator TTL scan completed (no expired rows)"
			);
		}
	}

	#[inline]
	fn emit_expired_event(&self, stats: &mut OperatorScanStats) {
		self.store.event_bus.emit(OperatorRowsExpiredEvent::new(
			stats.operators_scanned,
			stats.operators_skipped,
			stats.rows_expired,
			stats.versions_dropped,
			take(&mut stats.bytes_discovered),
			take(&mut stats.bytes_reclaimed),
		));
	}
}

impl<P: ListOperatorSettings> ActorTrait for Actor<P> {
	type State = ActorState;
	type Message = Message;

	fn init(&self, ctx: &Context<Message>) -> ActorState {
		debug!("Operator TTL actor started");
		let config = self.provider.config();
		let scan_interval = config.get_config_duration(ConfigKey::OperatorTtlScanInterval);

		let timer_handle = ctx.schedule_tick(scan_interval, |nanos| Message::Tick(DateTime::from_nanos(nanos)));
		ActorState {
			_timer_handle: Some(timer_handle),
			scanning: false,
			scanner: ScannerState::default(),
		}
	}

	fn handle(&self, state: &mut ActorState, msg: Message, ctx: &Context<Message>) -> Directive {
		if ctx.is_cancelled() {
			return Directive::Stop;
		}

		match msg {
			Message::Tick(now) => {
				self.run_scan(state, now);
			}
			Message::Shutdown => {
				debug!("Operator TTL actor shutting down");
				return Directive::Stop;
			}
		}

		Directive::Continue
	}

	fn post_stop(&self) {
		debug!("Operator TTL actor stopped");
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(64)
	}
}

pub fn spawn_operator_settings_actor<P: ListOperatorSettings>(
	store: StandardMultiStore,
	spawner: ActorSpawner,
	provider: P,
	epoch: VersionEpoch,
) -> ActorRef<Message> {
	Actor::spawn(&spawner, store, provider, epoch)
}

#[cfg(all(test, feature = "sqlite", not(target_arch = "wasm32")))]
mod tests {
	use std::sync::Arc;

	use reifydb_core::{
		common::CommitVersion,
		delta::Delta,
		encoded::row::{EncodedRow, SHAPE_HEADER_SIZE},
		interface::{catalog::config::GetConfig, store::MultiVersionCommit},
		row::OperatorSettings,
	};
	use reifydb_value::{
		util::cowvec::CowVec,
		value::{Value, duration::Duration},
	};

	use super::*;
	use crate::tier::VersionedGetResult;

	#[derive(Clone)]
	struct TestProvider {
		node: FlowNodeId,
		ttl: Ttl,
	}

	impl ListOperatorSettings for TestProvider {
		fn list_operator_settings(&self) -> Vec<(FlowNodeId, OperatorSettings)> {
			vec![(
				self.node,
				OperatorSettings {
					ttl: Some(self.ttl.clone()),
					join: None,
				},
			)]
		}

		fn config(&self) -> Arc<dyn GetConfig> {
			Arc::new(TestConfig)
		}
	}

	struct TestConfig;

	impl GetConfig for TestConfig {
		fn get_config(&self, key: ConfigKey) -> Value {
			key.default_value()
		}

		fn get_config_at(&self, key: ConfigKey, _version: CommitVersion) -> Value {
			key.default_value()
		}
	}

	fn row_with_created(payload: &[u8], created_at: u64) -> CowVec<u8> {
		let mut buf = vec![0u8; SHAPE_HEADER_SIZE + payload.len()];
		buf[8..16].copy_from_slice(&created_at.to_le_bytes());
		buf[16..24].copy_from_slice(&created_at.to_le_bytes());
		buf[SHAPE_HEADER_SIZE..].copy_from_slice(payload);
		CowVec::new(buf)
	}

	#[test]
	fn operator_ttl_gc_invalidates_read_cache_for_dropped_keys() {
		let (store, _g) = StandardMultiStore::testing_memory_with_persistent_sqlite();
		let read = store.read.clone().expect("read tier configured");

		let node = FlowNodeId(1);
		let opkey = FlowNodeStateKey::encoded(node, vec![1u8]);

		MultiVersionCommit::commit(
			&store,
			CowVec::new(vec![Delta::Set {
				key: opkey.clone(),
				row: EncodedRow(row_with_created(b"state", 1)),
			}]),
			CommitVersion(1),
		)
		.unwrap();

		assert!(
			matches!(read.get(&opkey, CommitVersion(1)), VersionedGetResult::Value { .. }),
			"write-through must have cached the operator state before GC, otherwise this test cannot \
			 prove the GC clears a stale entry"
		);

		let ttl = Ttl {
			duration: Duration::from_nanoseconds(100).unwrap(),
			cleanup_mode: TtlCleanupMode::Drop,
		};

		let epoch = VersionEpoch::new();
		epoch.record(1, 1);
		let actor = Actor::new(
			store.clone(),
			TestProvider {
				node,
				ttl,
			},
			epoch,
		);
		let mut state = ActorState {
			_timer_handle: None,
			scanning: false,
			scanner: ScannerState::default(),
		};
		actor.run_scan(&mut state, DateTime::from_nanos(1_000));

		assert!(
			matches!(read.get(&opkey, CommitVersion(1)), VersionedGetResult::NotFound),
			"operator TTL GC must invalidate the read cache for reclaimed keys"
		);
	}
}
