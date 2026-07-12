// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::{
	actors::ringbuffer_reconcile::RingBufferReconcileMessage as Message,
	interface::catalog::{config::ConfigKey, id::RingBufferId, ringbuffer::RingBuffer},
	key::ringbuffer::RingBufferMetadataKey,
};
use reifydb_runtime::actor::{
	context::Context,
	mailbox::ActorRef,
	system::{ActorConfig, ActorSpawner},
	timers::TimerHandle,
	traits::{Actor as ActorTrait, Directive},
};
use reifydb_value::value::datetime::DateTime;
use tracing::{debug, trace, warn};

use super::{ListRingBuffers, ReconcileStats, scanner};
use crate::store::{StandardMultiStore, multi::MultiVersionRangeCursor};

#[derive(Default)]
pub struct ScannerState {
	cursors: HashMap<RingBufferId, MultiVersionRangeCursor>,
}

pub struct ActorState {
	_timer_handle: Option<TimerHandle>,
	scanning: bool,
	scanner: ScannerState,
}

pub struct Actor<P: ListRingBuffers> {
	store: StandardMultiStore,
	provider: P,
}

impl<P: ListRingBuffers> Actor<P> {
	pub fn new(store: StandardMultiStore, provider: P) -> Self {
		Self {
			store,
			provider,
		}
	}

	pub fn spawn(spawner: &ActorSpawner, store: StandardMultiStore, provider: P) -> ActorRef<Message> {
		let actor = Self::new(store, provider);
		spawner.spawn_coordination("ringbuffer-reconcile", actor).actor_ref().clone()
	}

	fn run_scan(&self, state: &mut ActorState) {
		if state.scanning {
			debug!("Ring buffer reconciliation scan already in progress, skipping tick");
			return;
		}
		state.scanning = true;

		let ringbuffers = self.provider.list_ringbuffers();
		let config = self.provider.config();
		let batch_size = config.get_config_uint8(ConfigKey::RingBufferReconcileBatchSize);

		let mut stats = ReconcileStats::default();
		for rb in &ringbuffers {
			self.reconcile_ringbuffer(&mut state.scanner, rb, batch_size, &mut stats);
		}

		if stats.partitions_removed > 0 {
			debug!(
				ringbuffers_scanned = stats.ringbuffers_scanned,
				partitions_checked = stats.partitions_checked,
				partitions_removed = stats.partitions_removed,
				"Ring buffer reconciliation scan completed"
			);
		} else {
			trace!(
				ringbuffers_scanned = stats.ringbuffers_scanned,
				partitions_checked = stats.partitions_checked,
				"Ring buffer reconciliation scan completed (no orphaned metadata found)"
			);
		}

		state.scanning = false;
	}

	fn reconcile_ringbuffer(
		&self,
		scan_state: &mut ScannerState,
		rb: &RingBuffer,
		batch_size: u64,
		stats: &mut ReconcileStats,
	) {
		stats.ringbuffers_scanned += 1;
		let mut cursor = scan_state.cursors.remove(&rb.id).unwrap_or_default();

		let (entries, has_more) =
			match scanner::scan_partition_metadata_batch(&self.store, rb.id, &mut cursor, batch_size) {
				Ok(result) => result,
				Err(e) => {
					warn!(ringbuffer = ?rb.id, error = %e, "Failed to scan ring buffer partition metadata");
					return;
				}
			};

		for entry in &entries {
			stats.partitions_checked += 1;
			self.reconcile_partition(rb.id, entry, stats);
		}

		if has_more {
			scan_state.cursors.insert(rb.id, cursor);
		}
	}

	fn reconcile_partition(
		&self,
		ringbuffer: RingBufferId,
		entry: &scanner::PartitionEntry,
		stats: &mut ReconcileStats,
	) {
		let head_key = scanner::head_row_key(ringbuffer, &entry.partition_values, entry.metadata.head);
		let head_exists = match scanner::head_row_exists(&self.store, &head_key) {
			Ok(exists) => exists,
			Err(e) => {
				warn!(?ringbuffer, error = %e, "Failed to check ring buffer partition head row");
				return;
			}
		};
		if head_exists {
			return;
		}

		let has_any = match scanner::has_any_live_row(&self.store, ringbuffer, &entry.partition_values) {
			Ok(has_any) => has_any,
			Err(e) => {
				warn!(?ringbuffer, error = %e, "Failed to recount ring buffer partition rows");
				return;
			}
		};
		if has_any {
			trace!(
				?ringbuffer,
				"Ring buffer partition head row missing but partition not empty; leaving metadata as-is"
			);
			return;
		}

		let metadata_key = if entry.partition_values.is_empty() {
			RingBufferMetadataKey::encoded(ringbuffer)
		} else {
			RingBufferMetadataKey::encoded_partition(ringbuffer, entry.partition_values.clone())
		};

		match scanner::remove_partition_metadata_key(&self.store, &metadata_key) {
			Ok(()) => {
				stats.partitions_removed += 1;
				debug!(?ringbuffer, "Removed orphaned ring buffer partition metadata");
			}
			Err(e) => {
				warn!(?ringbuffer, error = %e, "Failed to remove orphaned ring buffer partition metadata");
			}
		}
	}
}

impl<P: ListRingBuffers> ActorTrait for Actor<P> {
	type State = ActorState;
	type Message = Message;

	fn init(&self, ctx: &Context<Message>) -> ActorState {
		debug!("Ring buffer reconciliation actor started");
		let config = self.provider.config();
		let scan_interval = config.get_config_duration(ConfigKey::RingBufferReconcileInterval);

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
			Message::Tick(_now) => {
				self.run_scan(state);
			}
			Message::Shutdown => {
				debug!("Ring buffer reconciliation actor shutting down");
				return Directive::Stop;
			}
		}

		Directive::Continue
	}

	fn post_stop(&self) {
		debug!("Ring buffer reconciliation actor stopped");
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(64)
	}
}

pub fn spawn_ringbuffer_reconcile_actor<P: ListRingBuffers>(
	store: StandardMultiStore,
	spawner: ActorSpawner,
	provider: P,
) -> ActorRef<Message> {
	Actor::spawn(&spawner, store, provider)
}

#[cfg(all(test, feature = "sqlite", not(target_arch = "wasm32")))]
mod tests {
	use std::sync::Arc;

	use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
	use reifydb_core::{
		common::CommitVersion,
		delta::Delta,
		interface::{
			catalog::{
				config::{ConfigKey, GetConfig},
				id::{NamespaceId, RingBufferId},
				ringbuffer::{RingBuffer, RingBufferMetadata, encode_ringbuffer_metadata},
				shape::ShapeId,
			},
			store::MultiVersionCommit,
		},
		key::partitioned_row::{PartitionedRowKey, RowLocator},
	};
	use reifydb_value::{
		util::cowvec::CowVec,
		value::{Value, partition::Partition, row_number::RowNumber},
	};

	use super::*;

	#[derive(Clone)]
	struct TestProvider {
		ringbuffers: Vec<RingBuffer>,
	}

	impl ListRingBuffers for TestProvider {
		fn list_ringbuffers(&self) -> Vec<RingBuffer> {
			self.ringbuffers.clone()
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

	fn test_ringbuffer(id: RingBufferId, partitioned: bool) -> RingBuffer {
		RingBuffer {
			id,
			namespace: NamespaceId::SYSTEM,
			name: "test_rb".to_string(),
			columns: vec![],
			capacity: 10,
			primary_key: None,
			partition_by: if partitioned {
				vec!["region".to_string()]
			} else {
				vec![]
			},
			underlying: false,
		}
	}

	fn commit_metadata(store: &StandardMultiStore, key: EncodedKey, metadata: &RingBufferMetadata) {
		MultiVersionCommit::commit(
			store,
			CowVec::new(vec![Delta::Set {
				key,
				row: encode_ringbuffer_metadata(metadata),
			}]),
			CommitVersion(1),
		)
		.unwrap();
	}

	fn commit_row(store: &StandardMultiStore, key: EncodedKey) {
		MultiVersionCommit::commit(
			store,
			CowVec::new(vec![Delta::Set {
				key,
				row: EncodedRow(CowVec::new(vec![0u8; 24])),
			}]),
			CommitVersion(1),
		)
		.unwrap();
	}

	fn run_once<P: ListRingBuffers>(actor: &Actor<P>) {
		let mut state = ActorState {
			_timer_handle: None,
			scanning: false,
			scanner: ScannerState::default(),
		};
		actor.run_scan(&mut state);
	}

	#[test]
	fn removes_orphaned_partitioned_metadata_with_no_live_rows() {
		let (store, _g) = StandardMultiStore::testing_memory_with_persistent_sqlite();
		let rb = test_ringbuffer(RingBufferId(1), true);
		let partition_values = vec![Value::Utf8("us".to_string())];

		commit_metadata(
			&store,
			RingBufferMetadataKey::encoded_partition(rb.id, partition_values.clone()),
			&RingBufferMetadata {
				id: rb.id,
				capacity: rb.capacity,
				count: 3,
				head: 1,
				tail: 4,
			},
		);

		let actor = Actor::new(
			store.clone(),
			TestProvider {
				ringbuffers: vec![rb.clone()],
			},
		);
		run_once(&actor);

		let mut cursor = MultiVersionRangeCursor::new();
		let (entries, _) = scanner::scan_partition_metadata_batch(&store, rb.id, &mut cursor, 100).unwrap();
		assert!(entries.is_empty(), "orphaned partition metadata should have been removed");
	}

	#[test]
	fn leaves_healthy_partition_metadata_untouched() {
		let (store, _g) = StandardMultiStore::testing_memory_with_persistent_sqlite();
		let rb = test_ringbuffer(RingBufferId(2), true);
		let partition_values = vec![Value::Utf8("us".to_string())];
		let partition = Partition::of(&partition_values);

		commit_metadata(
			&store,
			RingBufferMetadataKey::encoded_partition(rb.id, partition_values.clone()),
			&RingBufferMetadata {
				id: rb.id,
				capacity: rb.capacity,
				count: 1,
				head: 1,
				tail: 2,
			},
		);
		commit_row(
			&store,
			PartitionedRowKey::encoded(
				ShapeId::ringbuffer(rb.id),
				partition,
				RowLocator::Row(RowNumber(1)),
			),
		);

		let actor = Actor::new(
			store.clone(),
			TestProvider {
				ringbuffers: vec![rb.clone()],
			},
		);
		run_once(&actor);

		let mut cursor = MultiVersionRangeCursor::new();
		let (entries, _) = scanner::scan_partition_metadata_batch(&store, rb.id, &mut cursor, 100).unwrap();
		assert_eq!(entries.len(), 1, "healthy partition metadata must survive reconciliation");
	}

	#[test]
	fn leaves_drifted_but_nonempty_partition_metadata_untouched() {
		let (store, _g) = StandardMultiStore::testing_memory_with_persistent_sqlite();
		let rb = test_ringbuffer(RingBufferId(3), true);
		let partition_values = vec![Value::Utf8("us".to_string())];
		let partition = Partition::of(&partition_values);

		commit_metadata(
			&store,
			RingBufferMetadataKey::encoded_partition(rb.id, partition_values.clone()),
			&RingBufferMetadata {
				id: rb.id,
				capacity: rb.capacity,
				count: 2,
				head: 1,
				tail: 3,
			},
		);
		commit_row(
			&store,
			PartitionedRowKey::encoded(
				ShapeId::ringbuffer(rb.id),
				partition,
				RowLocator::Row(RowNumber(2)),
			),
		);

		let actor = Actor::new(
			store.clone(),
			TestProvider {
				ringbuffers: vec![rb.clone()],
			},
		);
		run_once(&actor);

		let mut cursor = MultiVersionRangeCursor::new();
		let (entries, _) = scanner::scan_partition_metadata_batch(&store, rb.id, &mut cursor, 100).unwrap();
		assert_eq!(
			entries.len(),
			1,
			"drifted-but-nonempty partition metadata must be left alone, not blindly removed"
		);
	}

	#[test]
	fn removes_orphaned_non_partitioned_metadata_with_no_live_rows() {
		let (store, _g) = StandardMultiStore::testing_memory_with_persistent_sqlite();
		let rb = test_ringbuffer(RingBufferId(4), false);

		commit_metadata(
			&store,
			RingBufferMetadataKey::encoded(rb.id),
			&RingBufferMetadata {
				id: rb.id,
				capacity: rb.capacity,
				count: 2,
				head: 1,
				tail: 3,
			},
		);

		let actor = Actor::new(
			store.clone(),
			TestProvider {
				ringbuffers: vec![rb.clone()],
			},
		);
		run_once(&actor);

		let mut cursor = MultiVersionRangeCursor::new();
		let (entries, _) = scanner::scan_partition_metadata_batch(&store, rb.id, &mut cursor, 100).unwrap();
		assert!(entries.is_empty(), "orphaned non-partitioned metadata should have been removed");
	}
}
