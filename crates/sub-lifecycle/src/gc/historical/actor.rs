// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	common::CommitVersion,
	event::metric::{MultiEviction, MultiSweptEvent},
	interface::{
		catalog::config::{ConfigKey, GetConfig},
		store::EntryKind,
	},
	lifecycle::{
		class::{Floor, FloorTerm, RetentionClass},
		metrics::GcMetrics,
		progress::Progress,
		task::LifecycleTask,
	},
};
use reifydb_runtime::context::clock::Clock;
use reifydb_store_multi::{
	store::StandardMultiStore,
	tier::{HistoricalCursor, TierStorage, commit::buffer::MultiCommitBufferTier},
};
use reifydb_value::{Result, value::duration::Duration};
use tracing::{debug, instrument, trace, warn};

use crate::plane::RetentionPlane;

pub struct Actor {
	store: StandardMultiStore,
	plane: RetentionPlane,
	clock: Clock,
	config: Arc<dyn GetConfig>,
}

impl Actor {
	pub fn new(store: StandardMultiStore, plane: RetentionPlane, clock: Clock, config: Arc<dyn GetConfig>) -> Self {
		Self {
			store,
			plane,
			clock,
			config,
		}
	}

	#[inline]
	fn batch_size(&self) -> usize {
		self.config.get_config_uint8(ConfigKey::HistoricalGcBatchSize) as usize
	}

	#[inline]
	fn finish_sweep(
		&self,
		buffer: &MultiCommitBufferTier,
		cutoff: CommitVersion,
		binding: FloorTerm,
		backlog: u64,
		stats: &GcMetrics,
	) {
		self.plane.record_reclamation(
			RetentionClass::BufferHistoricalGc,
			Some((Floor::Version(cutoff), binding)),
			stats.versions_dropped,
			backlog,
		);

		if stats.versions_dropped > 0 {
			buffer.maintenance();
			debug!(
				cutoff = cutoff.0,
				objects_scanned = stats.objects_scanned,
				versions_dropped = stats.versions_dropped,
				"Historical GC sweep completed"
			);
		} else {
			trace!(cutoff = cutoff.0, "Historical GC sweep completed (no drops)");
		}
	}

	#[instrument(name = "lifecycle::gc::historical::sweep_shape", level = "trace", skip_all, fields(?entry_kind, cutoff = cutoff.0, dropped))]
	fn sweep_shape(
		&self,
		buffer: &MultiCommitBufferTier,
		entry_kind: EntryKind,
		cutoff: CommitVersion,
		batch_size: usize,
		cursor: &mut HistoricalCursor,
	) -> Result<u64> {
		let entries = buffer.scan_historical_below(entry_kind, cutoff, cursor, batch_size)?;
		if entries.is_empty() {
			return Ok(0);
		}

		let mut batches: HashMap<EntryKind, Vec<(EncodedKey, CommitVersion)>> = HashMap::new();
		batches.insert(entry_kind, entries);
		let removed = buffer.compact(batches)?;
		if removed.is_empty() {
			return Ok(0);
		}

		let count = removed.len() as u64;
		let evictions = removed
			.into_iter()
			.map(|entry| MultiEviction {
				key: entry.key,
				value_bytes: entry.value_bytes,
				current: entry.current,
			})
			.collect();
		self.store.event_bus().emit(MultiSweptEvent::new(evictions, vec![], cutoff));
		Ok(count)
	}

	#[instrument(name = "lifecycle::gc::historical::sweep", level = "debug", skip_all)]
	fn run_sweep(&self, cursors: &mut HashMap<EntryKind, HistoricalCursor>) {
		let buffer = self.store.commit();
		let now = self.clock.now();
		let floor = self.plane.cutoff_with_binding(RetentionClass::BufferHistoricalGc, now, None);
		let Some((cutoff, binding)) = floor
			.and_then(|(floor, term)| floor.version().map(|version| (version, term)))
			.filter(|(version, _)| version.0 != 0)
		else {
			self.plane.record_reclamation(RetentionClass::BufferHistoricalGc, floor, 0, 0);
			return;
		};
		let entry_kinds = match buffer.list_all_entry_kinds() {
			Ok(v) => v,
			Err(e) => {
				warn!(error = %e, "Historical GC sweep failed: list_all_entry_kinds");
				return;
			}
		};
		if entry_kinds.is_empty() {
			return;
		}
		let batch_size = self.batch_size();
		let mut stats = GcMetrics::default();
		for entry_kind in entry_kinds {
			let cursor = cursors.entry(entry_kind).or_default();
			if cursor.exhausted {
				*cursor = HistoricalCursor::default();
			}
			let dropped = match self.sweep_shape(buffer, entry_kind, cutoff, batch_size, cursor) {
				Ok(n) => n,
				Err(e) => {
					warn!(?entry_kind, error = %e, "Historical GC sweep failed for shape");
					0
				}
			};
			stats.objects_scanned += 1;
			stats.versions_dropped += dropped;
		}
		self.finish_sweep(buffer, cutoff, binding, pending(cursors), &stats);
	}
}

fn pending(cursors: &HashMap<EntryKind, HistoricalCursor>) -> u64 {
	cursors.values().filter(|cursor| !cursor.exhausted).count() as u64
}

pub struct HistoricalGcTask {
	actor: Actor,
	cursors: HashMap<EntryKind, HistoricalCursor>,
}

impl HistoricalGcTask {
	pub fn new(store: StandardMultiStore, plane: RetentionPlane, clock: Clock, config: Arc<dyn GetConfig>) -> Self {
		Self {
			actor: Actor::new(store, plane, clock, config),
			cursors: HashMap::new(),
		}
	}
}

impl LifecycleTask for HistoricalGcTask {
	fn name(&self) -> &'static str {
		"historical-gc"
	}

	fn interval(&self) -> Duration {
		self.actor.config.get_config_duration(ConfigKey::HistoricalGcInterval)
	}

	fn classes(&self) -> &'static [RetentionClass] {
		&[RetentionClass::BufferHistoricalGc]
	}

	#[instrument(name = "lifecycle::gc::historical::slice", level = "debug", skip_all)]
	fn run_slice(&mut self) -> Progress {
		self.actor.run_sweep(&mut self.cursors);
		Progress::Exhausted
	}
}
