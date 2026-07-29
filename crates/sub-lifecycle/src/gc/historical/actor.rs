// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, mem::take, sync::Arc};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	actors::historical_gc::HistoricalGcMessage as Message,
	common::CommitVersion,
	event::row::HistoricalGcSweepEvent,
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
use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::ActorRef,
		system::{ActorConfig, ActorSpawner},
		timers::TimerHandle,
		traits::{Actor as ActorTrait, Directive},
	},
	context::clock::Clock,
};
use reifydb_store_multi::{
	store::StandardMultiStore,
	tier::{HistoricalCursor, TierStorage, commit::buffer::MultiCommitBufferTier},
};
use reifydb_value::{
	Result,
	value::{datetime::DateTime, duration::Duration},
};
use tracing::{debug, instrument, trace, warn};

use crate::plane::RetentionPlane;

struct SweepProgress {
	cutoff: CommitVersion,
	binding: FloorTerm,
	remaining: Vec<EntryKind>,
	stats: GcMetrics,
}

pub struct ActorState {
	_timer_handle: Option<TimerHandle>,
	in_progress: Option<SweepProgress>,
	cursors: HashMap<EntryKind, HistoricalCursor>,
}

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

	pub fn spawn(
		spawner: &ActorSpawner,
		store: StandardMultiStore,
		plane: RetentionPlane,
		clock: Clock,
		config: Arc<dyn GetConfig>,
	) -> ActorRef<Message> {
		let actor = Self::new(store, plane, clock, config);
		spawner.spawn_coordination("historical-historical", actor).actor_ref().clone()
	}

	#[instrument(name = "lifecycle::gc::historical::sweep_start", level = "debug", skip_all)]
	fn start_sweep(&self, state: &mut ActorState, ctx: &Context<Message>) {
		if state.in_progress.is_some() {
			trace!("Historical GC sweep already in progress, skipping tick");
			return;
		}
		let buffer = self.store.commit();

		let now = self.clock.now();
		let floor = self.plane.cutoff_with_binding(RetentionClass::BufferHistoricalGc, now, None);
		let Some((cutoff, binding)) =
			floor.and_then(|(floor, term)| floor.version().map(|version| (version, term)))
				.filter(|(version, _)| version.0 != 0)
		else {
			self.plane.record_reclamation(RetentionClass::BufferHistoricalGc, floor, 0, 0);
			trace!("Historical GC sweep skipped: no floor established yet");
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

		state.in_progress = Some(SweepProgress {
			cutoff,
			binding,
			remaining: entry_kinds,
			stats: GcMetrics::default(),
		});

		let _ = ctx.self_ref().send(Message::ContinueSweep);
	}

	#[instrument(name = "lifecycle::gc::historical::sweep_step", level = "trace", skip_all)]
	fn step_sweep(&self, state: &mut ActorState, ctx: &Context<Message>) {
		let buffer = self.store.commit();

		let progress = match state.in_progress.as_mut() {
			Some(p) => p,
			None => return,
		};

		let cutoff = progress.cutoff;
		let binding = progress.binding;
		let batch_size = self.batch_size();

		let Some(entry_kind) = progress.remaining.pop() else {
			let stats = take(&mut progress.stats);
			state.in_progress = None;
			let backlog = pending(&state.cursors);
			self.finish_sweep(buffer, cutoff, binding, backlog, &stats);
			return;
		};

		let cursor = state.cursors.entry(entry_kind).or_default();
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

		if let Some(progress) = state.in_progress.as_mut() {
			progress.stats.objects_scanned += 1;
			progress.stats.versions_dropped += dropped;
		}

		let _ = ctx.self_ref().send(Message::ContinueSweep);
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

		self.store.event_bus().emit(HistoricalGcSweepEvent::new(
			cutoff,
			stats.objects_scanned,
			stats.versions_dropped,
		));
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

		let count = entries.len() as u64;
		let mut batches: HashMap<EntryKind, Vec<(EncodedKey, CommitVersion)>> = HashMap::new();
		batches.insert(entry_kind, entries);
		buffer.compact(batches)?;
		Ok(count)
	}

	#[instrument(name = "lifecycle::gc::historical::sweep", level = "debug", skip_all)]
	fn run_sweep(&self, cursors: &mut HashMap<EntryKind, HistoricalCursor>) {
		let buffer = self.store.commit();
		let now = self.clock.now();
		let floor = self.plane.cutoff_with_binding(RetentionClass::BufferHistoricalGc, now, None);
		let Some((cutoff, binding)) =
			floor.and_then(|(floor, term)| floor.version().map(|version| (version, term)))
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

impl ActorTrait for Actor {
	type State = ActorState;
	type Message = Message;

	fn init(&self, ctx: &Context<Message>) -> ActorState {
		debug!("Historical GC actor started");
		let scan_interval = self.config.get_config_duration(ConfigKey::HistoricalGcInterval);

		let timer_handle = ctx.schedule_tick(scan_interval, |nanos| Message::Tick(DateTime::from_nanos(nanos)));
		ActorState {
			_timer_handle: Some(timer_handle),
			in_progress: None,
			cursors: HashMap::new(),
		}
	}

	fn handle(&self, state: &mut ActorState, msg: Message, ctx: &Context<Message>) -> Directive {
		if ctx.is_cancelled() {
			return Directive::Stop;
		}

		match msg {
			Message::Tick(_) => {
				self.start_sweep(state, ctx);
			}
			Message::ContinueSweep => {
				self.step_sweep(state, ctx);
			}
			Message::Shutdown => {
				debug!("Historical GC actor shutting down");
				return Directive::Stop;
			}
		}

		Directive::Yield
	}

	fn post_stop(&self) {
		debug!("Historical GC actor stopped");
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(64)
	}
}

pub fn spawn_historical_gc_actor(
	store: StandardMultiStore,
	spawner: ActorSpawner,
	plane: RetentionPlane,
	clock: Clock,
	config: Arc<dyn GetConfig>,
) -> ActorRef<Message> {
	Actor::spawn(&spawner, store, plane, clock, config)
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
