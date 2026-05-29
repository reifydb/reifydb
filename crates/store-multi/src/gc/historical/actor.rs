// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, mem::take, sync::Arc};

use reifydb_core::{
	actors::historical_gc::HistoricalGcMessage as Message,
	common::CommitVersion,
	encoded::key::EncodedKey,
	event::row::HistoricalGcSweepEvent,
	interface::{
		catalog::config::{ConfigKey, GetConfig},
		store::EntryKind,
	},
};
use reifydb_runtime::actor::{
	context::Context,
	mailbox::ActorRef,
	system::{ActorConfig, ActorSpawner},
	timers::TimerHandle,
	traits::{Actor as ActorTrait, Directive},
};
use reifydb_value::{Result, value::datetime::DateTime};
use tracing::{debug, trace, warn};

use super::{GcStats, QueryWatermark};
use crate::{
	store::StandardMultiStore,
	tier::{HistoricalCursor, TierStorage, commit::buffer::MultiCommitBufferTier},
};

struct SweepProgress {
	cutoff: CommitVersion,
	remaining: Vec<EntryKind>,
	stats: GcStats,
}

pub struct ActorState {
	_timer_handle: Option<TimerHandle>,
	in_progress: Option<SweepProgress>,
	cursors: HashMap<EntryKind, HistoricalCursor>,
}

pub struct Actor<W: QueryWatermark> {
	store: StandardMultiStore,
	watermark: W,
	config: Arc<dyn GetConfig>,
}

impl<W: QueryWatermark> Actor<W> {
	pub fn new(store: StandardMultiStore, watermark: W, config: Arc<dyn GetConfig>) -> Self {
		Self {
			store,
			watermark,
			config,
		}
	}

	pub fn spawn(
		spawner: &ActorSpawner,
		store: StandardMultiStore,
		watermark: W,
		config: Arc<dyn GetConfig>,
	) -> ActorRef<Message> {
		let actor = Self::new(store, watermark, config);
		spawner.spawn_background("historical-historical", actor).actor_ref().clone()
	}

	fn start_sweep(&self, state: &mut ActorState, ctx: &Context<Message>) {
		if state.in_progress.is_some() {
			trace!("Historical GC sweep already in progress, skipping tick");
			return;
		}
		let Some(buffer) = self.store.commit() else {
			warn!("Historical GC sweep skipped: buffer tier is not configured");
			return;
		};

		let cutoff = self.watermark.effective_gc_cutoff();
		if cutoff.0 == 0 {
			trace!("Historical GC sweep skipped: watermark is zero");
			return;
		}

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
			remaining: entry_kinds,
			stats: GcStats::default(),
		});

		let _ = ctx.self_ref().send(Message::ContinueSweep);
	}

	fn step_sweep(&self, state: &mut ActorState, ctx: &Context<Message>) {
		let Some(buffer) = self.store.commit() else {
			state.in_progress = None;
			return;
		};

		let progress = match state.in_progress.as_mut() {
			Some(p) => p,
			None => return,
		};

		let cutoff = progress.cutoff;
		let batch_size = self.batch_size();

		let Some(entry_kind) = progress.remaining.pop() else {
			let stats = take(&mut progress.stats);
			state.in_progress = None;
			self.finish_sweep(buffer, cutoff, &stats);
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
			progress.stats.shapes_scanned += 1;
			progress.stats.versions_dropped += dropped;
		}

		let _ = ctx.self_ref().send(Message::ContinueSweep);
	}

	#[inline]
	fn batch_size(&self) -> usize {
		self.config.get_config_uint8(ConfigKey::HistoricalGcBatchSize) as usize
	}

	#[inline]
	fn finish_sweep(&self, buffer: &MultiCommitBufferTier, cutoff: CommitVersion, stats: &GcStats) {
		if stats.versions_dropped > 0 {
			buffer.maintenance();
			debug!(
				cutoff = cutoff.0,
				shapes_scanned = stats.shapes_scanned,
				versions_dropped = stats.versions_dropped,
				"Historical GC sweep completed"
			);
		} else {
			trace!(cutoff = cutoff.0, "Historical GC sweep completed (no drops)");
		}

		self.store.event_bus.emit(HistoricalGcSweepEvent::new(
			cutoff,
			stats.shapes_scanned,
			stats.versions_dropped,
		));
	}

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
		buffer.drop(batches)?;
		Ok(count)
	}
}

impl<W: QueryWatermark> ActorTrait for Actor<W> {
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

pub fn spawn_historical_gc_actor<W: QueryWatermark>(
	store: StandardMultiStore,
	spawner: ActorSpawner,
	watermark: W,
	config: Arc<dyn GetConfig>,
) -> ActorRef<Message> {
	Actor::spawn(&spawner, store, watermark, config)
}
