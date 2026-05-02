// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{
	actors::historical_gc::HistoricalGcMessage as Message,
	common::CommitVersion,
	event::row::HistoricalGcSweepEvent,
	interface::{
		catalog::config::{ConfigKey, GetConfig},
		store::EntryKind,
	},
};
use reifydb_runtime::actor::{
	context::Context,
	mailbox::ActorRef,
	system::{ActorConfig, ActorSystem},
	timers::TimerHandle,
	traits::{Actor as ActorTrait, Directive},
};
use reifydb_type::{Result, util::cowvec::CowVec, value::datetime::DateTime};
use tracing::{debug, trace, warn};

use super::{GcStats, QueryWatermark};
use crate::{
	hot::storage::HotStorage,
	store::StandardMultiStore,
	tier::{HistoricalCursor, TierStorage},
};

pub struct ActorState {
	_timer_handle: Option<TimerHandle>,
	sweeping: bool,
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
		system: &ActorSystem,
		store: StandardMultiStore,
		watermark: W,
		config: Arc<dyn GetConfig>,
	) -> ActorRef<Message> {
		let actor = Self::new(store, watermark, config);
		system.spawn_system("historical-historical", actor).actor_ref().clone()
	}

	fn run_sweep(&self, state: &mut ActorState, _now: DateTime) {
		if state.sweeping {
			trace!("Historical GC sweep already in progress, skipping tick");
			return;
		}
		let Some(hot) = self.store.hot() else {
			warn!("Historical GC sweep skipped: hot tier is not configured");
			return;
		};

		state.sweeping = true;

		let cutoff = self.watermark.query_done_until();
		if cutoff.0 == 0 {
			trace!("Historical GC sweep skipped: watermark is zero");
			state.sweeping = false;
			return;
		}

		let batch_size = self.batch_size();
		let stats = self.sweep_all_shapes(hot, cutoff, batch_size, &mut state.cursors);
		self.finish_sweep(hot, cutoff, &stats);

		state.sweeping = false;
	}

	#[inline]
	fn batch_size(&self) -> usize {
		self.config.get_config_uint8(ConfigKey::HistoricalGcBatchSize) as usize
	}

	#[inline]
	fn sweep_all_shapes(
		&self,
		hot: &HotStorage,
		cutoff: CommitVersion,
		batch_size: usize,
		cursors: &mut HashMap<EntryKind, HistoricalCursor>,
	) -> GcStats {
		let mut stats = GcStats::default();

		let entry_kinds = match hot.list_all_entry_kinds() {
			Ok(v) => v,
			Err(e) => {
				warn!(error = %e, "Historical GC sweep failed: list_all_entry_kinds");
				return stats;
			}
		};

		for entry_kind in entry_kinds {
			let cursor = cursors.entry(entry_kind).or_default();
			if cursor.exhausted {
				*cursor = HistoricalCursor::default();
			}

			let dropped = match self.sweep_shape(hot, entry_kind, cutoff, batch_size, cursor) {
				Ok(n) => n,
				Err(e) => {
					warn!(?entry_kind, error = %e, "Historical GC sweep failed for shape");
					0
				}
			};

			stats.shapes_scanned += 1;
			stats.versions_dropped += dropped;
		}

		stats
	}

	#[inline]
	fn finish_sweep(&self, hot: &HotStorage, cutoff: CommitVersion, stats: &GcStats) {
		if stats.versions_dropped > 0 {
			hot.maintenance();
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
		hot: &HotStorage,
		entry_kind: EntryKind,
		cutoff: CommitVersion,
		batch_size: usize,
		cursor: &mut HistoricalCursor,
	) -> Result<u64> {
		let entries = hot.scan_historical_below(entry_kind, cutoff, cursor, batch_size)?;
		if entries.is_empty() {
			return Ok(0);
		}

		let count = entries.len() as u64;
		let mut batches: HashMap<EntryKind, Vec<(CowVec<u8>, CommitVersion)>> = HashMap::new();
		batches.insert(entry_kind, entries);
		hot.drop(batches)?;
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
			sweeping: false,
			cursors: HashMap::new(),
		}
	}

	fn handle(&self, state: &mut ActorState, msg: Message, ctx: &Context<Message>) -> Directive {
		if ctx.is_cancelled() {
			return Directive::Stop;
		}

		match msg {
			Message::Tick(now) => {
				self.run_sweep(state, now);
			}
			Message::Shutdown => {
				debug!("Historical GC actor shutting down");
				return Directive::Stop;
			}
		}

		Directive::Continue
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
	system: ActorSystem,
	watermark: W,
	config: Arc<dyn GetConfig>,
) -> ActorRef<Message> {
	Actor::spawn(&system, store, watermark, config)
}
