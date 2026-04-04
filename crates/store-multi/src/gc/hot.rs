// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	event::{EventBus, gc::MultiStoreVacuumEvent},
	row::RowTtlCleanupMode,
};
use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::ActorRef,
		system::{ActorConfig, ActorSystem},
		timers::TimerHandle,
		traits::{Actor, Directive},
	},
	context::clock::Clock,
};
use tracing::{debug, info, warn};

use super::{RowTtlProvider, config::GcConfig, scanner, stats::GcScanStats};
use crate::hot::storage::HotStorage;

/// Messages handled by the TTL GC actor.
#[derive(Debug, Clone)]
pub enum GcHotMessage {
	/// Periodic tick triggers a full scan cycle.
	Tick,
	/// Shutdown gracefully.
	Shutdown,
}

/// Internal state for the GC actor.
pub struct GcHotActorState {
	_timer_handle: Option<TimerHandle>,
	scanning: bool,
}

/// Background actor that periodically scans shapes for expired rows
/// and physically drops them based on TTL configuration.
pub struct GcHotActor<P: RowTtlProvider> {
	storage: HotStorage,
	provider: P,
	config: GcConfig,
	clock: Clock,
	event_bus: EventBus,
}

impl<P: RowTtlProvider> GcHotActor<P> {
	pub fn new(config: GcConfig, storage: HotStorage, provider: P, clock: Clock, event_bus: EventBus) -> Self {
		Self {
			storage,
			provider,
			config,
			clock,
			event_bus,
		}
	}

	pub fn spawn(
		system: &ActorSystem,
		config: GcConfig,
		storage: HotStorage,
		provider: P,
		clock: Clock,
		event_bus: EventBus,
	) -> ActorRef<GcHotMessage> {
		let actor = Self::new(config, storage, provider, clock, event_bus);
		system.spawn("ttl-gc", actor).actor_ref().clone()
	}

	fn run_scan(&self, state: &mut GcHotActorState) {
		if state.scanning {
			debug!("TTL GC scan already in progress, skipping tick");
			return;
		}
		state.scanning = true;

		let now_nanos = self.clock.now_nanos();
		let ttls = self.provider.row_ttls();
		let mut stats = GcScanStats::default();

		for (shape_id, ttl_config) in &ttls {
			if ttl_config.cleanup_mode == RowTtlCleanupMode::Delete {
				debug!(
					?shape_id,
					"Skipping shape with RowTtlCleanupMode::Delete (not supported in V1)"
				);
				stats.shapes_skipped += 1;
				continue;
			}

			match scanner::scan_shape_for_expired(
				&self.storage,
				*shape_id,
				ttl_config,
				now_nanos,
				self.config.scan_batch_size,
			) {
				Ok(expired) => {
					stats.shapes_scanned += 1;
					stats.rows_expired += expired.len() as u64;

					if !expired.is_empty()
						&& let Err(e) = scanner::drop_expired_keys(
							&self.storage,
							*shape_id,
							&expired,
							&mut stats,
						) {
						warn!(?shape_id, error = %e, "Failed to drop expired keys");
					}
				}
				Err(e) => {
					warn!(?shape_id, error = %e, "Failed to scan shape for expired rows");
				}
			}
		}

		if stats.rows_expired > 0 {
			info!(
				shapes_scanned = stats.shapes_scanned,
				shapes_skipped = stats.shapes_skipped,
				rows_expired = stats.rows_expired,
				versions_dropped = stats.versions_dropped,
				"TTL GC scan completed"
			);
		} else {
			debug!(
				shapes_scanned = stats.shapes_scanned,
				shapes_skipped = stats.shapes_skipped,
				"TTL GC scan completed (no expired rows)"
			);
		}

		self.event_bus.emit(MultiStoreVacuumEvent::new(
			stats.shapes_scanned,
			stats.shapes_skipped,
			stats.rows_expired,
			stats.versions_dropped,
			stats.bytes_reclaimed,
		));

		state.scanning = false;
	}
}

impl<P: RowTtlProvider> Actor for GcHotActor<P> {
	type State = GcHotActorState;
	type Message = GcHotMessage;

	fn init(&self, ctx: &Context<GcHotMessage>) -> GcHotActorState {
		debug!("TTL GC actor started");
		let timer_handle = ctx.schedule_repeat(self.config.scan_interval, GcHotMessage::Tick);
		GcHotActorState {
			_timer_handle: Some(timer_handle),
			scanning: false,
		}
	}

	fn handle(&self, state: &mut GcHotActorState, msg: GcHotMessage, ctx: &Context<GcHotMessage>) -> Directive {
		if ctx.is_cancelled() {
			return Directive::Stop;
		}

		match msg {
			GcHotMessage::Tick => {
				self.run_scan(state);
			}
			GcHotMessage::Shutdown => {
				debug!("TTL GC actor shutting down");
				return Directive::Stop;
			}
		}

		Directive::Continue
	}

	fn post_stop(&self) {
		debug!("TTL GC actor stopped");
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(64)
	}
}
