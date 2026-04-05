// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{event::gc::MultiStoreVacuumEvent, row::RowTtlCleanupMode};
use reifydb_runtime::actor::{
	context::Context,
	mailbox::ActorRef,
	system::{ActorConfig, ActorSystem},
	timers::TimerHandle,
	traits::{Actor as ActorTrait, Directive},
};
use reifydb_type::value::datetime::DateTime;
use tracing::{debug, info, warn};

use super::{ListRowTtls, config::Config, scanner, stats::GcScanStats};
use crate::store::StandardMultiStore;

/// Messages handled by the TTL GC actor.
#[derive(Debug, Clone)]
pub enum Message {
	/// Periodic tick triggers a full scan cycle.
	Tick(DateTime),
	/// Shutdown gracefully.
	Shutdown,
}

/// Internal state for the GC actor.
pub struct ActorState {
	_timer_handle: Option<TimerHandle>,
	scanning: bool,
}

/// Background actor that periodically scans shapes for expired rows
/// and physically drops them based on TTL configuration.
pub struct Actor<P: ListRowTtls> {
	store: StandardMultiStore,
	provider: P,
	config: Config,
}

impl<P: ListRowTtls> Actor<P> {
	pub fn new(config: Config, store: StandardMultiStore, provider: P) -> Self {
		Self {
			store,
			provider,
			config,
		}
	}

	pub fn spawn(
		system: &ActorSystem,
		config: Config,
		store: StandardMultiStore,
		provider: P,
	) -> ActorRef<Message> {
		let actor = Self::new(config, store, provider);
		system.spawn("row-ttl", actor).actor_ref().clone()
	}
	fn run_scan(&self, state: &mut ActorState, now: DateTime) {
		if state.scanning {
			debug!("TTL GC scan already in progress, skipping tick");
			return;
		}

		let Some(hot) = self.store.hot() else {
			warn!("TTL GC skipped: hot tier is not configured");
			return;
		};

		state.scanning = true;

		let now_nanos = now.to_nanos();
		let ttls = self.provider.list_row_ttls();
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
				hot,
				*shape_id,
				ttl_config,
				now_nanos,
				self.config.scan_batch_size,
			) {
				Ok(expired) => {
					stats.shapes_scanned += 1;
					stats.rows_expired += expired.len() as u64;

					if !expired.is_empty()
						&& let Err(e) =
							scanner::drop_expired_keys(hot, *shape_id, &expired, &mut stats)
					{
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

		self.store.event_bus.emit(MultiStoreVacuumEvent::new(
			stats.shapes_scanned,
			stats.shapes_skipped,
			stats.rows_expired,
			stats.versions_dropped,
			stats.bytes_reclaimed,
		));

		state.scanning = false;
	}
}

impl<P: ListRowTtls> ActorTrait for Actor<P> {
	type State = ActorState;
	type Message = Message;

	fn init(&self, ctx: &Context<Message>) -> ActorState {
		debug!("TTL GC actor started");
		let timer_handle = ctx
			.schedule_tick(self.config.scan_interval, |nanos| Message::Tick(DateTime::from_nanos(nanos)));
		ActorState {
			_timer_handle: Some(timer_handle),
			scanning: false,
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

/// Spawn a TTL GC actor that periodically scans and drops expired rows.
///
/// The provider is typically implemented by the engine layer wrapping
/// the materialized catalog. Call this after both store and catalog
/// are available.
pub fn spawn_row_ttl_actor<P: ListRowTtls>(
	store: StandardMultiStore,
	system: ActorSystem,
	config: Config,
	provider: P,
) -> ActorRef<Message> {
	Actor::spawn(&system, config, store, provider)
}
