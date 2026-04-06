// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_core::{
	event::row::RowsExpiredEvent,
	interface::catalog::{config::ConfigKey, shape::ShapeId},
	row::{RowTtlAnchor, RowTtlCleanupMode},
};
use reifydb_runtime::actor::{
	context::Context,
	mailbox::ActorRef,
	system::{ActorConfig, ActorSystem},
	timers::TimerHandle,
	traits::{Actor as ActorTrait, Directive},
};
use reifydb_type::value::datetime::DateTime;
use tracing::{debug, info, trace, warn};

use super::{ListRowTtls, ScanStats, scanner};
use crate::{store::StandardMultiStore, tier::RangeCursor};

/// Messages handled by the row TTL actor.
#[derive(Debug, Clone)]
pub enum Message {
	/// Periodic tick triggers a full scan cycle.
	Tick(DateTime),
	/// Shutdown gracefully.
	Shutdown,
}

/// Holds state for the chunked, stateful scanner.
#[derive(Default)]
pub struct ScannerState {
	cursors: HashMap<ShapeId, RangeCursor>,
}

/// Internal state for the row TTL actor.
pub struct ActorState {
	_timer_handle: Option<TimerHandle>,
	scanning: bool,
	scanner: ScannerState,
}

/// Background actor that periodically scans shapes for expired rows
/// and physically drops them based on TTL configuration.
pub struct Actor<P: ListRowTtls> {
	store: StandardMultiStore,
	provider: P,
}

impl<P: ListRowTtls> Actor<P> {
	pub fn new(store: StandardMultiStore, provider: P) -> Self {
		Self {
			store,
			provider,
		}
	}

	pub fn spawn(system: &ActorSystem, store: StandardMultiStore, provider: P) -> ActorRef<Message> {
		let actor = Self::new(store, provider);
		system.spawn("row-ttl", actor).actor_ref().clone()
	}

	fn run_scan(&self, state: &mut ActorState, now: DateTime) {
		if state.scanning {
			debug!("Row TTL scan already in progress, skipping tick");
			return;
		}

		let Some(hot) = self.store.hot() else {
			warn!("Row TTL scan skipped: hot tier is not configured");
			return;
		};

		state.scanning = true;

		let now_nanos = now.to_nanos();
		trace!(now_nanos, "Starting row TTL scan");

		let ttls = self.provider.list_row_ttls();
		let config = self.provider.config();
		let mut stats = ScanStats::default();

		let batch_size = config.get_config_uint8(ConfigKey::RowTtlScanBatchSize) as usize;

		for (shape_id, ttl_config) in &ttls {
			trace!(?shape_id, ?ttl_config, "Evaluating TTL config for shape");
			if ttl_config.cleanup_mode == RowTtlCleanupMode::Delete {
				debug!(
					?shape_id,
					"Skipping shape with RowTtlCleanupMode::Delete (not supported in V1)"
				);
				stats.shapes_skipped += 1;
				continue;
			}

			let mut cursor = state.scanner.cursors.remove(shape_id).unwrap_or_default();

			let scan_result = match ttl_config.anchor {
				RowTtlAnchor::Created => scanner::scan_shape_by_created_at(
					hot,
					*shape_id,
					ttl_config,
					now_nanos,
					batch_size,
					&mut cursor,
				),
				RowTtlAnchor::Updated => scanner::scan_shape_by_updated_at(
					hot,
					*shape_id,
					ttl_config,
					now_nanos,
					batch_size,
					&mut cursor,
				),
			};

			match scan_result {
				Ok((expired, result)) => {
					debug!(
						?shape_id,
						expired_count = expired.len(),
						?result,
						"Shape scan iteration completed"
					);
					stats.shapes_scanned += 1;

					if !expired.is_empty() {
						stats.rows_expired += expired.len() as u64;
						for row in &expired {
							*stats.bytes_discovered.entry(row.shape_id).or_insert(0) +=
								row.scanned_bytes;
						}

						match scanner::drop_expired_keys(hot, &expired, &mut stats) {
							Ok(_) => {
								let bytes_freed: u64 =
									stats.bytes_reclaimed.values().sum();
								debug!(
									?shape_id,
									bytes_freed,
									"Freed storage from expired rows for shape"
								);
							}
							Err(e) => {
								warn!(?shape_id, error = %e, "Failed to drop expired keys");
							}
						}
					}

					match result {
						scanner::ScanResult::Yielded => {
							state.scanner.cursors.insert(*shape_id, cursor);
						}
						scanner::ScanResult::Exhausted => {
							// Cursor is already removed, shape will restart from beginning
							// next tick.
						}
					}
				}
				Err(e) => {
					warn!(?shape_id, error = %e, "Failed to scan shape for expired rows");
					// On error, we drop the cursor to restart scanning for this shape next tick.
				}
			}
		}

		if stats.rows_expired > 0 {
			// Trigger maintenance to physically reclaim memory/disk space (especially for SQLite)
			hot.maintenance();

			info!(
				shapes_scanned = stats.shapes_scanned,
				shapes_skipped = stats.shapes_skipped,
				rows_expired = stats.rows_expired,
				versions_dropped = stats.versions_dropped,
				bytes_discovered = ?stats.bytes_discovered.values().sum::<u64>(),
				bytes_reclaimed = ?stats.bytes_reclaimed.values().sum::<u64>(),
				"Row TTL scan completed"
			);
		} else {
			debug!(
				shapes_scanned = stats.shapes_scanned,
				shapes_skipped = stats.shapes_skipped,
				"Row TTL scan completed (no expired rows)"
			);
		}

		self.store.event_bus.emit(RowsExpiredEvent::new(
			stats.shapes_scanned,
			stats.shapes_skipped,
			stats.rows_expired,
			stats.versions_dropped,
			stats.bytes_discovered,
			stats.bytes_reclaimed,
		));

		state.scanning = false;
	}
}

impl<P: ListRowTtls> ActorTrait for Actor<P> {
	type State = ActorState;
	type Message = Message;

	fn init(&self, ctx: &Context<Message>) -> ActorState {
		debug!("Row TTL actor started");
		let config = self.provider.config();
		let scan_interval = config.get_config_duration(ConfigKey::RowTtlScanInterval);

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
				debug!("Row TTL actor shutting down");
				return Directive::Stop;
			}
		}

		Directive::Continue
	}

	fn post_stop(&self) {
		debug!("Row TTL actor stopped");
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(64)
	}
}

/// Spawn a row TTL actor that periodically scans and drops expired rows.
///
/// The provider is typically implemented by the engine layer wrapping
/// the materialized catalog. Call this after both store and catalog
/// are available.
pub fn spawn_row_ttl_actor<P: ListRowTtls>(
	store: StandardMultiStore,
	system: ActorSystem,
	provider: P,
) -> ActorRef<Message> {
	Actor::spawn(&system, store, provider)
}
