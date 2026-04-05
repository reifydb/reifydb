// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;
use std::time::Duration as StdDuration;

use reifydb_core::{
	event::row::RowsExpiredEvent,
	interface::catalog::shape::ShapeId,
	row::{RowTtlAnchor, RowTtlCleanupMode},
};
use reifydb_runtime::actor::{
	context::Context,
	mailbox::ActorRef,
	system::{ActorConfig, ActorSystem},
	timers::TimerHandle,
	traits::{Actor as ActorTrait, Directive},
};
use reifydb_type::value::{Value, datetime::DateTime, duration::Duration};
use tracing::{debug, info, warn};

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
		let system_config = provider.system_config();
		system_config.register(
			"ROW_TTL_SCAN_BATCH_SIZE",
			Value::Uint8(10000),
			"Max rows to examine per batch during a row TTL scan.",
			false,
		);
		system_config.register(
			"ROW_TTL_SCAN_INTERVAL",
			Value::Duration(Duration::from_seconds(60).unwrap()),
			"How often the row TTL actor should scan for expired rows.",
			false,
		);

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
		let ttls = self.provider.list_row_ttls();
		let system_config = self.provider.system_config();
		let mut stats = ScanStats::default();
		let mut all_expired = Vec::new();

		let batch_size =
			system_config.get_uint8("ROW_TTL_SCAN_BATCH_SIZE").map(|v| v as usize).unwrap_or(10000);

		for (shape_id, ttl_config) in &ttls {
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
					stats.shapes_scanned += 1;
					all_expired.extend(expired);

					match result {
						scanner::ScanResult::PrunedEarly | scanner::ScanResult::Yielded => {
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

		stats.rows_expired = all_expired.len() as u64;
		for row in &all_expired {
			*stats.bytes_discovered.entry(row.shape_id).or_insert(0) += row.scanned_bytes;
		}
		if !all_expired.is_empty()
			&& let Err(e) = scanner::drop_expired_keys(hot, &all_expired, &mut stats)
		{
			warn!(error = %e, "Failed to drop expired keys");
		}
		if stats.rows_expired > 0 {
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
		let system_config = self.provider.system_config();
		let scan_interval =
			system_config.get_duration("ROW_TTL_SCAN_INTERVAL").unwrap_or_else(|| StdDuration::from_secs(60));

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
