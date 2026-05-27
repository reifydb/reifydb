// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::{
	actors::ttl::RowTtlMessage as Message,
	event::row::RowsExpiredEvent,
	interface::{
		catalog::{config::ConfigKey, shape::ShapeId},
		store::EntryKind,
	},
	row::{TtlAnchor, TtlCleanupMode},
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

use super::{ListRowSettings, ScanStats, scanner};
use crate::{store::StandardMultiStore, tier::RangeCursor};

#[derive(Default)]
pub struct ScannerState {
	cursors: HashMap<ShapeId, RangeCursor>,
}

pub struct ActorState {
	_timer_handle: Option<TimerHandle>,
	scanning: bool,
	scanner: ScannerState,
}

pub struct Actor<P: ListRowSettings> {
	store: StandardMultiStore,
	provider: P,
}

impl<P: ListRowSettings> Actor<P> {
	pub fn new(store: StandardMultiStore, provider: P) -> Self {
		Self {
			store,
			provider,
		}
	}

	pub fn spawn(system: &ActorSystem, store: StandardMultiStore, provider: P) -> ActorRef<Message> {
		let actor = Self::new(store, provider);
		system.spawn_background("row-row", actor).actor_ref().clone()
	}

	fn run_scan(&self, state: &mut ActorState, now: DateTime) {
		if state.scanning {
			debug!("Row TTL scan already in progress, skipping tick");
			return;
		}

		let buffer = self.store.commit();
		let persistent = self.store.persistent();
		if buffer.is_none() && persistent.is_none() {
			warn!("Row TTL scan skipped: no storage tier is configured");
			return;
		}

		state.scanning = true;

		let now_nanos = now.to_nanos();
		trace!(now_nanos, "Starting row TTL scan");

		let entries = self.provider.list_row_settings();
		let config = self.provider.config();
		let mut stats = ScanStats::default();
		let mut persistent_rows_deleted: u64 = 0;

		let batch_size = config.get_config_uint8(ConfigKey::RowTtlScanBatchSize) as usize;

		for (shape_id, settings) in &entries {
			let Some(ttl) = settings.ttl.as_ref() else {
				continue;
			};
			trace!(?shape_id, ?ttl, "Evaluating TTL config for shape");
			if ttl.cleanup_mode == TtlCleanupMode::Delete {
				debug!(?shape_id, "Skipping shape with TtlCleanupMode::Delete (not supported in V1)");
				stats.shapes_skipped += 1;
				continue;
			}

			if let Some(buffer) = buffer {
				let mut cursor = state.scanner.cursors.remove(shape_id).unwrap_or_default();

				let scan_result = match ttl.anchor {
					TtlAnchor::Created => scanner::scan_shape_by_created_at(
						buffer,
						*shape_id,
						ttl,
						now_nanos,
						batch_size,
						&mut cursor,
					),
					TtlAnchor::Updated => scanner::scan_shape_by_updated_at(
						buffer,
						*shape_id,
						ttl,
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
								*stats.bytes_discovered
									.entry(row.shape_id)
									.or_insert(0) += row.scanned_bytes;
								self.store.invalidate_read_key(&row.key);
							}

							match scanner::drop_expired_keys(buffer, &expired, &mut stats) {
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
							scanner::ScanResult::Exhausted => {}
						}
					}
					Err(e) => {
						warn!(?shape_id, error = %e, "Failed to scan shape for expired rows");
					}
				}
			}

			if let Some(persistent) = persistent {
				let cutoff = now_nanos.saturating_sub(ttl.duration_nanos);
				match persistent.delete_expired(EntryKind::Source(*shape_id), ttl.anchor, cutoff, None)
				{
					Ok(deleted) => {
						persistent_rows_deleted += deleted;
						if deleted > 0 {
							self.store.clear_read();
							debug!(
								?shape_id,
								deleted, "Evicted expired rows from persistent tier"
							);
						}
					}
					Err(e) => {
						warn!(?shape_id, error = %e, "Failed to evict expired persistent rows");
					}
				}
			}
		}

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

		if stats.rows_expired > 0 || persistent_rows_deleted > 0 {
			info!(
				shapes_scanned = stats.shapes_scanned,
				shapes_skipped = stats.shapes_skipped,
				rows_expired = stats.rows_expired,
				versions_dropped = stats.versions_dropped,
				persistent_rows_deleted,
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

impl<P: ListRowSettings> ActorTrait for Actor<P> {
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

pub fn spawn_row_settings_actor<P: ListRowSettings>(
	store: StandardMultiStore,
	system: ActorSystem,
	provider: P,
) -> ActorRef<Message> {
	Actor::spawn(&system, store, provider)
}
