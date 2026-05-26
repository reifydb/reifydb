// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::{
	actors::operator_ttl::OperatorTtlMessage as Message,
	event::row::OperatorRowsExpiredEvent,
	interface::{catalog::config::ConfigKey, store::EntryKind},
	key::flow_node_state::FlowNodeStateKey,
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

use super::{ListOperatorSettings, OperatorScanStats, scanner};
use crate::{gc::row::scanner::ScanResult, store::StandardMultiStore, tier::RangeCursor};

#[derive(Default)]
pub struct ScannerState {
	cursors: HashMap<reifydb_core::interface::catalog::flow::FlowNodeId, RangeCursor>,
}

pub struct ActorState {
	_timer_handle: Option<TimerHandle>,
	scanning: bool,
	scanner: ScannerState,
}

pub struct Actor<P: ListOperatorSettings> {
	store: StandardMultiStore,
	provider: P,
}

impl<P: ListOperatorSettings> Actor<P> {
	pub fn new(store: StandardMultiStore, provider: P) -> Self {
		Self {
			store,
			provider,
		}
	}

	pub fn spawn(system: &ActorSystem, store: StandardMultiStore, provider: P) -> ActorRef<Message> {
		let actor = Self::new(store, provider);
		system.spawn_background("operator-row", actor).actor_ref().clone()
	}

	fn run_scan(&self, state: &mut ActorState, now: DateTime) {
		if state.scanning {
			debug!("Operator TTL scan already in progress, skipping tick");
			return;
		}

		let buffer = self.store.buffer();
		let persistent = self.store.persistent();
		if buffer.is_none() && persistent.is_none() {
			warn!("Operator TTL scan skipped: no storage tier is configured");
			return;
		}

		state.scanning = true;

		let now_nanos = now.to_nanos();
		trace!(now_nanos, "Starting operator TTL scan");

		let entries = self.provider.list_operator_settings();
		let config = self.provider.config();
		let mut stats = OperatorScanStats::default();
		let mut persistent_rows_deleted: u64 = 0;

		let batch_size = config.get_config_uint8(ConfigKey::OperatorTtlScanBatchSize) as usize;

		for (node_id, settings) in &entries {
			if let Some(join) = settings.join.as_ref() {
				let left = join.left.as_ref();
				let right = join.right.as_ref();
				if left.is_none() && right.is_none() {
					continue;
				}

				if let Some(buffer) = buffer {
					let mut cursor = state.scanner.cursors.remove(node_id).unwrap_or_default();
					match scanner::scan_operator_join(
						buffer,
						*node_id,
						left,
						right,
						now_nanos,
						batch_size,
						&mut cursor,
					) {
						Ok((expired, result)) => {
							stats.operators_scanned += 1;
							if !expired.is_empty() {
								stats.rows_expired += expired.len() as u64;
								for row in &expired {
									*stats.bytes_discovered
										.entry(row.node_id)
										.or_insert(0) += row.scanned_bytes;
								}
								if let Err(e) = scanner::drop_expired_operator_keys(
									buffer, &expired, &mut stats,
								) {
									warn!(?node_id, error = %e, "Failed to drop expired join-state keys");
								}
							}
							if let ScanResult::Yielded = result {
								state.scanner.cursors.insert(*node_id, cursor);
							}
						}
						Err(e) => {
							warn!(?node_id, error = %e, "Failed to scan join operator state for expired rows");
						}
					}
				}

				if let Some(persistent) = persistent {
					for (side_ttl, side_prefix) in
						[(left, scanner::JOIN_LEFT_PREFIX), (right, scanner::JOIN_RIGHT_PREFIX)]
					{
						let Some(ttl) = side_ttl else {
							continue;
						};
						let cutoff = now_nanos.saturating_sub(ttl.duration_nanos);
						let prefix = FlowNodeStateKey::encoded(*node_id, vec![side_prefix]);
						match persistent.delete_expired(
							EntryKind::Operator(*node_id),
							ttl.anchor,
							cutoff,
							Some(prefix.as_ref()),
						) {
							Ok(deleted) => persistent_rows_deleted += deleted,
							Err(e) => {
								warn!(?node_id, error = %e, "Failed to evict expired persistent join rows");
							}
						}
					}
				}

				continue;
			}

			let Some(ttl) = settings.ttl.as_ref() else {
				continue;
			};
			trace!(?node_id, ?ttl, "Evaluating TTL config for operator");
			if ttl.cleanup_mode == TtlCleanupMode::Delete {
				debug!(?node_id, "Skipping operator with TtlCleanupMode::Delete (not supported in V1)");
				stats.operators_skipped += 1;
				continue;
			}

			if let Some(buffer) = buffer {
				let mut cursor = state.scanner.cursors.remove(node_id).unwrap_or_default();

				let scan_result = match ttl.anchor {
					TtlAnchor::Created => scanner::scan_operator_by_created_at(
						buffer,
						*node_id,
						ttl,
						now_nanos,
						batch_size,
						&mut cursor,
					),
					TtlAnchor::Updated => scanner::scan_operator_by_updated_at(
						buffer,
						*node_id,
						ttl,
						now_nanos,
						batch_size,
						&mut cursor,
					),
				};

				match scan_result {
					Ok((expired, result)) => {
						stats.operators_scanned += 1;

						if !expired.is_empty() {
							stats.rows_expired += expired.len() as u64;
							for row in &expired {
								*stats.bytes_discovered
									.entry(row.node_id)
									.or_insert(0) += row.scanned_bytes;
							}

							if let Err(e) = scanner::drop_expired_operator_keys(
								buffer, &expired, &mut stats,
							) {
								warn!(?node_id, error = %e, "Failed to drop expired operator-state keys");
							}
						}

						match result {
							ScanResult::Yielded => {
								state.scanner.cursors.insert(*node_id, cursor);
							}
							ScanResult::Exhausted => {}
						}
					}
					Err(e) => {
						warn!(?node_id, error = %e, "Failed to scan operator state for expired rows");
					}
				}
			}

			if let Some(persistent) = persistent {
				let cutoff = now_nanos.saturating_sub(ttl.duration_nanos);
				match persistent.delete_expired(EntryKind::Operator(*node_id), ttl.anchor, cutoff, None)
				{
					Ok(deleted) => {
						persistent_rows_deleted += deleted;
						if deleted > 0 {
							debug!(
								?node_id,
								deleted,
								"Evicted expired operator rows from persistent tier"
							);
						}
					}
					Err(e) => {
						warn!(?node_id, error = %e, "Failed to evict expired persistent operator rows");
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
				operators_scanned = stats.operators_scanned,
				operators_skipped = stats.operators_skipped,
				rows_expired = stats.rows_expired,
				versions_dropped = stats.versions_dropped,
				persistent_rows_deleted,
				"Operator TTL scan completed"
			);
		} else {
			debug!(
				operators_scanned = stats.operators_scanned,
				operators_skipped = stats.operators_skipped,
				"Operator TTL scan completed (no expired rows)"
			);
		}

		self.store.event_bus.emit(OperatorRowsExpiredEvent::new(
			stats.operators_scanned,
			stats.operators_skipped,
			stats.rows_expired,
			stats.versions_dropped,
			stats.bytes_discovered,
			stats.bytes_reclaimed,
		));

		state.scanning = false;
	}
}

impl<P: ListOperatorSettings> ActorTrait for Actor<P> {
	type State = ActorState;
	type Message = Message;

	fn init(&self, ctx: &Context<Message>) -> ActorState {
		debug!("Operator TTL actor started");
		let config = self.provider.config();
		let scan_interval = config.get_config_duration(ConfigKey::OperatorTtlScanInterval);

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
				debug!("Operator TTL actor shutting down");
				return Directive::Stop;
			}
		}

		Directive::Continue
	}

	fn post_stop(&self) {
		debug!("Operator TTL actor stopped");
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(64)
	}
}

pub fn spawn_operator_settings_actor<P: ListOperatorSettings>(
	store: StandardMultiStore,
	system: ActorSystem,
	provider: P,
) -> ActorRef<Message> {
	Actor::spawn(&system, store, provider)
}
