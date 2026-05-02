// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_core::{
	actors::operator_ttl::OperatorTtlMessage as Message,
	event::row::OperatorRowsExpiredEvent,
	interface::catalog::{config::ConfigKey, flow::FlowNodeId},
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

use super::{ListOperatorTtls, OperatorScanStats, scanner};
use crate::{gc::row::scanner::ScanResult, store::StandardMultiStore, tier::RangeCursor};

/// Holds state for the chunked, stateful scanner.
#[derive(Default)]
pub struct ScannerState {
	cursors: HashMap<FlowNodeId, RangeCursor>,
}

/// Internal state for the operator-state TTL actor.
pub struct ActorState {
	_timer_handle: Option<TimerHandle>,
	scanning: bool,
	scanner: ScannerState,
}

/// Background actor that periodically scans operator-state rows for expired
/// entries and physically drops them based on per-operator TTL configuration.
pub struct Actor<P: ListOperatorTtls> {
	store: StandardMultiStore,
	provider: P,
}

impl<P: ListOperatorTtls> Actor<P> {
	pub fn new(store: StandardMultiStore, provider: P) -> Self {
		Self {
			store,
			provider,
		}
	}

	pub fn spawn(system: &ActorSystem, store: StandardMultiStore, provider: P) -> ActorRef<Message> {
		let actor = Self::new(store, provider);
		system.spawn_system("operator-row", actor).actor_ref().clone()
	}

	fn run_scan(&self, state: &mut ActorState, now: DateTime) {
		if state.scanning {
			debug!("Operator TTL scan already in progress, skipping tick");
			return;
		}

		let Some(hot) = self.store.hot() else {
			warn!("Operator TTL scan skipped: hot tier is not configured");
			return;
		};

		state.scanning = true;

		let now_nanos = now.to_nanos();
		trace!(now_nanos, "Starting operator TTL scan");

		let ttls = self.provider.list_operator_ttls();
		let config = self.provider.config();
		let mut stats = OperatorScanStats::default();

		let batch_size = config.get_config_uint8(ConfigKey::OperatorTtlScanBatchSize) as usize;

		for (node_id, ttl_config) in &ttls {
			trace!(?node_id, ?ttl_config, "Evaluating TTL config for operator");
			if ttl_config.cleanup_mode == TtlCleanupMode::Delete {
				debug!(?node_id, "Skipping operator with TtlCleanupMode::Delete (not supported in V1)");
				stats.operators_skipped += 1;
				continue;
			}

			let mut cursor = state.scanner.cursors.remove(node_id).unwrap_or_default();

			let scan_result = match ttl_config.anchor {
				TtlAnchor::Created => scanner::scan_operator_by_created_at(
					hot,
					*node_id,
					ttl_config,
					now_nanos,
					batch_size,
					&mut cursor,
				),
				TtlAnchor::Updated => scanner::scan_operator_by_updated_at(
					hot,
					*node_id,
					ttl_config,
					now_nanos,
					batch_size,
					&mut cursor,
				),
			};

			match scan_result {
				Ok((expired, result)) => {
					debug!(
						?node_id,
						expired_count = expired.len(),
						?result,
						"Operator scan iteration completed"
					);
					stats.operators_scanned += 1;

					if !expired.is_empty() {
						stats.rows_expired += expired.len() as u64;
						for row in &expired {
							*stats.bytes_discovered.entry(row.node_id).or_insert(0) +=
								row.scanned_bytes;
						}

						match scanner::drop_expired_operator_keys(hot, &expired, &mut stats) {
							Ok(_) => {
								let bytes_freed: u64 =
									stats.bytes_reclaimed.values().sum();
								debug!(
									?node_id,
									bytes_freed,
									"Freed storage from expired operator-state rows"
								);
							}
							Err(e) => {
								warn!(?node_id, error = %e, "Failed to drop expired operator-state keys");
							}
						}
					}

					match result {
						ScanResult::Yielded => {
							state.scanner.cursors.insert(*node_id, cursor);
						}
						ScanResult::Exhausted => {
							// Cursor is removed; operator restarts from the beginning next
							// tick.
						}
					}
				}
				Err(e) => {
					warn!(?node_id, error = %e, "Failed to scan operator state for expired rows");
				}
			}
		}

		if stats.rows_expired > 0 {
			hot.maintenance();

			info!(
				operators_scanned = stats.operators_scanned,
				operators_skipped = stats.operators_skipped,
				rows_expired = stats.rows_expired,
				versions_dropped = stats.versions_dropped,
				bytes_discovered = ?stats.bytes_discovered.values().sum::<u64>(),
				bytes_reclaimed = ?stats.bytes_reclaimed.values().sum::<u64>(),
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

impl<P: ListOperatorTtls> ActorTrait for Actor<P> {
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

/// Spawn an operator-state TTL actor that periodically scans and drops expired
/// rows under per-operator TTL configurations.
///
/// The provider is typically implemented by the catalog layer reading from a
/// materialized cache populated from `OperatorTtlKey` entries in storage.
pub fn spawn_operator_ttl_actor<P: ListOperatorTtls>(
	store: StandardMultiStore,
	system: ActorSystem,
	provider: P,
) -> ActorRef<Message> {
	Actor::spawn(&system, store, provider)
}
