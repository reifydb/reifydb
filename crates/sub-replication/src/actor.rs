// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Actor implementations for the replication subsystem.
//!
//! These actors wrap the core replication logic so DST tests can exercise
//! CDC reading and applying without any network transport.

use std::ops::Bound;

use reifydb_cdc::storage::CdcStore;
use reifydb_core::actors::replication::{ReplicationPrimaryMessage, ReplicationReplicaMessage, VersionInfo};
use reifydb_runtime::actor::{
	context::Context,
	traits::{Actor, Directive},
};

use crate::replica::applier::ReplicaApplier;

const MAX_BATCH_SIZE: u64 = 1024;

pub struct ReplicationPrimaryActor {
	cdc_store: CdcStore,
}

impl ReplicationPrimaryActor {
	pub fn new(cdc_store: CdcStore) -> Self {
		Self {
			cdc_store,
		}
	}
}

pub struct ReplicationPrimaryState {
	pub batches_read: u64,
	pub errors: u64,
}

impl Actor for ReplicationPrimaryActor {
	type State = ReplicationPrimaryState;
	type Message = ReplicationPrimaryMessage;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
		ReplicationPrimaryState {
			batches_read: 0,
			errors: 0,
		}
	}

	fn handle(
		&self,
		state: &mut ReplicationPrimaryState,
		msg: ReplicationPrimaryMessage,
		_ctx: &Context<ReplicationPrimaryMessage>,
	) -> Directive {
		match msg {
			ReplicationPrimaryMessage::ReadCdcBatch {
				since_version,
				batch_size,
				reply,
			} => {
				let batch_size = batch_size.min(MAX_BATCH_SIZE);
				match self.cdc_store.read_range(
					Bound::Excluded(since_version),
					Bound::Unbounded,
					batch_size,
				) {
					Ok(batch) => {
						state.batches_read += 1;
						reply.send(Ok(batch));
					}
					Err(e) => {
						state.errors += 1;
						reply.send(Err(e.into()));
					}
				}
			}
			ReplicationPrimaryMessage::GetVersion {
				reply,
			} => {
				let result = self.cdc_store.max_version().and_then(|max| {
					self.cdc_store.min_version().map(|min| VersionInfo {
						current: max,
						min_cdc: min,
						max_cdc: max,
					})
				});
				match result {
					Ok(info) => reply.send(Ok(info)),
					Err(e) => {
						state.errors += 1;
						reply.send(Err(e.into()));
					}
				}
			}
		}
		Directive::Continue
	}
}

pub struct ReplicationReplicaActor {
	applier: ReplicaApplier,
}

impl ReplicationReplicaActor {
	pub fn new(applier: ReplicaApplier) -> Self {
		Self {
			applier,
		}
	}
}

pub struct ReplicationReplicaState {
	pub entries_applied: u64,
	pub errors: u64,
}

impl Actor for ReplicationReplicaActor {
	type State = ReplicationReplicaState;
	type Message = ReplicationReplicaMessage;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
		ReplicationReplicaState {
			entries_applied: 0,
			errors: 0,
		}
	}

	fn handle(
		&self,
		state: &mut ReplicationReplicaState,
		msg: ReplicationReplicaMessage,
		_ctx: &Context<ReplicationReplicaMessage>,
	) -> Directive {
		match msg {
			ReplicationReplicaMessage::ApplyEntry {
				version,
				system_changes,
				reply,
			} => match self.applier.apply_changes(version, &system_changes) {
				Ok(()) => {
					state.entries_applied += 1;
					reply.send(Ok(()));
				}
				Err(e) => {
					state.errors += 1;
					reply.send(Err(e));
				}
			},
			ReplicationReplicaMessage::GetCurrentVersion {
				reply,
			} => {
				reply.send(self.applier.current_version());
			}
		}
		Directive::Continue
	}
}
