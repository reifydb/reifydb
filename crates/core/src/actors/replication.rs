// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_runtime::actor::{reply::Reply, system::ActorHandle};
use reifydb_type::error::Error;

use crate::{
	common::CommitVersion,
	interface::cdc::{CdcBatch, SystemChange},
};

/// Handle to the replication primary actor.
pub type ReplicationPrimaryHandle = ActorHandle<ReplicationPrimaryMessage>;

/// Messages for the replication primary actor.
pub enum ReplicationPrimaryMessage {
	/// Read a batch of CDC entries starting after `since_version`.
	ReadCdcBatch {
		since_version: CommitVersion,
		batch_size: u64,
		reply: Reply<Result<CdcBatch, Error>>,
	},
	/// Get the current CDC version range.
	GetVersion {
		reply: Reply<Result<VersionInfo, Error>>,
	},
}

/// CDC version range returned by the primary.
pub struct VersionInfo {
	pub current: Option<CommitVersion>,
	pub min_cdc: Option<CommitVersion>,
	pub max_cdc: Option<CommitVersion>,
}

/// Handle to the replication replica actor.
pub type ReplicationReplicaHandle = ActorHandle<ReplicationReplicaMessage>;

/// Messages for the replication replica actor.
pub enum ReplicationReplicaMessage {
	/// Apply system changes at a given version.
	ApplyEntry {
		version: CommitVersion,
		system_changes: Vec<SystemChange>,
		reply: Reply<Result<(), Error>>,
	},
	/// Get the last applied version.
	GetCurrentVersion {
		reply: Reply<CommitVersion>,
	},
}
