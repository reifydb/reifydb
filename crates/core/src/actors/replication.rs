// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_runtime::actor::{reply::Reply, system::ActorHandle};
use reifydb_type::error::Error;

use crate::{
	common::CommitVersion,
	interface::cdc::{CdcBatch, SystemChange},
};

pub type ReplicationPrimaryHandle = ActorHandle<ReplicationPrimaryMessage>;

pub enum ReplicationPrimaryMessage {
	ReadCdcBatch {
		since_version: CommitVersion,
		batch_size: u64,
		reply: Reply<Result<CdcBatch, Error>>,
	},

	GetVersion {
		reply: Reply<Result<VersionInfo, Error>>,
	},
}

pub struct VersionInfo {
	pub current: Option<CommitVersion>,
	pub min_cdc: Option<CommitVersion>,
	pub max_cdc: Option<CommitVersion>,
}

pub type ReplicationReplicaHandle = ActorHandle<ReplicationReplicaMessage>;

pub enum ReplicationReplicaMessage {
	ApplyEntry {
		version: CommitVersion,
		system_changes: Vec<SystemChange>,
		reply: Reply<Result<(), Error>>,
	},

	GetCurrentVersion {
		reply: Reply<CommitVersion>,
	},
}
