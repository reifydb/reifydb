// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_runtime::actor::system::ActorHandle;
use reifydb_value::{Result, value::datetime::DateTime};

use crate::{
	common::{ChangeVersion, CommitVersion},
	delta::Delta,
};

pub type CdcProduceHandle = ActorHandle<CdcProduceMessage>;

#[derive(Clone, Debug)]
pub enum CdcProduceMessage {
	Produce {
		version: ChangeVersion,
		changed_at: DateTime,
		deltas: Vec<Delta>,
	},
}

pub type CdcPollHandle = ActorHandle<CdcPollMessage>;

pub enum CdcPollMessage {
	Poll,

	CheckWatermark,

	ConsumeResponse {
		result: Result<()>,
	},

	ResyncResponse {
		result: Result<CommitVersion>,
	},

	Tick,

	Shutdown,
}
