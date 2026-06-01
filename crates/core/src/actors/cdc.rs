// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_runtime::actor::system::ActorHandle;
use reifydb_value::{Result, value::datetime::DateTime};

use crate::{common::CommitVersion, delta::Delta, interface::change::Change};

pub type CdcProduceHandle = ActorHandle<CdcProduceMessage>;

#[derive(Clone, Debug)]
pub enum CdcProduceMessage {
	Produce {
		version: CommitVersion,
		changed_at: DateTime,
		deltas: Vec<Delta>,
		flow_changes: Vec<Change>,
	},
	Tick,
}

pub type CdcPollHandle = ActorHandle<CdcPollMessage>;

pub enum CdcPollMessage {
	Poll,

	CheckWatermark,

	ConsumeResponse(Result<()>),

	Shutdown,
}
