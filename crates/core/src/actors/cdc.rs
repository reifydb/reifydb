// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_runtime::actor::system::ActorHandle;
use reifydb_type::{Result, value::datetime::DateTime};

use crate::{common::CommitVersion, delta::Delta};

/// Handle to the CDC producer actor.
pub type CdcProduceHandle = ActorHandle<CdcProduceMessage>;

/// Message type for the CDC producer actor.
#[derive(Clone, Debug)]
pub enum CdcProduceMessage {
	Produce {
		version: CommitVersion,
		changed_at: DateTime,
		deltas: Vec<Delta>,
	},
	Tick,
}

/// Handle to the CDC consumer poll actor.
pub type CdcPollHandle = ActorHandle<CdcPollMessage>;

/// Messages for the CDC consumer poll actor.
pub enum CdcPollMessage {
	/// Trigger a poll for CDC events
	Poll,
	/// Retry watermark readiness check
	CheckWatermark,
	/// Async response from the consumer
	ConsumeResponse(Result<()>),
}
