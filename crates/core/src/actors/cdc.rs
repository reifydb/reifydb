// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_runtime::actor::system::ActorHandle;
use reifydb_value::value::datetime::DateTime;

use crate::{common::ChangeVersion, delta::Delta};

pub type CdcProduceHandle = ActorHandle<CdcProduceMessage>;

#[derive(Clone, Debug)]
pub enum CdcProduceMessage {
	Produce {
		version: ChangeVersion,
		changed_at: DateTime,
		deltas: Vec<Delta>,
	},
}
