// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod census;
mod checkpoint;
mod flush;
mod state;

use std::{collections::BTreeMap, sync::Arc};

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator::state::GroupStateKey,
	metrics::collect::MetricsCollector,
};
use reifydb_runtime::sync::mutex::Mutex;
use tracing::instrument;

use crate::persistent::Persistent;

type Rows = BTreeMap<OperatorId, BTreeMap<GroupStateKey, EncodedPodRow>>;

#[derive(Default)]
struct Inner {
	rows: Mutex<Rows>,
	checkpoints: Mutex<BTreeMap<FlowId, CommitVersion>>,
}

#[derive(Clone, Default)]
pub struct MemoryPersistent(Arc<Inner>);

impl MemoryPersistent {
	#[instrument(name = "store::operator::persistent::memory::new", level = "trace", skip_all)]
	pub fn new() -> Self {
		Self::default()
	}
}

#[instrument(name = "store::operator::persistent::memory::row_bytes", level = "trace", skip_all)]
fn row_bytes(key: &GroupStateKey, row: &EncodedPodRow) -> u64 {
	key.as_slice().len() as u64 + row.len() as u64
}

impl Persistent for MemoryPersistent {
	#[instrument(name = "store::operator::persistent::memory::metrics_collectors", level = "trace", skip_all)]
	fn metrics_collectors(&self) -> Vec<Arc<dyn MetricsCollector>> {
		Vec::new()
	}
}
