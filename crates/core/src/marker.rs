// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, sync::Arc};

use reifydb_runtime::context::clock::Instant;
use reifydb_value::value::{digest::Digest, duration::Duration};

use crate::{common::CommitVersion, interface::catalog::id::ViewId};

pub const MARKER_T0_CAP: usize = 64;

#[derive(Debug, Clone)]
pub struct Marker {
	pub id: CommitVersion,
	pub t0s: Arc<[Instant]>,
}

#[derive(Debug, Clone)]
pub struct MarkerEntry {
	pub committed_at: Instant,
	pub writer: Option<Marker>,
	pub by_view: BTreeMap<ViewId, Vec<Marker>>,
}

pub trait MarkerSink: Send + Sync + 'static {
	fn insert(&self, version: CommitVersion, entry: MarkerEntry);
}

pub trait MarkerSweep: Send + Sync + 'static {
	fn sweep(&self, floor: Option<CommitVersion>) -> usize;
}

pub trait LatencyReader: Send + Sync + 'static {
	fn take(&self) -> Vec<ViewLatency>;
}

#[derive(Debug, Clone)]
pub struct ViewLatency {
	pub view: ViewId,
	pub n: u64,
	pub max: Duration,
	pub end_to_end: Digest,
	pub hop_max: Duration,
	pub hop: Digest,
}

#[derive(Debug, Clone, Copy)]
pub struct LatencyConsumer;
