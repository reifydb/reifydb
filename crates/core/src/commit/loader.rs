// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::VecDeque,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use reifydb_runtime::actor::{
	context::Context,
	system::{ActorConfig, ActorHandle},
	traits::{Actor, Directive},
};
use reifydb_value::{Result, byte_size::ByteSize};
use reifydb_wal::lsn::Lsn;

use crate::{
	common::CommitVersion,
	interface::commit::{CommitLog, Loaded},
	metrics::{
		collect::MetricsCollector,
		sample::{MetricsSample, Reading},
	},
};

pub type CommitLoaderHandle = ActorHandle<CommitLoaderMessage>;

pub type CommitLoaderReply = Box<dyn FnOnce(Result<Loaded>) + Send>;

pub enum CommitLoaderMessage {
	Fetch {
		after: (CommitVersion, Lsn),
		up_to: CommitVersion,
		budget: ByteSize,
		reply: CommitLoaderReply,
	},
}

const MEMO_DEPTH: usize = 4;

struct MemoEntry {
	after: CommitVersion,
	loaded: Loaded,
}

#[derive(Clone, Default)]
pub struct CommitLoaderMetrics {
	inner: Arc<CommitLoaderMetricsInner>,
}

#[derive(Default)]
struct CommitLoaderMetricsInner {
	loads: AtomicU64,
	memo_hits: AtomicU64,
	bytes_loaded: AtomicU64,
}

impl MetricsCollector for CommitLoaderMetrics {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		out.push(MetricsSample::counter("commit_loader", "loads", self.inner.loads.load(Ordering::Relaxed)));
		out.push(MetricsSample::counter(
			"commit_loader",
			"memo_hits",
			self.inner.memo_hits.load(Ordering::Relaxed),
		));
		out.push(MetricsSample::cumulative(
			"commit_loader",
			"bytes_loaded",
			Reading::Bytes(ByteSize::from_bytes(self.inner.bytes_loaded.load(Ordering::Relaxed))),
		));
	}
}

pub struct CommitLoader {
	log: Arc<dyn CommitLog>,
	metrics: CommitLoaderMetrics,
}

pub struct CommitLoaderState {
	memo: VecDeque<MemoEntry>,
}

impl CommitLoader {
	pub fn new(log: Arc<dyn CommitLog>, metrics: CommitLoaderMetrics) -> Self {
		Self {
			log,
			metrics,
		}
	}

	fn serve(
		&self,
		state: &mut CommitLoaderState,
		after: (CommitVersion, Lsn),
		up_to: CommitVersion,
		budget: ByteSize,
	) -> Result<Loaded> {
		let cursor = after.0;
		if let Some(memo) = state.memo.iter().find(|memo| {
			memo.after <= cursor && cursor < memo.loaded.advance_to && memo.loaded.advance_to <= up_to
		}) {
			self.metrics.inner.memo_hits.fetch_add(1, Ordering::Relaxed);
			let start = memo.loaded.entries.partition_point(|entry| entry.version.commit <= cursor);
			return Ok(Loaded {
				entries: memo.loaded.entries[start..].to_vec(),
				advance_to: memo.loaded.advance_to,
				more: memo.loaded.more || memo.loaded.advance_to < up_to,
			});
		}

		let loaded = self.log.read(after, up_to, budget)?;
		self.metrics.inner.loads.fetch_add(1, Ordering::Relaxed);
		self.metrics
			.inner
			.bytes_loaded
			.fetch_add(loaded.entries.iter().map(|entry| entry.bytes()).sum::<u64>(), Ordering::Relaxed);
		state.memo.push_front(MemoEntry {
			after: cursor,
			loaded: loaded.clone(),
		});
		state.memo.truncate(MEMO_DEPTH);
		Ok(loaded)
	}
}

impl Actor for CommitLoader {
	type State = CommitLoaderState;
	type Message = CommitLoaderMessage;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
		CommitLoaderState {
			memo: VecDeque::new(),
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		match msg {
			CommitLoaderMessage::Fetch {
				after,
				up_to,
				budget,
				reply,
			} => {
				let loaded = self.serve(state, after, up_to, budget);
				(reply)(loaded);
			}
		}
		Directive::Continue
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new()
	}
}
