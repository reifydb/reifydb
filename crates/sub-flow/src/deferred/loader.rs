// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::VecDeque, ops::Bound, sync::Arc};

use std::sync::atomic::{AtomicU64, Ordering};

use reifydb_cdc::{consume::backlog::cdc_bytes, storage::CdcHotReader};
use reifydb_core::{
	common::CommitVersion,
	interface::cdc::Cdc,
	metrics::{collect::MetricsCollector, sample::MetricsSample},
};
use reifydb_runtime::actor::{
	context::Context,
	system::{ActorConfig, ActorHandle},
	traits::{Actor, Directive},
};
use reifydb_value::byte_size::ByteSize;

pub type LoaderHandle = ActorHandle<LoaderMessage>;

pub type LoadedChunk = std::result::Result<(Vec<Arc<Cdc>>, CommitVersion), String>;

pub type LoaderReply = Box<dyn FnOnce(LoadedChunk) + Send>;

pub enum LoaderMessage {
	Fetch {
		from: CommitVersion,
		up_to: CommitVersion,
		budget: ByteSize,
		reply: LoaderReply,
	},
}

const MEMO_DEPTH: usize = 4;
const READ_CHUNK: u64 = 256;

struct MemoEntry {
	from: CommitVersion,
	advance_to: CommitVersion,
	items: Vec<Arc<Cdc>>,
}

#[derive(Clone, Default)]
pub struct LoaderMetrics {
	inner: Arc<LoaderMetricsInner>,
}

#[derive(Default)]
struct LoaderMetricsInner {
	loads: AtomicU64,
	memo_hits: AtomicU64,
	bytes_loaded: AtomicU64,
}

impl MetricsCollector for LoaderMetrics {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		out.push(MetricsSample::count("flow_loader", "loads", self.inner.loads.load(Ordering::Relaxed)));
		out.push(MetricsSample::count("flow_loader", "memo_hits", self.inner.memo_hits.load(Ordering::Relaxed)));
		out.push(MetricsSample::heap(
			"flow_loader",
			"bytes_loaded",
			ByteSize::from_bytes(self.inner.bytes_loaded.load(Ordering::Relaxed)),
		));
	}
}

pub struct LoaderActor {
	reader: CdcHotReader,
	metrics: LoaderMetrics,
}

pub struct LoaderState {
	memo: VecDeque<MemoEntry>,
}

impl LoaderActor {
	pub fn new(reader: CdcHotReader, metrics: LoaderMetrics) -> Self {
		Self {
			reader,
			metrics,
		}
	}

	fn serve(&self, state: &mut LoaderState, from: CommitVersion, up_to: CommitVersion, budget: ByteSize) -> LoadedChunk {
		if let Some(memo) = state.memo.iter().find(|m| m.from == from && m.advance_to <= up_to) {
			self.metrics.inner.memo_hits.fetch_add(1, Ordering::Relaxed);
			return Ok((memo.items.clone(), memo.advance_to));
		}

		let (items, advance_to) = self.load(from, up_to, budget)?;
		self.metrics.inner.loads.fetch_add(1, Ordering::Relaxed);
		self.metrics
			.inner
			.bytes_loaded
			.fetch_add(items.iter().map(|c| cdc_bytes(c)).sum::<u64>(), Ordering::Relaxed);
		state.memo.push_front(MemoEntry {
			from,
			advance_to,
			items: items.clone(),
		});
		state.memo.truncate(MEMO_DEPTH);
		Ok((items, advance_to))
	}

	fn load(
		&self,
		from: CommitVersion,
		up_to: CommitVersion,
		budget: ByteSize,
	) -> std::result::Result<(Vec<Arc<Cdc>>, CommitVersion), String> {
		let budget = budget.as_bytes().max(1);
		let mut items: Vec<Arc<Cdc>> = Vec::new();
		let mut taken = 0u64;
		let mut cursor = from;
		loop {
			let batch = self
				.reader
				.read_range(Bound::Excluded(cursor), Bound::Included(up_to), READ_CHUNK)
				.map_err(|e| format!("cdc catch-up read failed: {e:?}"))?;
			let exhausted = !batch.has_more;
			for cdc in batch.items {
				let bytes = cdc_bytes(&cdc);
				if !items.is_empty() && taken + bytes > budget {
					let advance_to = items.last().expect("non-empty").version;
					return Ok((items, advance_to));
				}
				taken += bytes;
				cursor = cdc.version;
				items.push(Arc::new(cdc));
			}
			if exhausted {
				return Ok((items, up_to));
			}
			if taken >= budget {
				let advance_to = items.last().map(|c| c.version).unwrap_or(up_to);
				return Ok((items, advance_to));
			}
		}
	}
}

impl Actor for LoaderActor {
	type State = LoaderState;
	type Message = LoaderMessage;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
		LoaderState {
			memo: VecDeque::new(),
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		match msg {
			LoaderMessage::Fetch {
				from,
				up_to,
				budget,
				reply,
			} => {
				let outcome = self.serve(state, from, up_to, budget);
				(reply)(outcome);
			}
		}
		Directive::Continue
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new()
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Mutex;

	use reifydb_cdc::storage::{CdcStore, CdcStorage, memory::MemoryCdcStorage};
	use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
	use reifydb_core::interface::cdc::SystemChange;
	use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock, pool::Pools};
	use reifydb_value::{util::cowvec::CowVec, value::datetime::DateTime};

	use super::*;

	fn cv(n: u64) -> CommitVersion {
		CommitVersion(n)
	}

	fn cdc(version: u64, payload: usize) -> Cdc {
		Cdc::new(
			cv(version),
			DateTime::default(),
			Vec::new(),
			vec![SystemChange::Insert {
				key: EncodedKey::new(vec![0xAB; 4]),
				post: EncodedRow(CowVec::new(vec![0u8; payload])),
			}],
		)
	}

	fn store_with(versions: impl IntoIterator<Item = u64>) -> CdcStore {
		let storage = MemoryCdcStorage::new();
		for v in versions {
			storage.write(&cdc(v, 100)).unwrap();
		}
		CdcStore::Memory(storage)
	}

	fn spawn(store: &CdcStore) -> (LoaderHandle, ActorSystem) {
		let system = ActorSystem::new(Pools::default(), Clock::Real);
		let handle = system.spawner().spawn_flow("test-loader", LoaderActor::new(store.hot_reader(), LoaderMetrics::default()));
		(handle, system)
	}

	fn fetch(handle: &LoaderHandle, from: u64, up_to: u64, budget: ByteSize) -> LoadedChunk {
		let slot: Arc<Mutex<Option<LoadedChunk>>> = Arc::new(Mutex::new(None));
		let out = slot.clone();
		handle.actor_ref()
			.send(LoaderMessage::Fetch {
				from: cv(from),
				up_to: cv(up_to),
				budget,
				reply: Box::new(move |chunk| {
					*out.lock().unwrap() = Some(chunk);
				}),
			})
			.map_err(|_| "loader mailbox closed")
			.expect("send fetch");
		let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
		loop {
			if let Some(chunk) = slot.lock().unwrap().take() {
				return chunk;
			}
			assert!(std::time::Instant::now() < deadline, "loader never replied");
			std::thread::sleep(std::time::Duration::from_millis(2));
		}
	}

	#[test]
	fn an_exhausted_range_advances_to_the_requested_bound() {
		// When the store has nothing above the last read entry, the flow must still advance its
		// cursor all the way to the bound it asked for, or versions with no CDC would be
		// re-requested forever.
		let store = store_with([2, 3]);
		let (handle, _system) = spawn(&store);
		let (items, advance_to) = fetch(&handle, 1, 9, ByteSize::from_mib(1)).expect("chunk");
		assert_eq!(items.iter().map(|c| c.version).collect::<Vec<_>>(), vec![cv(2), cv(3)]);
		assert_eq!(advance_to, cv(9), "an exhausted read must advance to up_to, not the last entry");
	}

	#[test]
	fn the_byte_budget_truncates_and_advances_only_to_the_last_served() {
		let store = store_with(1..=10);
		let (handle, _system) = spawn(&store);
		let one = cdc_bytes(&cdc(1, 100));
		let (items, advance_to) = fetch(&handle, 0, 10, ByteSize::from_bytes(one * 3)).expect("chunk");
		assert_eq!(items.len(), 3);
		assert_eq!(
			advance_to,
			cv(3),
			"a truncated load must advance exactly to the last served version so nothing is skipped"
		);
	}

	#[test]
	fn identical_requests_are_served_from_the_memo_without_a_second_read() {
		// The restart cohort case: N flows resume from the same checkpoint and issue the same
		// fetch. The disk read and decode must happen once; the rest are refcount clones. The
		// probe is destructive rather than counting: wipe the storage after the first fetch,
		// so an identical second fetch can only succeed if it never touches storage again.
		let storage = MemoryCdcStorage::new();
		for v in 1..=5 {
			storage.write(&cdc(v, 100)).unwrap();
		}
		let store = CdcStore::Memory(storage.clone());
		let (handle, _system) = spawn(&store);

		let first = fetch(&handle, 0, 5, ByteSize::from_mib(1)).expect("chunk");
		assert_eq!(first.0.len(), 5);

		storage.clear();

		let second = fetch(&handle, 0, 5, ByteSize::from_mib(1)).expect("chunk");
		assert_eq!(
			second.0.len(),
			5,
			"an identical fetch must be served from the memo; a storage re-read would find nothing"
		);
		assert_eq!(second.1, first.1);

		let miss = fetch(&handle, 2, 5, ByteSize::from_mib(1)).expect("chunk");
		assert!(
			miss.0.is_empty(),
			"a different cursor must go to storage, and the wiped storage proves it did"
		);
	}
}
