// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicUsize, Ordering},
};

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{common::CommitVersion, event::EventBus, internal_err, testing::ProfileConfig};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::{
		clock::{Clock, MockClock},
		rng::Rng,
	},
	pool::Pools,
	sync::{mutex::Mutex, waiter::WaiterHandle},
	version_epoch::VersionEpoch,
};
use reifydb_store_multi::MultiStore;
use reifydb_store_single::SingleStore;
use reifydb_transaction::{
	commit::{CommitBegin, CommitHandle, CommitSubmission},
	interceptor::interceptors::Interceptors,
	multi::transaction::MultiTransaction,
	single::SingleTransaction,
	transaction::command::CommandTransaction,
};
use reifydb_value::{
	Result,
	error::Error,
	util::cowvec::CowVec,
	value::{Value, duration::Duration, identity::IdentityId},
};

struct Harness {
	_actor_system: ActorSystem,
	begin: CommitBegin,
}

fn harness() -> Harness {
	let multi_store = MultiStore::testing_memory();
	let single_store = SingleStore::testing_memory();
	let actor_system = ActorSystem::new(Pools::default(), Clock::Real);
	let spawner = actor_system.spawner();
	let bus = EventBus::new(&spawner);
	let multi = MultiTransaction::new(
		multi_store,
		SingleTransaction::new(single_store, bus.clone()),
		bus.clone(),
		spawner.clone(),
		Clock::Mock(MockClock::from_millis(1000)),
		VersionEpoch::new(),
		Rng::seeded(42),
		Arc::new(ProfileConfig),
	)
	.unwrap();
	let single = SingleTransaction::new(SingleStore::testing_memory(), bus.clone());

	let begin: CommitBegin = Arc::new(move || {
		CommandTransaction::new(
			multi.clone(),
			single.clone(),
			bus.clone(),
			Interceptors::new(),
			IdentityId::system(),
			Clock::Real,
		)
	});

	Harness {
		_actor_system: actor_system,
		begin,
	}
}

fn key(name: &str) -> EncodedKey {
	EncodedKey::new(name.as_bytes())
}

fn encoded_bytes(value: &str) -> EncodedBytes {
	EncodedBytes(CowVec::new(value.as_bytes().to_vec()))
}

struct Recorder {
	results: Mutex<Vec<(usize, Result<CommitVersion>)>>,
	remaining: AtomicUsize,
	done: WaiterHandle,
}

impl Recorder {
	fn new(expected: usize) -> Arc<Self> {
		Arc::new(Self {
			results: Mutex::new(Vec::new()),
			remaining: AtomicUsize::new(expected),
			done: WaiterHandle::new(),
		})
	}

	fn completion(self: &Arc<Self>, index: usize) -> Box<dyn FnOnce(Result<CommitVersion>) + Send> {
		let recorder = Arc::clone(self);
		Box::new(move |result| {
			recorder.results.lock().push((index, result));
			if recorder.remaining.fetch_sub(1, Ordering::SeqCst) == 1 {
				recorder.done.notify();
			}
		})
	}

	fn wait(&self) {
		assert!(self.done.wait_timeout(Duration::from_seconds(10).unwrap()), "completions timed out");
	}

	fn versions(&self) -> Vec<(usize, CommitVersion)> {
		self.results
			.lock()
			.iter()
			.map(|(i, r)| (*i, *r.as_ref().expect("expected successful commit")))
			.collect()
	}
}

fn write_submission(recorder: &Arc<Recorder>, index: usize, k: EncodedKey, v: EncodedBytes) -> CommitSubmission {
	CommitSubmission {
		apply: Box::new(move |txn| txn.set(&k, v)),
		completion: recorder.completion(index),
	}
}

fn read_back(begin: &CommitBegin, k: &EncodedKey) -> Option<Vec<u8>> {
	let mut txn = begin().expect("begin read-back transaction");
	let result = txn.get(k).expect("get").map(|bytes| bytes.bytes.to_vec());
	txn.rollback().expect("rollback read-back transaction");
	result
}

#[test]
fn each_submission_commits_in_its_own_version() {
	// Nothing merges submissions, so one must never be able to roll back another's writes.
	let h = harness();
	let handle = CommitHandle::new(h.begin.clone());

	let recorder = Recorder::new(3);
	for i in 0..3 {
		handle.submit(write_submission(&recorder, i, key(&format!("inline-{i}")), encoded_bytes("y")));
	}
	recorder.wait();

	let mut versions: Vec<CommitVersion> = recorder.versions().iter().map(|(_, v)| *v).collect();
	let deduped: Vec<CommitVersion> = {
		let mut v = versions.clone();
		v.dedup();
		v
	};
	assert_eq!(deduped.len(), 3, "submissions must never be merged: {versions:?}");
	versions.sort();
	assert!(versions.windows(2).all(|w| w[0] < w[1]));
}

#[test]
fn a_failing_apply_rolls_back_its_own_writes_and_the_handle_keeps_committing() {
	// A failed apply must leave nothing behind, otherwise one bad submission poisons every later commit.
	let h = harness();
	let handle = CommitHandle::new(h.begin.clone());

	let failures = Arc::new(AtomicUsize::new(0));
	let recorder = Recorder::new(2);

	let k0 = key("poisoned-0");
	let k0_apply = k0.clone();
	let failures_0 = Arc::clone(&failures);
	let recorder_0 = Arc::clone(&recorder);
	handle.submit(CommitSubmission {
		apply: Box::new(move |txn| {
			txn.set(&k0_apply, encoded_bytes("should-roll-back"))?;
			internal_err!("boom")
		}),
		completion: Box::new(move |result: Result<CommitVersion>| {
			if result.is_err() {
				failures_0.fetch_add(1, Ordering::SeqCst);
			}
			recorder_0.results.lock().push((0, result));
			if recorder_0.remaining.fetch_sub(1, Ordering::SeqCst) == 1 {
				recorder_0.done.notify();
			}
		}),
	});

	handle.submit(write_submission(&recorder, 1, key("survivor"), encoded_bytes("ok")));
	recorder.wait();

	assert_eq!(failures.load(Ordering::SeqCst), 1, "the failing submission must observe the error");
	assert_eq!(read_back(&h.begin, &k0), None, "the writes of a failed apply must be rolled back");
	assert_eq!(
		read_back(&h.begin, &key("survivor")),
		Some(b"ok".to_vec()),
		"a later submission must still commit after a failed one"
	);
}

#[test]
fn a_failed_apply_reports_its_error_to_its_own_completion() {
	let h = harness();
	let handle = CommitHandle::new(h.begin.clone());

	let received: Arc<Mutex<Vec<Error>>> = Arc::new(Mutex::new(Vec::new()));
	let received_completion = Arc::clone(&received);
	let done = Arc::new(WaiterHandle::new());
	let done_completion = Arc::clone(&done);
	handle.submit(CommitSubmission {
		apply: Box::new(|_txn| internal_err!("apply failure")),
		completion: Box::new(move |result| {
			received_completion.lock().push(result.unwrap_err());
			done_completion.notify();
		}),
	});
	assert!(done.wait_timeout(Duration::from_seconds(10).unwrap()));
	let received = received.lock();
	assert_eq!(received.len(), 1);
	assert!(format!("{:?}", received[0]).contains("apply failure"));
}
