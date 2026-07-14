// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicUsize, Ordering},
};

use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
use reifydb_core::{
	common::CommitVersion,
	event::EventBus,
	interface::catalog::config::{ConfigKey, GetConfig},
	internal_err,
};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::{
		clock::{Clock, MockClock},
		rng::Rng,
	},
	pool::Pools,
	sync::{mutex::Mutex, waiter::WaiterHandle},
};
use reifydb_store_multi::MultiStore;
use reifydb_store_single::SingleStore;
use reifydb_transaction::{
	group::{GroupCommitApply, GroupCommitBegin, GroupCommitHandle, GroupCommitSubmission},
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

struct DefaultConfig;

impl GetConfig for DefaultConfig {
	fn get_config(&self, key: ConfigKey) -> Value {
		key.default_value()
	}
	fn get_config_at(&self, key: ConfigKey, _version: CommitVersion) -> Value {
		key.default_value()
	}
}

struct Harness {
	_actor_system: ActorSystem,
	spawner: reifydb_runtime::actor::system::ActorSpawner,
	begin: GroupCommitBegin,
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
		Rng::seeded(42),
		Arc::new(DefaultConfig),
	)
	.unwrap();
	let single = SingleTransaction::new(SingleStore::testing_memory(), bus.clone());

	let begin: GroupCommitBegin = Arc::new(move || {
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
		spawner,
		begin,
	}
}

fn key(name: &str) -> EncodedKey {
	EncodedKey::new(name.as_bytes().to_vec())
}

fn row(value: &str) -> EncodedRow {
	EncodedRow(CowVec::new(value.as_bytes().to_vec()))
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

fn write_submission(recorder: &Arc<Recorder>, index: usize, k: EncodedKey, v: EncodedRow) -> GroupCommitSubmission {
	GroupCommitSubmission {
		apply: Box::new(move |txn| txn.set(&k, v)),
		completion: recorder.completion(index),
	}
}

fn read_back(begin: &GroupCommitBegin, k: &EncodedKey) -> Option<Vec<u8>> {
	let mut txn = begin().expect("begin read-back transaction");
	let result = txn.get(k).expect("get").map(|row| row.row.to_vec());
	txn.rollback().expect("rollback read-back transaction");
	result
}

#[test]
fn grouped_submissions_share_one_version_in_arrival_order() {
	let h = harness();
	let handle = GroupCommitHandle::spawn(&h.spawner, h.begin.clone(), Duration::from_milliseconds(50).unwrap(), 16);

	let recorder = Recorder::new(3);
	let shared = key("shared");
	for i in 0..3 {
		let mut submission =
			write_submission(&recorder, i, key(&format!("grouped-{i}")), row(&format!("value-{i}")));
		let shared_key = shared.clone();
		let shared_row = row(&format!("writer-{i}"));
		let apply = submission.apply;
		submission.apply = Box::new(move |txn| {
			apply(txn)?;
			txn.set(&shared_key, shared_row)
		});
		handle.submit(submission);
	}
	recorder.wait();

	let versions = recorder.versions();
	assert_eq!(versions.len(), 3);
	let first = versions[0].1;
	assert!(first > CommitVersion(0));
	assert!(versions.iter().all(|(_, v)| *v == first), "all submissions must share one commit version");

	for i in 0..3 {
		assert_eq!(
			read_back(&h.begin, &key(&format!("grouped-{i}"))),
			Some(format!("value-{i}").into_bytes())
		);
	}
	assert_eq!(
		read_back(&h.begin, &shared),
		Some(b"writer-2".to_vec()),
		"last submission in arrival order must win on a shared key"
	);
}

#[test]
fn single_submission_commits_after_linger_expiry() {
	let h = harness();
	let handle = GroupCommitHandle::spawn(&h.spawner, h.begin.clone(), Duration::from_milliseconds(20).unwrap(), 16);

	let first = Recorder::new(1);
	handle.submit(write_submission(&first, 0, key("lone-1"), row("a")));
	first.wait();

	let second = Recorder::new(1);
	handle.submit(write_submission(&second, 0, key("lone-2"), row("b")));
	second.wait();

	let v1 = first.versions()[0].1;
	let v2 = second.versions()[0].1;
	assert!(v2 > v1, "submissions outside the linger window must land in separate versions");
}

#[test]
fn max_entries_flushes_before_linger_deadline() {
	let h = harness();
	let handle =
		GroupCommitHandle::spawn(&h.spawner, h.begin.clone(), Duration::from_seconds(3600).unwrap(), 3);

	let recorder = Recorder::new(3);
	for i in 0..3 {
		handle.submit(write_submission(&recorder, i, key(&format!("bound-{i}")), row("x")));
	}
	recorder.wait();

	let versions = recorder.versions();
	let first = versions[0].1;
	assert!(versions.iter().all(|(_, v)| *v == first));
}

#[test]
fn inline_handle_commits_each_submission_in_its_own_version() {
	let h = harness();
	let handle = GroupCommitHandle::inline(h.begin.clone());

	let recorder = Recorder::new(3);
	for i in 0..3 {
		handle.submit(write_submission(&recorder, i, key(&format!("inline-{i}")), row("y")));
	}
	recorder.wait();

	let mut versions: Vec<CommitVersion> = recorder.versions().iter().map(|(_, v)| *v).collect();
	let deduped: Vec<CommitVersion> = {
		let mut v = versions.clone();
		v.dedup();
		v
	};
	assert_eq!(deduped.len(), 3, "inline mode must not merge submissions: {versions:?}");
	versions.sort();
	assert!(versions.windows(2).all(|w| w[0] < w[1]));
}

#[test]
fn failing_apply_fails_the_whole_group_and_recovers() {
	let h = harness();
	let handle =
		GroupCommitHandle::spawn(&h.spawner, h.begin.clone(), Duration::from_seconds(3600).unwrap(), 3);

	let failures = Arc::new(AtomicUsize::new(0));
	let recorder = Recorder::new(3);

	let k0 = key("poisoned-0");
	let failures_0 = Arc::clone(&failures);
	let completion_recorder = Arc::clone(&recorder);
	let completion: Box<dyn FnOnce(Result<CommitVersion>) + Send> = Box::new(move |result| {
		if result.is_err() {
			failures_0.fetch_add(1, Ordering::SeqCst);
		}
		completion_recorder.results.lock().push((0, result));
		if completion_recorder.remaining.fetch_sub(1, Ordering::SeqCst) == 1 {
			completion_recorder.done.notify();
		}
	});
	let k0_apply = k0.clone();
	handle.submit(GroupCommitSubmission {
		apply: Box::new(move |txn| txn.set(&k0_apply, row("should-roll-back"))),
		completion,
	});

	for i in 1..3 {
		let failures_i = Arc::clone(&failures);
		let recorder_i = Arc::clone(&recorder);
		let apply: GroupCommitApply = if i == 1 {
			Box::new(move |_txn| internal_err!("boom"))
		} else {
			let k = key("poisoned-2");
			Box::new(move |txn| txn.set(&k, row("also-rolled-back")))
		};
		handle.submit(GroupCommitSubmission {
			apply,
			completion: Box::new(move |result: Result<CommitVersion>| {
				if result.is_err() {
					failures_i.fetch_add(1, Ordering::SeqCst);
				}
				recorder_i.results.lock().push((i, result));
				if recorder_i.remaining.fetch_sub(1, Ordering::SeqCst) == 1 {
					recorder_i.done.notify();
				}
			}),
		});
	}
	recorder.wait();

	assert_eq!(failures.load(Ordering::SeqCst), 3, "every member of a failed group must observe the error");
	assert_eq!(read_back(&h.begin, &k0), None, "writes of a failed group must be rolled back");

	let retry = Recorder::new(3);
	for i in 0..3 {
		retry.submit_ok(&handle, i);
	}
	retry.wait();
	let versions = retry.versions();
	let first = versions[0].1;
	assert!(versions.iter().all(|(_, v)| *v == first), "coordinator must keep committing after a failed group");
}

impl Recorder {
	fn submit_ok(self: &Arc<Self>, handle: &GroupCommitHandle, index: usize) {
		handle.submit(write_submission(self, index, key(&format!("retry-{index}")), row("z")));
	}
}

#[test]
fn shutdown_flushes_pending_group() {
	let h = harness();
	let handle =
		GroupCommitHandle::spawn(&h.spawner, h.begin.clone(), Duration::from_seconds(3600).unwrap(), 16);

	let recorder = Recorder::new(2);
	for i in 0..2 {
		handle.submit(write_submission(&recorder, i, key(&format!("drain-{i}")), row("d")));
	}
	handle.shutdown();
	recorder.wait();

	let versions = recorder.versions();
	let first = versions[0].1;
	assert!(versions.iter().all(|(_, v)| *v == first));
	assert_eq!(read_back(&h.begin, &key("drain-0")), Some(b"d".to_vec()));
	assert_eq!(read_back(&h.begin, &key("drain-1")), Some(b"d".to_vec()));

	let post_shutdown = Recorder::new(1);
	handle.submit(GroupCommitSubmission {
		apply: Box::new(|_txn| Ok(())),
		completion: {
			let recorder = Arc::clone(&post_shutdown);
			Box::new(move |result| {
				assert!(result.is_err(), "submissions after shutdown must fail loudly");
				recorder.results.lock().push((0, Err(result.unwrap_err())));
				recorder.done.notify();
			})
		},
	});
	assert!(post_shutdown.done.wait_timeout(Duration::from_seconds(10).unwrap()));
}

#[test]
fn error_is_fanned_out_as_distinct_clones() {
	let h = harness();
	let handle = GroupCommitHandle::inline(h.begin.clone());

	let received: Arc<Mutex<Vec<Error>>> = Arc::new(Mutex::new(Vec::new()));
	let received_completion = Arc::clone(&received);
	let done = Arc::new(WaiterHandle::new());
	let done_completion = Arc::clone(&done);
	handle.submit(GroupCommitSubmission {
		apply: Box::new(|_txn| internal_err!("inline failure")),
		completion: Box::new(move |result| {
			received_completion.lock().push(result.unwrap_err());
			done_completion.notify();
		}),
	});
	assert!(done.wait_timeout(Duration::from_seconds(10).unwrap()));
	let received = received.lock();
	assert_eq!(received.len(), 1);
	assert!(format!("{:?}", received[0]).contains("inline failure"));
}
