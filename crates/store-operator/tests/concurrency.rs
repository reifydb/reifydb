// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Writers racing the flush actor. The three-layer read path (live buffer, in-flight batch, sqlite) exists
//! precisely so that a reader crossing a flush boundary never observes a value the writer has already
//! replaced; a testscript runs one command at a time and so cannot put a read inside a flush.

use std::{
	sync::{
		Arc, Barrier,
		atomic::{AtomicBool, Ordering},
	},
	thread,
};

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_state::{GroupId, Keyspace, OperatorStateKey},
};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::clock::Clock,
	pool::{PoolConfig, Pools},
};
use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard};
use reifydb_store_operator::{
	config::{OperatorPersistentConfig, OperatorStoreConfig},
	store::OperatorStore,
	tier::read::OperatorReadBufferConfig,
};
use reifydb_value::value::duration::Duration;

const OP: OperatorId = OperatorId(1);

const GROUP: GroupId = GroupId(1);

const WRITERS: u64 = 8;

const KEYS_PER_WRITER: u64 = 64;

const FLUSH_ROUNDS: usize = 16;

fn store() -> (OperatorStore, SqliteTempPathGuard) {
	// a real actor system is what lets the flusher run on its own thread while the test writes
	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let spawner = actor_system.spawner();
	std::mem::forget(actor_system);
	let (config, guard) = SqliteConfig::in_memory();
	let store = OperatorStore::standard(OperatorStoreConfig {
		commit: Default::default(),
		persistent: Some(OperatorPersistentConfig::sqlite(config).flush_interval(Duration::from_hours_const(1))),
		read: Some(OperatorReadBufferConfig::default()),
		dictionary: None,
		spawner,
		clock: Clock::Real,
	});
	(store, guard)
}

fn key(suffix: u64) -> EncodedKey {
	OperatorStateKey::inner_encoded(GROUP, Keyspace::ACCUMULATOR, suffix.to_be_bytes()).as_encoded().clone()
}

fn row(body: &str) -> EncodedPodRow {
	EncodedPodRow::new(body.as_bytes())
}

fn version(round: usize) -> String {
	format!("v{round}")
}

fn parse_version(body: &str) -> usize {
	body.strip_prefix('v').and_then(|round| round.parse().ok()).expect("test bodies are versioned")
}

fn body(store: &OperatorStore, operator: OperatorId, suffix: u64) -> Option<String> {
	store.get(operator, &key(suffix))
		.map(|row| String::from_utf8(row.body().to_vec()).expect("test bodies are utf8"))
}

#[test]
fn every_write_from_every_concurrent_writer_is_readable_after_the_flush() {
	// one lost write means an operator silently resumes on state that is missing a key it wrote
	let (store, _guard) = store();

	let writers: Vec<_> = (1..=WRITERS)
		.map(|writer| {
			let store = store.clone();
			thread::spawn(move || {
				for index in 0..KEYS_PER_WRITER {
					store.set(OperatorId(writer), key(index), row(&format!("{writer}-{index}")));
				}
			})
		})
		.collect();
	for writer in writers {
		writer.join().expect("a writing thread must not panic");
	}

	assert!(store.flush_pending_blocking(), "the racing writes must all reach the flusher");

	for writer in 1..=WRITERS {
		for index in 0..KEYS_PER_WRITER {
			assert_eq!(
				body(&store, OperatorId(writer), index).as_deref(),
				Some(format!("{writer}-{index}").as_str()),
				"writer {writer} lost key {index}; concurrent writers must not overwrite each other's \
				 entries in the shared buffer"
			);
		}
	}

	let persistent = store.persistent().expect("the sqlite tier is configured");
	for writer in 1..=WRITERS {
		for index in 0..KEYS_PER_WRITER {
			assert!(
				persistent.get(OperatorId(writer), &key(index)).is_some(),
				"writer {writer} key {index} is served from memory but never reached sqlite, so it is \
				 lost on the next restart"
			);
		}
	}
}

#[test]
fn a_read_racing_the_flush_never_sees_the_value_the_flush_is_replacing() {
	// between take_for_flush and complete_flush the row is in neither the live batch nor sqlite yet
	let (store, _guard) = store();
	let persistent = store.persistent().expect("the sqlite tier is configured");
	// only the flusher may advance sqlite past this seed, otherwise a torn two-tier read reports a false regression
	for index in 0..KEYS_PER_WRITER {
		persistent.set(OP, key(index), row(&version(0)));
	}
	for index in 0..KEYS_PER_WRITER {
		store.set(OP, key(index), row(&version(1)));
	}

	let stop = Arc::new(AtomicBool::new(false));
	// the reader must already be spinning before the first flush, otherwise it wakes after stop and reads nothing
	let barrier = Arc::new(Barrier::new(2));
	let reader = {
		let store = store.clone();
		let stop = Arc::clone(&stop);
		let barrier = Arc::clone(&barrier);
		thread::spawn(move || {
			let mut highest = vec![0usize; KEYS_PER_WRITER as usize];
			let mut regressions = 0usize;
			let mut missing = 0usize;
			let mut reads = 0usize;
			barrier.wait();
			while !stop.load(Ordering::Acquire) {
				for index in 0..KEYS_PER_WRITER {
					reads += 1;
					match body(&store, OP, index).as_deref() {
						Some(observed) => {
							let observed = parse_version(observed);
							let seen = &mut highest[index as usize];
							if observed < *seen {
								regressions += 1;
							} else {
								*seen = observed;
							}
						}
						None => missing += 1,
					}
				}
			}
			(reads, regressions, missing)
		})
	};

	barrier.wait();
	for round in 2..=FLUSH_ROUNDS {
		assert!(store.flush_pending_blocking(), "the buffered version must reach the flusher");
		for index in 0..KEYS_PER_WRITER {
			store.set(OP, key(index), row(&version(round)));
		}
	}
	assert!(store.flush_pending_blocking(), "the last buffered version must reach the flusher");
	stop.store(true, Ordering::Release);
	let (reads, regressions, missing) = reader.join().expect("the reading thread must not panic");

	assert!(reads > 0, "the reader must have observed something, otherwise this test asserts nothing");
	assert_eq!(
		regressions, 0,
		"a read that falls through to sqlite while the flush is in flight serves the value the writer already \
		 replaced, and the operator computes on state it has overwritten"
	);
	assert_eq!(
		missing, 0,
		"a row taken for flushing must stay readable until the flush completes, otherwise it vanishes under a \
		 live reader for the width of one sqlite transaction"
	);
}

#[test]
fn a_write_that_lands_after_a_drop_marker_survives_the_drop() {
	// the marker runs first at flush time, so whatever the buffer still holds must outlive it
	let (store, _guard) = store();
	let persistent = store.persistent().expect("the sqlite tier is configured");
	for index in 0..KEYS_PER_WRITER {
		persistent.set(OP, key(index), row("pre-drop"));
	}
	store.set(OP, key(1_000), row("pre-drop-buffered"));

	let dropper = {
		let store = store.clone();
		thread::spawn(move || {
			store.drop_operator_state(OP);
			store.set(OP, key(2_000), row("post-drop"));
		})
	};

	for _ in 0..16 {
		assert!(store.flush_pending_blocking(), "a flush racing the drop must still complete");
	}
	dropper.join().expect("the dropping thread must not panic");
	assert!(store.flush_pending_blocking(), "the marker and the later write must reach the flusher");

	assert_eq!(
		body(&store, OP, 2_000).as_deref(),
		Some("post-drop"),
		"a drop that clears state the flusher is still writing, or one whose marker runs after the writes, \
		 deletes this key again and the recreated operator starts from empty state"
	);
	for index in 0..KEYS_PER_WRITER {
		assert!(
			persistent.get(OP, &key(index)).is_none(),
			"the marker must erase every row it masked no matter when the flush interleaved with it"
		);
	}
	assert!(
		persistent.get(OP, &key(1_000)).is_none(),
		"a write the drop erased must never be replayed into sqlite behind the drop"
	);
	assert_eq!(
		persistent.get(OP, &key(2_000)).map(|row| row.body().to_vec()),
		Some(b"post-drop".to_vec()),
		"the post-drop write must be durable too, not merely readable from memory"
	);
}

#[test]
fn interleaved_writes_and_removals_converge_on_the_last_write() {
	// a tombstone that outlives the write after it hides a live key forever
	let (store, _guard) = store();

	let churners: Vec<_> = (0..WRITERS)
		.map(|_| {
			let store = store.clone();
			thread::spawn(move || {
				for _ in 0..KEYS_PER_WRITER {
					store.set(OP, key(1), row("churn"));
					store.remove(OP, &key(1));
				}
			})
		})
		.collect();
	for churner in churners {
		churner.join().expect("a churning thread must not panic");
	}

	assert!(store.flush_pending_blocking(), "the churn must be drained before the final write");
	assert!(store.get(OP, &key(1)).is_none(), "every thread ended on a removal, so the key must read as missing");

	store.set(OP, key(1), row("final"));
	assert!(store.flush_pending_blocking(), "the final write must reach the flusher");

	assert_eq!(
		body(&store, OP, 1).as_deref(),
		Some("final"),
		"the last write must win over every tombstone that preceded it"
	);
	let persistent = store.persistent().expect("the sqlite tier is configured");
	assert_eq!(
		persistent.get(OP, &key(1)).map(|row| row.body().to_vec()),
		Some(b"final".to_vec()),
		"a delete flushed after the write it follows would leave sqlite empty while memory reports the key"
	);
}
