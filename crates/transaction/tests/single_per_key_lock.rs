// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	sync::{Arc, Barrier},
	thread,
};

use reifydb_core::{
	encoded::{key::EncodedKey, shape::RowShape},
	event::EventBus,
};
use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock, pool::Pools};
use reifydb_store_single::SingleStore;
use reifydb_transaction::single::SingleTransaction;
use reifydb_value::value::value_type::ValueType;

fn u64_shape() -> RowShape {
	RowShape::testing(&[ValueType::Uint8])
}

// The single-version per-key write lock is the ONLY thing that serializes concurrent
// read-modify-write on one key: the single-version path has no conflict detector (its module
// doc states "last-writer-wins semantics between concurrent writers"). The row-number /
// sequence generator depends on exactly this serialization - it reads the counter, adds the
// batch size, and writes it back while holding the key's write lock, so two concurrent
// allocators can never hand out the same id.
//
// The bug this guards: the per-key lock is created lazily, and `get_or_create_lock` did a
// non-atomic get-then-insert. For a freshly-created key (a just-created table's sequence),
// concurrent first-callers each created and kept a DIFFERENT lock instance, so their write
// guards did not exclude each other. Their read-modify-write ran in parallel and lost
// updates - in the real system that meant two rows getting the same RowNumber (the same
// encoded key), so one committed table row was silently overwritten and vanished from reads.
//
// Each round uses a brand-new key and releases N threads through a barrier so they all reach
// that key's first lock acquisition at once. If the lock serializes them the counter is
// exactly N; any lost update (two threads reading the same value) makes it less than N. Many
// rounds make the otherwise-flaky first-access race reliably observable.
#[test]
fn concurrent_read_modify_write_on_a_fresh_key_is_serialized() {
	let actor_system = ActorSystem::new(Pools::default(), Clock::Real);
	let spawner = actor_system.spawner();
	let bus = EventBus::new(&spawner);
	let txn = SingleTransaction::new(SingleStore::testing_memory(), bus);

	const THREADS: u64 = 8;
	const ROUNDS: u64 = 100;

	for round in 0..ROUNDS {
		let key = EncodedKey::new(format!("counter:{round}").into_bytes());
		let barrier = Arc::new(Barrier::new(THREADS as usize));

		thread::scope(|scope| {
			for _ in 0..THREADS {
				let txn = txn.clone();
				let key = key.clone();
				let barrier = Arc::clone(&barrier);
				scope.spawn(move || {
					let shape = u64_shape();
					barrier.wait();
					txn.with_command([&key], |tx| {
						let current = match tx.get(&key)? {
							Some(existing) => shape.get_u64(&existing.row, 0),
							None => 0,
						};
						let mut row = shape.allocate();
						shape.set_u64(&mut row, 0, current + 1);
						tx.set(&key, row)
					})
					.unwrap();
				});
			}
		});

		let shape = u64_shape();
		let total = txn
			.with_command([&key], |tx| {
				Ok(shape.get_u64(
					&tx.get(&key)?.expect("counter key must exist after the round").row,
					0,
				))
			})
			.unwrap();

		assert_eq!(
			total,
			THREADS,
			"round {round}: {THREADS} concurrent increments of a fresh key produced {total}; \
			 {} update(s) lost - the per-key write lock did not serialize them",
			THREADS - total
		);
	}
}
