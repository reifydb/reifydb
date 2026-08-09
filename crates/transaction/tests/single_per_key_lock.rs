// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	sync::{Arc, Barrier},
	thread,
};

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::shape::{RowFamily, RowShape},
};
use reifydb_core::event::EventBus;
use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock, pool::Pools};
use reifydb_store_single::SingleStore;
use reifydb_transaction::single::SingleTransaction;
use reifydb_value::value::value_type::ValueType;

fn u64_shape() -> RowShape {
	RowShape::testing(RowFamily::Pod, &[ValueType::Uint8])
}

#[test]
fn concurrent_read_modify_write_on_a_fresh_key_is_serialized() {
	// The single-version path has no conflict detector, so this lock is the only thing keeping the
	// row-number generator from handing two rows the same id and silently overwriting one.
	// Every round uses a fresh key and a barrier so all threads race its very first lock creation,
	// which is where a non-atomic get-then-insert hands each of them a different lock.
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
							Some(existing) => shape.get::<u64>(&existing.bytes, 0),
							None => 0,
						};
						let mut row = shape.allocate_pod();
						shape.set::<u64>(&mut row, 0, current + 1);
						tx.set(&key, row.freeze())
					})
					.unwrap();
				});
			}
		});

		let shape = u64_shape();
		let total = txn
			.with_command([&key], |tx| {
				Ok(shape.get::<u64>(
					&tx.get(&key)?.expect("counter key must exist after the round").bytes,
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
