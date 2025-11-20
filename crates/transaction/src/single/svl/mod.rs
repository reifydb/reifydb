// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::{Arc, RwLock};

use crossbeam_skiplist::SkipMap;
use parking_lot::RwLock as ParkingRwLock;
use reifydb_core::{
	CowVec, EncodedKey,
	delta::Delta,
	event::EventBus,
	interface::{SingleVersionTransaction, SingleVersionValues, WithEventBus},
	log_timed_trace,
	value::encoded::EncodedValues,
};
use reifydb_store_transaction::TransactionStore;
use reifydb_type::util::hex;

mod read;
mod write;

pub use read::SvlQueryTransaction;
use write::KeyWriteLock;
pub use write::SvlCommandTransaction;

#[derive(Clone)]
pub struct TransactionSvl {
	inner: Arc<TransactionSvlInner>,
}

struct TransactionSvlInner {
	store: RwLock<TransactionStore>,
	event_bus: EventBus,
	key_locks: SkipMap<EncodedKey, Arc<ParkingRwLock<()>>>,
}

impl TransactionSvlInner {
	fn get_or_create_lock(&self, key: &EncodedKey) -> Arc<ParkingRwLock<()>> {
		// Check if lock exists
		if let Some(entry) = self.key_locks.get(key) {
			return entry.value().clone();
		}

		// Create new lock
		let lock = Arc::new(ParkingRwLock::new(()));
		self.key_locks.insert(key.clone(), lock.clone());
		lock
	}
}

impl TransactionSvl {
	pub fn new(store: TransactionStore, event_bus: EventBus) -> Self {
		Self {
			inner: Arc::new(TransactionSvlInner {
				store: RwLock::new(store),
				event_bus,
				key_locks: SkipMap::new(),
			}),
		}
	}
}

impl WithEventBus for TransactionSvl {
	fn event_bus(&self) -> &EventBus {
		&self.inner.event_bus
	}
}

impl SingleVersionTransaction for TransactionSvl {
	type Query<'a> = SvlQueryTransaction<'a>;
	type Command<'a> = SvlCommandTransaction<'a>;

	fn begin_query<'a, I>(&self, keys: I) -> crate::Result<Self::Query<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey>,
	{
		let mut keys_vec: Vec<EncodedKey> = keys.into_iter().cloned().collect();
		assert!(
			!keys_vec.is_empty(),
			"SVL transactions must declare keys upfront - empty keysets are not allowed"
		);

		// Sort keys to establish consistent lock ordering and prevent deadlocks
		keys_vec.sort();

		// Acquire read locks on all keys in sorted order
		let mut locks = Vec::new();
		for key in &keys_vec {
			let arc = self.inner.get_or_create_lock(key);
			let key_hex = hex::encode(&key);
			let lock = log_timed_trace!("SVL read lock acquisition for key {key_hex}", {
				read::KeyReadLock::new(arc, |arc_ref| arc_ref.read())
			});
			locks.push(lock);
		}

		Ok(SvlQueryTransaction {
			inner: &self.inner,
			keys: keys_vec,
			_key_locks: locks,
		})
	}

	fn begin_command<'a, I>(&self, keys: I) -> crate::Result<Self::Command<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey>,
	{
		let mut keys_vec: Vec<EncodedKey> = keys.into_iter().cloned().collect();
		assert!(
			!keys_vec.is_empty(),
			"SVL transactions must declare keys upfront - empty keysets are not allowed"
		);

		// Sort keys to establish consistent lock ordering and prevent deadlocks
		keys_vec.sort();

		// Acquire write locks on all keys in sorted order
		let mut locks = Vec::new();
		for key in &keys_vec {
			let arc = self.inner.get_or_create_lock(key);
			let key_hex = hex::encode(&key);
			let lock = log_timed_trace!("SVL write lock acquisition for key {key_hex}", {
				KeyWriteLock::new(arc, |arc_ref| arc_ref.write())
			});
			locks.push(lock);
		}

		Ok(SvlCommandTransaction::new(&self.inner, keys_vec, locks))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{SingleVersionCommandTransaction, SingleVersionQueryTransaction};

	use super::*;

	fn make_key(s: &str) -> EncodedKey {
		EncodedKey(CowVec::new(s.as_bytes().to_vec()))
	}

	fn make_value(s: &str) -> EncodedValues {
		EncodedValues(CowVec::new(s.as_bytes().to_vec()))
	}

	fn create_test_svl() -> TransactionSvl {
		TransactionSvl::new(TransactionStore::testing_memory(), EventBus::default())
	}

	#[test]
	fn test_allowed_key_query() {
		let svl = create_test_svl();
		let key = make_key("test_key");

		// Start scoped query with the key
		let mut tx = svl.begin_query(vec![&key]).unwrap();

		// Should be able to get the key
		let result = tx.get(&key);
		assert!(result.is_ok());
	}

	#[test]
	fn test_disallowed_key_query() {
		let svl = create_test_svl();
		let key1 = make_key("allowed");
		let key2 = make_key("disallowed");

		// Start scoped query with only key1
		let mut tx = svl.begin_query(vec![&key1]).unwrap();

		// Should succeed for key1
		assert!(tx.get(&key1).is_ok());

		// Should fail for key2
		let result = tx.get(&key2);
		assert!(result.is_err());
		let err = result.unwrap_err();
		assert_eq!(err.0.code, "TXN_010");
	}

	#[test]
	#[should_panic(expected = "SVL transactions must declare keys upfront - empty keysets are not allowed")]
	fn test_empty_keyset_query_panics() {
		let svl = create_test_svl();

		// Should panic when trying to create transaction with empty keys
		let _tx = svl.begin_query(std::iter::empty());
	}

	#[test]
	#[should_panic(expected = "SVL transactions must declare keys upfront - empty keysets are not allowed")]
	fn test_empty_keyset_command_panics() {
		let svl = create_test_svl();

		// Should panic when trying to create transaction with empty keys
		let _tx = svl.begin_command(std::iter::empty());
	}

	#[test]
	fn test_allowed_key_command() {
		let svl = create_test_svl();
		let key = make_key("test_key");
		let value = make_value("test_value");

		// Start scoped command with the key
		let mut tx = svl.begin_command(vec![&key]).unwrap();

		// Should be able to set and get the key
		assert!(tx.set(&key, value.clone()).is_ok());
		assert!(tx.get(&key).is_ok());
		assert!(tx.commit().is_ok());
	}

	#[test]
	fn test_disallowed_key_command() {
		let svl = create_test_svl();
		let key1 = make_key("allowed");
		let key2 = make_key("disallowed");
		let value = make_value("test_value");

		// Start scoped command with only key1
		let mut tx = svl.begin_command(vec![&key1]).unwrap();

		// Should succeed for key1
		assert!(tx.set(&key1, value.clone()).is_ok());

		// Should fail for key2
		let result = tx.set(&key2, value);
		assert!(result.is_err());
		let err = result.unwrap_err();
		assert_eq!(err.0.code, "TXN_010");
	}

	#[test]
	fn test_command_commit_with_valid_keys() {
		let svl = create_test_svl();
		let key1 = make_key("key1");
		let key2 = make_key("key2");
		let value1 = make_value("value1");
		let value2 = make_value("value2");

		// Write with scoped transaction
		{
			let mut tx = svl.begin_command(vec![&key1, &key2]).unwrap();
			tx.set(&key1, value1.clone()).unwrap();
			tx.set(&key2, value2.clone()).unwrap();
			tx.commit().unwrap();
		}

		// Verify with query
		{
			let mut tx = svl.begin_query(vec![&key1, &key2]).unwrap();
			let result1 = tx.get(&key1).unwrap();
			let result2 = tx.get(&key2).unwrap();
			assert!(result1.is_some());
			assert!(result2.is_some());
			assert_eq!(result1.unwrap().values, value1);
			assert_eq!(result2.unwrap().values, value2);
		}
	}

	#[test]
	fn test_rollback_with_scoped_keys() {
		let svl = create_test_svl();
		let key = make_key("test_key");
		let value = make_value("test_value");

		// Start transaction and rollback
		{
			let mut tx = svl.begin_command(vec![&key]).unwrap();
			tx.set(&key, value).unwrap();
			tx.rollback().unwrap();
		}

		// Verify nothing was committed
		{
			let mut tx = svl.begin_query(vec![&key]).unwrap();
			let result = tx.get(&key).unwrap();
			assert!(result.is_none());
		}
	}

	#[test]
	fn test_concurrent_reads() {
		use std::{sync::Arc, thread};

		let svl = Arc::new(create_test_svl());
		let key = make_key("shared_key");
		let value = make_value("shared_value");

		// Write initial value
		{
			let mut tx = svl.begin_command(vec![&key]).unwrap();
			tx.set(&key, value.clone()).unwrap();
			tx.commit().unwrap();
		}

		// Spawn multiple readers
		let mut handles = vec![];
		for _ in 0..5 {
			let svl_clone = Arc::clone(&svl);
			let key_clone = key.clone();
			let value_clone = value.clone();

			let handle = thread::spawn(move || {
				let mut tx = svl_clone.begin_query(vec![&key_clone]).unwrap();
				let result = tx.get(&key_clone).unwrap();
				assert!(result.is_some());
				assert_eq!(result.unwrap().values, value_clone);
			});
			handles.push(handle);
		}

		// Wait for all threads
		for handle in handles {
			handle.join().unwrap();
		}
	}

	#[test]
	fn test_concurrent_writers_disjoint_keys() {
		use std::{sync::Arc, thread};

		let svl = Arc::new(create_test_svl());

		// Spawn multiple writers with disjoint keys
		let mut handles = vec![];
		for i in 0..5 {
			let svl_clone = Arc::clone(&svl);
			let key = make_key(&format!("key_{}", i));
			let value = make_value(&format!("value_{}", i));

			let handle = thread::spawn(move || {
				let mut tx = svl_clone.begin_command(vec![&key]).unwrap();
				tx.set(&key, value).unwrap();
				tx.commit().unwrap();
			});
			handles.push(handle);
		}

		// Wait for all threads
		for handle in handles {
			handle.join().unwrap();
		}

		// Verify all values were written
		for i in 0..5 {
			let key = make_key(&format!("key_{}", i));
			let expected_value = make_value(&format!("value_{}", i));

			let mut tx = svl.begin_query(vec![&key]).unwrap();
			let result = tx.get(&key).unwrap();
			assert!(result.is_some());
			assert_eq!(result.unwrap().values, expected_value);
		}
	}

	#[test]
	fn test_concurrent_readers_and_writer() {
		use std::{sync::Arc, thread};

		let svl = Arc::new(create_test_svl());
		let key1 = make_key("key1");
		let key2 = make_key("key2");
		let value1 = make_value("value1");
		let value2 = make_value("value2");

		// Write initial values
		{
			let mut tx = svl.begin_command(vec![&key1, &key2]).unwrap();
			tx.set(&key1, value1.clone()).unwrap();
			tx.set(&key2, value2.clone()).unwrap();
			tx.commit().unwrap();
		}

		// Spawn readers for key1
		let mut handles = vec![];
		for _ in 0..3 {
			let svl_clone = Arc::clone(&svl);
			let key_clone = key1.clone();
			let value_clone = value1.clone();

			let handle = thread::spawn(move || {
				let mut tx = svl_clone.begin_query(vec![&key_clone]).unwrap();
				let result = tx.get(&key_clone).unwrap();
				assert!(result.is_some());
				assert_eq!(result.unwrap().values, value_clone);
			});
			handles.push(handle);
		}

		// Spawn a writer for key2 (different key, should not block readers)
		let svl_clone = Arc::clone(&svl);
		let new_value = make_value("new_value2");
		let handle = thread::spawn(move || {
			let mut tx = svl_clone.begin_command(vec![&key2]).unwrap();
			tx.set(&key2, new_value).unwrap();
			tx.commit().unwrap();
		});
		handles.push(handle);

		// Wait for all threads
		for handle in handles {
			handle.join().unwrap();
		}
	}

	#[test]
	fn test_no_panics_with_rwlock() {
		use std::{sync::Arc, thread};

		let svl = Arc::new(create_test_svl());

		// Mix of operations across multiple threads
		let mut handles = vec![];
		for i in 0..10 {
			let svl_clone = Arc::clone(&svl);
			let key = make_key(&format!("key_{}", i % 3)); // Some key overlap
			let value = make_value(&format!("value_{}", i));

			let handle = thread::spawn(move || {
				// Alternate between reads and writes
				if i % 2 == 0 {
					let mut tx = svl_clone.begin_command(vec![&key]).unwrap();
					let _ = tx.set(&key, value);
					let _ = tx.commit();
				} else {
					let mut tx = svl_clone.begin_query(vec![&key]).unwrap();
					let _ = tx.get(&key);
				}
			});
			handles.push(handle);
		}

		// Wait for all threads - should not panic
		for handle in handles {
			handle.join().unwrap();
		}
	}

	#[test]
	fn test_write_blocks_concurrent_write() {
		use std::{
			sync::{Arc, Barrier},
			thread,
			time::Duration,
		};

		let svl = Arc::new(create_test_svl());
		let key = make_key("blocking_key");
		let barrier = Arc::new(Barrier::new(2));

		// Thread 1: Hold write lock on key
		let svl1 = Arc::clone(&svl);
		let key1 = key.clone();
		let barrier1 = Arc::clone(&barrier);
		let handle1 = thread::spawn(move || {
			let mut tx = svl1.begin_command(vec![&key1]).unwrap();
			tx.set(&key1, make_value("value1")).unwrap();

			// Signal that we have the lock
			barrier1.wait();

			// Hold the transaction (and locks) for a bit
			thread::sleep(Duration::from_millis(100));

			tx.commit().unwrap();
		});

		// Thread 2: Try to acquire write lock on same key (should block)
		let svl2 = Arc::clone(&svl);
		let key2 = key.clone();
		let barrier2 = Arc::clone(&barrier);
		let handle2 = thread::spawn(move || {
			// Wait for thread 1 to acquire its lock
			barrier2.wait();

			// Small delay to ensure thread 1 is holding the lock
			thread::sleep(Duration::from_millis(10));

			// This should block until thread 1 commits
			let mut tx = svl2.begin_command(vec![&key2]).unwrap();
			tx.set(&key2, make_value("value2")).unwrap();
			tx.commit().unwrap();
		});

		handle1.join().unwrap();
		handle2.join().unwrap();

		// Verify final value is from thread 2
		let mut tx = svl.begin_query(vec![&key]).unwrap();
		let result = tx.get(&key).unwrap();
		assert!(result.is_some());
		assert_eq!(result.unwrap().values, make_value("value2"));
	}

	#[test]
	fn test_write_blocks_concurrent_read() {
		use std::{
			sync::{Arc, Barrier},
			thread,
			time::Duration,
		};

		let svl = Arc::new(create_test_svl());
		let key = make_key("blocking_key");

		// Write initial value
		{
			let mut tx = svl.begin_command(vec![&key]).unwrap();
			tx.set(&key, make_value("initial")).unwrap();
			tx.commit().unwrap();
		}

		let barrier = Arc::new(Barrier::new(2));

		// Thread 1: Hold write lock
		let svl1 = Arc::clone(&svl);
		let key1 = key.clone();
		let barrier1 = Arc::clone(&barrier);
		let handle1 = thread::spawn(move || {
			let mut tx = svl1.begin_command(vec![&key1]).unwrap();
			tx.set(&key1, make_value("updated")).unwrap();

			// Signal that we have the lock
			barrier1.wait();

			// Hold the transaction for a bit
			thread::sleep(Duration::from_millis(100));

			tx.commit().unwrap();
		});

		// Thread 2: Try to read (should block until write commits)
		let svl2 = Arc::clone(&svl);
		let key2 = key.clone();
		let barrier2 = Arc::clone(&barrier);
		let handle2 = thread::spawn(move || {
			// Wait for thread 1 to acquire its lock
			barrier2.wait();

			// Small delay to ensure thread 1 is holding the lock
			thread::sleep(Duration::from_millis(10));

			// This should block until thread 1 commits
			let mut tx = svl2.begin_query(vec![&key2]).unwrap();
			let result = tx.get(&key2).unwrap();

			// Should see the updated value after blocking
			assert!(result.is_some());
			assert_eq!(result.unwrap().values, make_value("updated"));
		});

		handle1.join().unwrap();
		handle2.join().unwrap();
	}

	#[test]
	fn test_concurrent_reads_allowed() {
		use std::{
			sync::{Arc, Barrier},
			thread,
			time::Duration,
		};

		let svl = Arc::new(create_test_svl());
		let key = make_key("shared_read_key");

		// Write initial value
		{
			let mut tx = svl.begin_command(vec![&key]).unwrap();
			tx.set(&key, make_value("shared")).unwrap();
			tx.commit().unwrap();
		}

		let barrier = Arc::new(Barrier::new(3));
		let mut handles = vec![];

		// Spawn 3 concurrent readers
		for _ in 0..3 {
			let svl_clone = Arc::clone(&svl);
			let key_clone = key.clone();
			let barrier_clone = Arc::clone(&barrier);

			let handle = thread::spawn(move || {
				let mut tx = svl_clone.begin_query(vec![&key_clone]).unwrap();

				// Wait for all readers to start
				barrier_clone.wait();

				// All should be able to read concurrently
				let result = tx.get(&key_clone).unwrap();
				assert!(result.is_some());
				assert_eq!(result.unwrap().values, make_value("shared"));

				// Hold for a bit to ensure overlap
				thread::sleep(Duration::from_millis(50));
			});
			handles.push(handle);
		}

		for handle in handles {
			handle.join().unwrap();
		}
	}

	#[test]
	fn test_overlapping_keys_different_order() {
		use std::{
			sync::{Arc, Barrier},
			thread,
			time::Duration,
		};

		let svl = Arc::new(create_test_svl());
		let key1 = make_key("deadlock_key1");
		let key2 = make_key("deadlock_key2");
		let barrier = Arc::new(Barrier::new(2));

		// Thread 1: locks [key1, key2]
		let svl1 = Arc::clone(&svl);
		let key1_clone = key1.clone();
		let key2_clone = key2.clone();
		let barrier1 = Arc::clone(&barrier);
		let handle1 = thread::spawn(move || {
			barrier1.wait();
			let mut tx = svl1.begin_command(vec![&key1_clone, &key2_clone]).unwrap();
			tx.set(&key1_clone, make_value("from_thread1")).unwrap();
			thread::sleep(Duration::from_millis(10)); // Hold locks briefly
			tx.commit().unwrap();
		});

		// Thread 2: locks [key2, key1] - REVERSED ORDER
		// With sorted locking, this should not deadlock
		let svl2 = Arc::clone(&svl);
		let key1_clone2 = key1.clone();
		let key2_clone2 = key2.clone();
		let barrier2 = Arc::clone(&barrier);
		let handle2 = thread::spawn(move || {
			barrier2.wait();
			let mut tx = svl2.begin_command(vec![&key2_clone2, &key1_clone2]).unwrap();
			tx.set(&key2_clone2, make_value("from_thread2")).unwrap();
			thread::sleep(Duration::from_millis(10)); // Hold locks briefly
			tx.commit().unwrap();
		});

		// Both threads should complete without deadlock
		handle1.join().unwrap();
		handle2.join().unwrap();

		// Verify both commits succeeded
		let mut tx = svl.begin_query(vec![&key1, &key2]).unwrap();
		let result1 = tx.get(&key1).unwrap();
		let result2 = tx.get(&key2).unwrap();
		assert!(result1.is_some());
		assert!(result2.is_some());
	}

	#[test]
	fn test_circular_dependency_three_transactions() {
		use std::{
			sync::{Arc, Barrier},
			thread,
			time::Duration,
		};

		let svl = Arc::new(create_test_svl());
		let key1 = make_key("circular_key1");
		let key2 = make_key("circular_key2");
		let key3 = make_key("circular_key3");
		let barrier = Arc::new(Barrier::new(3));

		// Thread 1: locks [key1, key2]
		let svl1 = Arc::clone(&svl);
		let k1_1 = key1.clone();
		let k2_1 = key2.clone();
		let barrier1 = Arc::clone(&barrier);
		let handle1 = thread::spawn(move || {
			barrier1.wait();
			let mut tx = svl1.begin_command(vec![&k1_1, &k2_1]).unwrap();
			tx.set(&k1_1, make_value("t1")).unwrap();
			thread::sleep(Duration::from_millis(10));
			tx.commit().unwrap();
		});

		// Thread 2: locks [key2, key3]
		let svl2 = Arc::clone(&svl);
		let k2_2 = key2.clone();
		let k3_2 = key3.clone();
		let barrier2 = Arc::clone(&barrier);
		let handle2 = thread::spawn(move || {
			barrier2.wait();
			let mut tx = svl2.begin_command(vec![&k2_2, &k3_2]).unwrap();
			tx.set(&k2_2, make_value("t2")).unwrap();
			thread::sleep(Duration::from_millis(10));
			tx.commit().unwrap();
		});

		// Thread 3: locks [key3, key1] - completes the potential cycle
		// With sorted locking, this should not create a circular dependency
		let svl3 = Arc::clone(&svl);
		let barrier3 = Arc::clone(&barrier);
		let handle3 = thread::spawn(move || {
			barrier3.wait();
			let mut tx = svl3.begin_command(vec![&key3, &key1]).unwrap();
			tx.set(&key3, make_value("t3")).unwrap();
			thread::sleep(Duration::from_millis(10));
			tx.commit().unwrap();
		});

		// All threads should complete without circular deadlock
		handle1.join().unwrap();
		handle2.join().unwrap();
		handle3.join().unwrap();
	}

	#[test]
	fn test_locks_released_on_drop() {
		use std::{sync::Arc, thread, time::Duration};

		let svl = Arc::new(create_test_svl());
		let key = make_key("drop_test_key");

		// Thread 1: Acquire lock and drop without commit
		let svl1 = Arc::clone(&svl);
		let key_clone = key.clone();
		let handle1 = thread::spawn(move || {
			let mut tx = svl1.begin_command(vec![&key_clone]).unwrap();
			tx.set(&key_clone, make_value("dropped")).unwrap();
			// Transaction dropped here without commit
		});

		handle1.join().unwrap();

		// Small delay to ensure drop completes
		thread::sleep(Duration::from_millis(10));

		// Thread 2: Should be able to acquire the lock immediately
		// If locks weren't released on drop, this would block indefinitely
		let svl2 = Arc::clone(&svl);
		let key_clone2 = key.clone();
		let handle2 = thread::spawn(move || {
			let mut tx = svl2.begin_command(vec![&key_clone2]).unwrap();
			tx.set(&key_clone2, make_value("success")).unwrap();
			tx.commit().unwrap();
		});

		// This should complete quickly if locks are released properly
		handle2.join().unwrap();

		// Verify the second transaction succeeded
		let mut tx = svl.begin_query(vec![&key]).unwrap();
		let result = tx.get(&key).unwrap();
		assert!(result.is_some());
		assert_eq!(result.unwrap().values, make_value("success"));
	}

	#[test]
	fn test_read_locks_released_on_panic_unwinding() {
		use std::{
			panic::{AssertUnwindSafe, catch_unwind},
			sync::Arc,
		};

		let svl = Arc::new(create_test_svl());
		let key = make_key("panic_read_test");

		// First write a value to read
		{
			let mut tx = svl.begin_command(vec![&key]).unwrap();
			tx.set(&key, make_value("initial")).unwrap();
			tx.commit().unwrap();
		}

		// Panic while holding read lock
		let svl_clone = Arc::clone(&svl);
		let key_clone = key.clone();
		let result = catch_unwind(AssertUnwindSafe(move || {
			let mut tx = svl_clone.begin_query(vec![&key_clone]).unwrap();
			let _value = tx.get(&key_clone).unwrap();
			panic!("Intentional panic during read transaction");
		}));

		assert!(result.is_err(), "Should have panicked");

		// Read lock should be released, subsequent write transaction should succeed
		let mut tx = svl.begin_command(vec![&key]).unwrap();
		tx.set(&key, make_value("after_read_panic")).unwrap();
		tx.commit().unwrap();

		// Verify the value was updated
		let mut tx = svl.begin_query(vec![&key]).unwrap();
		let result = tx.get(&key).unwrap();
		assert!(result.is_some());
		assert_eq!(result.unwrap().values, make_value("after_read_panic"));
	}

	#[test]
	fn test_write_locks_released_on_panic_unwinding() {
		use std::{
			panic::{AssertUnwindSafe, catch_unwind},
			sync::Arc,
		};

		let svl = Arc::new(create_test_svl());
		let key = make_key("panic_write_test");

		// Panic while holding write lock
		let svl_clone = Arc::clone(&svl);
		let key_clone = key.clone();
		let result = catch_unwind(AssertUnwindSafe(move || {
			let mut tx = svl_clone.begin_command(vec![&key_clone]).unwrap();
			tx.set(&key_clone, make_value("will_panic")).unwrap();
			panic!("Intentional panic during write transaction");
		}));

		assert!(result.is_err(), "Should have panicked");

		// Write lock should be released, subsequent transaction should succeed
		let mut tx = svl.begin_command(vec![&key]).unwrap();
		tx.set(&key, make_value("after_write_panic")).unwrap();
		tx.commit().unwrap();

		// Verify the value (panic rolled back, so this is the first commit)
		let mut tx = svl.begin_query(vec![&key]).unwrap();
		let result = tx.get(&key).unwrap();
		assert!(result.is_some());
		assert_eq!(result.unwrap().values, make_value("after_write_panic"));
	}
}
