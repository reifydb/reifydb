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
		let keys_vec: Vec<EncodedKey> = keys.into_iter().cloned().collect();
		assert!(
			!keys_vec.is_empty(),
			"SVL transactions must declare keys upfront - empty keysets are not allowed"
		);

		// Acquire read locks on all keys
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
		let keys_vec: Vec<EncodedKey> = keys.into_iter().cloned().collect();
		assert!(
			!keys_vec.is_empty(),
			"SVL transactions must declare keys upfront - empty keysets are not allowed"
		);

		// Acquire write locks on all keys
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
}
