// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use crossbeam_skiplist::SkipMap;
use reifydb_core::{
	CowVec, EncodedKey, delta::Delta, event::EventBus, interface::WithEventBus, value::encoded::EncodedValues,
};
use reifydb_store_transaction::TransactionStore;
use tokio::sync::RwLock as TokioRwLock;

mod read;
mod write;

use read::KeyReadLock;
pub use read::SvlQueryTransaction;
use write::KeyWriteLock;
pub use write::SvlCommandTransaction;

#[derive(Clone)]
pub struct TransactionSvl {
	inner: Arc<TransactionSvlInner>,
}

struct TransactionSvlInner {
	store: TokioRwLock<TransactionStore>,
	event_bus: EventBus,
	key_locks: SkipMap<EncodedKey, Arc<TokioRwLock<()>>>,
}

impl TransactionSvlInner {
	fn get_or_create_lock(&self, key: &EncodedKey) -> Arc<TokioRwLock<()>> {
		// Check if lock exists
		if let Some(entry) = self.key_locks.get(key) {
			return entry.value().clone();
		}

		// Create new lock
		let lock = Arc::new(TokioRwLock::new(()));
		self.key_locks.insert(key.clone(), lock.clone());
		lock
	}
}

impl TransactionSvl {
	pub fn new(store: TransactionStore, event_bus: EventBus) -> Self {
		Self {
			inner: Arc::new(TransactionSvlInner {
				store: TokioRwLock::new(store),
				event_bus,
				key_locks: SkipMap::new(),
			}),
		}
	}

	pub async fn begin_query<'a, I>(&self, keys: I) -> crate::Result<SvlQueryTransaction<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send,
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
			// Use owned guard for Send safety
			let lock = KeyReadLock(arc.read_owned().await);
			locks.push(lock);
		}

		Ok(SvlQueryTransaction {
			inner: &self.inner,
			keys: keys_vec,
			_key_locks: locks,
		})
	}

	pub async fn begin_command<'a, I>(&self, keys: I) -> crate::Result<SvlCommandTransaction<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send,
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
			// Use owned guard for Send safety
			let lock = KeyWriteLock(arc.write_owned().await);
			locks.push(lock);
		}

		Ok(SvlCommandTransaction::new(&self.inner, keys_vec, locks))
	}
}

impl WithEventBus for TransactionSvl {
	fn event_bus(&self) -> &EventBus {
		&self.inner.event_bus
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use tokio::time::Duration;

	use super::*;

	fn make_key(s: &str) -> EncodedKey {
		EncodedKey(CowVec::new(s.as_bytes().to_vec()))
	}

	fn make_value(s: &str) -> EncodedValues {
		EncodedValues(CowVec::new(s.as_bytes().to_vec()))
	}

	async fn create_test_svl() -> TransactionSvl {
		TransactionSvl::new(TransactionStore::testing_memory().await, EventBus::default())
	}

	#[tokio::test]
	async fn test_allowed_key_query() {
		let svl = create_test_svl().await;
		let key = make_key("test_key");

		// Start scoped query with the key
		let mut tx = svl.begin_query(vec![&key]).await.unwrap();

		// Should be able to get the key
		let result = tx.get(&key).await;
		assert!(result.is_ok());
	}

	#[tokio::test]
	async fn test_disallowed_key_query() {
		let svl = create_test_svl().await;
		let key1 = make_key("allowed");
		let key2 = make_key("disallowed");

		// Start scoped query with only key1
		let mut tx = svl.begin_query(vec![&key1]).await.unwrap();

		// Should succeed for key1
		assert!(tx.get(&key1).await.is_ok());

		// Should fail for key2
		let result = tx.get(&key2).await;
		assert!(result.is_err());
		let err = result.unwrap_err();
		assert_eq!(err.0.code, "TXN_010");
	}

	#[tokio::test]
	#[should_panic(expected = "SVL transactions must declare keys upfront - empty keysets are not allowed")]
	async fn test_empty_keyset_query_panics() {
		let svl = create_test_svl().await;

		// Should panic when trying to create transaction with empty keys
		let _tx = svl.begin_query(std::iter::empty()).await;
	}

	#[tokio::test]
	#[should_panic(expected = "SVL transactions must declare keys upfront - empty keysets are not allowed")]
	async fn test_empty_keyset_command_panics() {
		let svl = create_test_svl().await;

		// Should panic when trying to create transaction with empty keys
		let _tx = svl.begin_command(std::iter::empty()).await;
	}

	#[tokio::test]
	async fn test_allowed_key_command() {
		let svl = create_test_svl().await;
		let key = make_key("test_key");
		let value = make_value("test_value");

		// Start scoped command with the key
		let mut tx = svl.begin_command(vec![&key]).await.unwrap();

		// Should be able to set and get the key
		assert!(tx.set(&key, value.clone()).is_ok());
		assert!(tx.get(&key).await.is_ok());
		assert!(tx.commit().await.is_ok());
	}

	#[tokio::test]
	async fn test_disallowed_key_command() {
		let svl = create_test_svl().await;
		let key1 = make_key("allowed");
		let key2 = make_key("disallowed");
		let value = make_value("test_value");

		// Start scoped command with only key1
		let mut tx = svl.begin_command(vec![&key1]).await.unwrap();

		// Should succeed for key1
		assert!(tx.set(&key1, value.clone()).is_ok());

		// Should fail for key2
		let result = tx.set(&key2, value);
		assert!(result.is_err());
		let err = result.unwrap_err();
		assert_eq!(err.0.code, "TXN_010");
	}

	#[tokio::test]
	async fn test_command_commit_with_valid_keys() {
		let svl = create_test_svl().await;
		let key1 = make_key("key1");
		let key2 = make_key("key2");
		let value1 = make_value("value1");
		let value2 = make_value("value2");

		// Write with scoped transaction
		{
			let mut tx = svl.begin_command(vec![&key1, &key2]).await.unwrap();
			tx.set(&key1, value1.clone()).unwrap();
			tx.set(&key2, value2.clone()).unwrap();
			tx.commit().await.unwrap();
		}

		// Verify with query
		{
			let mut tx = svl.begin_query(vec![&key1, &key2]).await.unwrap();
			let result1 = tx.get(&key1).await.unwrap();
			let result2 = tx.get(&key2).await.unwrap();
			assert!(result1.is_some());
			assert!(result2.is_some());
			assert_eq!(result1.unwrap().values, value1);
			assert_eq!(result2.unwrap().values, value2);
		}
	}

	#[tokio::test]
	async fn test_rollback_with_scoped_keys() {
		let svl = create_test_svl().await;
		let key = make_key("test_key");
		let value = make_value("test_value");

		// Start transaction and rollback
		{
			let mut tx = svl.begin_command(vec![&key]).await.unwrap();
			tx.set(&key, value).unwrap();
			tx.rollback().await.unwrap();
		}

		// Verify nothing was committed
		{
			let mut tx = svl.begin_query(vec![&key]).await.unwrap();
			let result = tx.get(&key).await.unwrap();
			assert!(result.is_none());
		}
	}

	#[tokio::test]
	async fn test_concurrent_reads() {
		let svl = Arc::new(create_test_svl().await);
		let key = make_key("shared_key");
		let value = make_value("shared_value");

		// Write initial value
		{
			let mut tx = svl.begin_command(vec![&key]).await.unwrap();
			tx.set(&key, value.clone()).unwrap();
			tx.commit().await.unwrap();
		}

		// Spawn multiple readers
		let mut handles = vec![];
		for _ in 0..5 {
			let svl_clone = Arc::clone(&svl);
			let key_clone = key.clone();
			let value_clone = value.clone();

			let handle = tokio::spawn(async move {
				let mut tx = svl_clone.begin_query(vec![&key_clone]).await.unwrap();
				let result = tx.get(&key_clone).await.unwrap();
				assert!(result.is_some());
				assert_eq!(result.unwrap().values, value_clone);
			});
			handles.push(handle);
		}

		// Wait for all tasks
		for handle in handles {
			handle.await.unwrap();
		}
	}

	#[tokio::test]
	async fn test_concurrent_writers_disjoint_keys() {
		let svl = Arc::new(create_test_svl().await);

		// Spawn multiple writers with disjoint keys
		let mut handles = vec![];
		for i in 0..5 {
			let svl_clone = Arc::clone(&svl);
			let key = make_key(&format!("key_{}", i));
			let value = make_value(&format!("value_{}", i));

			let handle = tokio::spawn(async move {
				let mut tx = svl_clone.begin_command(vec![&key]).await.unwrap();
				tx.set(&key, value).unwrap();
				tx.commit().await.unwrap();
			});
			handles.push(handle);
		}

		// Wait for all tasks
		for handle in handles {
			handle.await.unwrap();
		}

		// Verify all values were written
		for i in 0..5 {
			let key = make_key(&format!("key_{}", i));
			let expected_value = make_value(&format!("value_{}", i));

			let mut tx = svl.begin_query(vec![&key]).await.unwrap();
			let result = tx.get(&key).await.unwrap();
			assert!(result.is_some());
			assert_eq!(result.unwrap().values, expected_value);
		}
	}

	#[tokio::test]
	async fn test_concurrent_readers_and_writer() {
		let svl = Arc::new(create_test_svl().await);
		let key1 = make_key("key1");
		let key2 = make_key("key2");
		let value1 = make_value("value1");
		let value2 = make_value("value2");

		// Write initial values
		{
			let mut tx = svl.begin_command(vec![&key1, &key2]).await.unwrap();
			tx.set(&key1, value1.clone()).unwrap();
			tx.set(&key2, value2.clone()).unwrap();
			tx.commit().await.unwrap();
		}

		// Spawn readers for key1
		let mut handles = vec![];
		for _ in 0..3 {
			let svl_clone = Arc::clone(&svl);
			let key_clone = key1.clone();
			let value_clone = value1.clone();

			let handle = tokio::spawn(async move {
				let mut tx = svl_clone.begin_query(vec![&key_clone]).await.unwrap();
				let result = tx.get(&key_clone).await.unwrap();
				assert!(result.is_some());
				assert_eq!(result.unwrap().values, value_clone);
			});
			handles.push(handle);
		}

		// Spawn a writer for key2 (different key, should not block readers)
		let svl_clone = Arc::clone(&svl);
		let new_value = make_value("new_value2");
		let handle = tokio::spawn(async move {
			let mut tx = svl_clone.begin_command(vec![&key2]).await.unwrap();
			tx.set(&key2, new_value).unwrap();
			tx.commit().await.unwrap();
		});
		handles.push(handle);

		// Wait for all tasks
		for handle in handles {
			handle.await.unwrap();
		}
	}

	#[tokio::test]
	async fn test_no_panics_with_rwlock() {
		let svl = Arc::new(create_test_svl().await);

		// Mix of operations across multiple tasks
		let mut handles = vec![];
		for i in 0..10 {
			let svl_clone = Arc::clone(&svl);
			let key = make_key(&format!("key_{}", i % 3)); // Some key overlap
			let value = make_value(&format!("value_{}", i));

			let handle = tokio::spawn(async move {
				// Alternate between reads and writes
				if i % 2 == 0 {
					let mut tx = svl_clone.begin_command(vec![&key]).await.unwrap();
					let _ = tx.set(&key, value);
					let _ = tx.commit().await;
				} else {
					let mut tx = svl_clone.begin_query(vec![&key]).await.unwrap();
					let _ = tx.get(&key).await;
				}
			});
			handles.push(handle);
		}

		// Wait for all tasks - should not panic
		for handle in handles {
			handle.await.unwrap();
		}
	}

	#[tokio::test]
	async fn test_write_blocks_concurrent_write() {
		let svl = Arc::new(create_test_svl().await);
		let key = make_key("blocking_key");
		let barrier = Arc::new(tokio::sync::Barrier::new(2));

		// Task 1: Hold write lock on key
		let svl1 = Arc::clone(&svl);
		let key1 = key.clone();
		let barrier1 = Arc::clone(&barrier);
		let handle1 = tokio::spawn(async move {
			let mut tx = svl1.begin_command(vec![&key1]).await.unwrap();
			tx.set(&key1, make_value("value1")).unwrap();

			// Signal that we have the lock
			barrier1.wait().await;

			// Hold the transaction (and locks) for a bit
			tokio::time::sleep(Duration::from_millis(100)).await;

			tx.commit().await.unwrap();
		});

		// Task 2: Try to acquire write lock on same key (should block)
		let svl2 = Arc::clone(&svl);
		let key2 = key.clone();
		let barrier2 = Arc::clone(&barrier);
		let handle2 = tokio::spawn(async move {
			// Wait for task 1 to acquire its lock
			barrier2.wait().await;

			// Small delay to ensure task 1 is holding the lock
			tokio::time::sleep(Duration::from_millis(10)).await;

			// This should block until task 1 commits
			let mut tx = svl2.begin_command(vec![&key2]).await.unwrap();
			tx.set(&key2, make_value("value2")).unwrap();
			tx.commit().await.unwrap();
		});

		handle1.await.unwrap();
		handle2.await.unwrap();

		// Verify final value is from task 2
		let mut tx = svl.begin_query(vec![&key]).await.unwrap();
		let result = tx.get(&key).await.unwrap();
		assert!(result.is_some());
		assert_eq!(result.unwrap().values, make_value("value2"));
	}

	#[tokio::test]
	async fn test_write_blocks_concurrent_read() {
		let svl = Arc::new(create_test_svl().await);
		let key = make_key("blocking_key");

		// Write initial value
		{
			let mut tx = svl.begin_command(vec![&key]).await.unwrap();
			tx.set(&key, make_value("initial")).unwrap();
			tx.commit().await.unwrap();
		}

		let barrier = Arc::new(tokio::sync::Barrier::new(2));

		// Task 1: Hold write lock
		let svl1 = Arc::clone(&svl);
		let key1 = key.clone();
		let barrier1 = Arc::clone(&barrier);
		let handle1 = tokio::spawn(async move {
			let mut tx = svl1.begin_command(vec![&key1]).await.unwrap();
			tx.set(&key1, make_value("updated")).unwrap();

			// Signal that we have the lock
			barrier1.wait().await;

			// Hold the transaction for a bit
			tokio::time::sleep(Duration::from_millis(100)).await;

			tx.commit().await.unwrap();
		});

		// Task 2: Try to read (should block until write commits)
		let svl2 = Arc::clone(&svl);
		let key2 = key.clone();
		let barrier2 = Arc::clone(&barrier);
		let handle2 = tokio::spawn(async move {
			// Wait for task 1 to acquire its lock
			barrier2.wait().await;

			// Small delay to ensure task 1 is holding the lock
			tokio::time::sleep(Duration::from_millis(10)).await;

			// This should block until task 1 commits
			let mut tx = svl2.begin_query(vec![&key2]).await.unwrap();
			let result = tx.get(&key2).await.unwrap();

			// Should see the updated value after blocking
			assert!(result.is_some());
			assert_eq!(result.unwrap().values, make_value("updated"));
		});

		handle1.await.unwrap();
		handle2.await.unwrap();
	}

	#[tokio::test]
	async fn test_concurrent_reads_allowed() {
		let svl = Arc::new(create_test_svl().await);
		let key = make_key("shared_read_key");

		// Write initial value
		{
			let mut tx = svl.begin_command(vec![&key]).await.unwrap();
			tx.set(&key, make_value("shared")).unwrap();
			tx.commit().await.unwrap();
		}

		let barrier = Arc::new(tokio::sync::Barrier::new(3));
		let mut handles = vec![];

		// Spawn 3 concurrent readers
		for _ in 0..3 {
			let svl_clone = Arc::clone(&svl);
			let key_clone = key.clone();
			let barrier_clone = Arc::clone(&barrier);

			let handle = tokio::spawn(async move {
				let mut tx = svl_clone.begin_query(vec![&key_clone]).await.unwrap();

				// Wait for all readers to start
				barrier_clone.wait().await;

				// All should be able to read concurrently
				let result = tx.get(&key_clone).await.unwrap();
				assert!(result.is_some());
				assert_eq!(result.unwrap().values, make_value("shared"));

				// Hold for a bit to ensure overlap
				tokio::time::sleep(Duration::from_millis(50)).await;
			});
			handles.push(handle);
		}

		for handle in handles {
			handle.await.unwrap();
		}
	}

	#[tokio::test]
	async fn test_overlapping_keys_different_order() {
		let svl = Arc::new(create_test_svl().await);
		let key1 = make_key("deadlock_key1");
		let key2 = make_key("deadlock_key2");
		let barrier = Arc::new(tokio::sync::Barrier::new(2));

		// Task 1: locks [key1, key2]
		let svl1 = Arc::clone(&svl);
		let key1_clone = key1.clone();
		let key2_clone = key2.clone();
		let barrier1 = Arc::clone(&barrier);
		let handle1 = tokio::spawn(async move {
			barrier1.wait().await;
			let mut tx = svl1.begin_command(vec![&key1_clone, &key2_clone]).await.unwrap();
			tx.set(&key1_clone, make_value("from_task1")).unwrap();
			tokio::time::sleep(Duration::from_millis(10)).await; // Hold locks briefly
			tx.commit().await.unwrap();
		});

		// Task 2: locks [key2, key1] - REVERSED ORDER
		// With sorted locking, this should not deadlock
		let svl2 = Arc::clone(&svl);
		let key1_clone2 = key1.clone();
		let key2_clone2 = key2.clone();
		let barrier2 = Arc::clone(&barrier);
		let handle2 = tokio::spawn(async move {
			barrier2.wait().await;
			let mut tx = svl2.begin_command(vec![&key2_clone2, &key1_clone2]).await.unwrap();
			tx.set(&key2_clone2, make_value("from_task2")).unwrap();
			tokio::time::sleep(Duration::from_millis(10)).await; // Hold locks briefly
			tx.commit().await.unwrap();
		});

		// Both tasks should complete without deadlock
		handle1.await.unwrap();
		handle2.await.unwrap();

		// Verify both commits succeeded
		let mut tx = svl.begin_query(vec![&key1, &key2]).await.unwrap();
		let result1 = tx.get(&key1).await.unwrap();
		let result2 = tx.get(&key2).await.unwrap();
		assert!(result1.is_some());
		assert!(result2.is_some());
	}

	#[tokio::test]
	async fn test_circular_dependency_three_transactions() {
		let svl = Arc::new(create_test_svl().await);
		let key1 = make_key("circular_key1");
		let key2 = make_key("circular_key2");
		let key3 = make_key("circular_key3");
		let barrier = Arc::new(tokio::sync::Barrier::new(3));

		// Task 1: locks [key1, key2]
		let svl1 = Arc::clone(&svl);
		let k1_1 = key1.clone();
		let k2_1 = key2.clone();
		let barrier1 = Arc::clone(&barrier);
		let handle1 = tokio::spawn(async move {
			barrier1.wait().await;
			let mut tx = svl1.begin_command(vec![&k1_1, &k2_1]).await.unwrap();
			tx.set(&k1_1, make_value("t1")).unwrap();
			tokio::time::sleep(Duration::from_millis(10)).await;
			tx.commit().await.unwrap();
		});

		// Task 2: locks [key2, key3]
		let svl2 = Arc::clone(&svl);
		let k2_2 = key2.clone();
		let k3_2 = key3.clone();
		let barrier2 = Arc::clone(&barrier);
		let handle2 = tokio::spawn(async move {
			barrier2.wait().await;
			let mut tx = svl2.begin_command(vec![&k2_2, &k3_2]).await.unwrap();
			tx.set(&k2_2, make_value("t2")).unwrap();
			tokio::time::sleep(Duration::from_millis(10)).await;
			tx.commit().await.unwrap();
		});

		// Task 3: locks [key3, key1] - completes the potential cycle
		// With sorted locking, this should not create a circular dependency
		let svl3 = Arc::clone(&svl);
		let barrier3 = Arc::clone(&barrier);
		let handle3 = tokio::spawn(async move {
			barrier3.wait().await;
			let mut tx = svl3.begin_command(vec![&key3, &key1]).await.unwrap();
			tx.set(&key3, make_value("t3")).unwrap();
			tokio::time::sleep(Duration::from_millis(10)).await;
			tx.commit().await.unwrap();
		});

		// All tasks should complete without circular deadlock
		handle1.await.unwrap();
		handle2.await.unwrap();
		handle3.await.unwrap();
	}

	#[tokio::test]
	async fn test_locks_released_on_drop() {
		let svl = Arc::new(create_test_svl().await);
		let key = make_key("drop_test_key");

		// Task 1: Acquire lock and drop without commit
		let svl1 = Arc::clone(&svl);
		let key_clone = key.clone();
		let handle1 = tokio::spawn(async move {
			let mut tx = svl1.begin_command(vec![&key_clone]).await.unwrap();
			tx.set(&key_clone, make_value("dropped")).unwrap();
			// Transaction dropped here without commit
		});

		handle1.await.unwrap();

		// Small delay to ensure drop completes
		tokio::time::sleep(Duration::from_millis(10)).await;

		// Task 2: Should be able to acquire the lock immediately
		// If locks weren't released on drop, this would block indefinitely
		let svl2 = Arc::clone(&svl);
		let key_clone2 = key.clone();
		let handle2 = tokio::spawn(async move {
			let mut tx = svl2.begin_command(vec![&key_clone2]).await.unwrap();
			tx.set(&key_clone2, make_value("success")).unwrap();
			tx.commit().await.unwrap();
		});

		// This should complete quickly if locks are released properly
		handle2.await.unwrap();

		// Verify the second transaction succeeded
		let mut tx = svl.begin_query(vec![&key]).await.unwrap();
		let result = tx.get(&key).await.unwrap();
		assert!(result.is_some());
		assert_eq!(result.unwrap().values, make_value("success"));
	}
}
