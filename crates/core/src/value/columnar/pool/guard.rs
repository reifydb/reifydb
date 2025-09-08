// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Automatic pool management through RAII guards
//!
//! This module provides the `PooledGuard` type that automatically returns
//! containers to their respective pools when dropped, implementing the RAII
//! pattern for memory pool management.

use std::{
	fmt::{Debug, Formatter},
	ops::{Deref, DerefMut},
	rc::{Rc, Weak},
};

use reifydb_type::{Date, DateTime, Interval, Time, Uuid4, Uuid7};

use super::{PoolAllocator, Pools, PoolsInner};
use crate::value::container::*;

/// Trait for containers that can be released back to a pool
pub trait Releasable: Clone + Debug {
	fn release_to_pool(self, pools: &Pools);
}

// Implement Releasable for all container types
impl Releasable for BoolContainer {
	fn release_to_pool(self, pools: &Pools) {
		pools.bool_pool().release(self);
	}
}

impl Releasable for Utf8Container {
	fn release_to_pool(self, pools: &Pools) {
		pools.string_pool().release(self);
	}
}

impl Releasable for BlobContainer {
	fn release_to_pool(self, pools: &Pools) {
		pools.blob_pool().release(self);
	}
}

impl Releasable for RowNumberContainer {
	fn release_to_pool(self, pools: &Pools) {
		pools.row_number_pool().release(self);
	}
}

impl Releasable for UndefinedContainer {
	fn release_to_pool(self, pools: &Pools) {
		pools.undefined_pool().release(self);
	}
}

impl Releasable for NumberContainer<i32> {
	fn release_to_pool(self, pools: &Pools) {
		pools.i32_pool().release(self);
	}
}

impl Releasable for NumberContainer<i64> {
	fn release_to_pool(self, pools: &Pools) {
		pools.i64_pool().release(self);
	}
}

impl Releasable for NumberContainer<f32> {
	fn release_to_pool(self, pools: &Pools) {
		pools.f32_pool().release(self);
	}
}

impl Releasable for NumberContainer<f64> {
	fn release_to_pool(self, pools: &Pools) {
		pools.f64_pool().release(self);
	}
}

impl Releasable for NumberContainer<i8> {
	fn release_to_pool(self, pools: &Pools) {
		pools.i8_pool().release(self);
	}
}

impl Releasable for NumberContainer<i16> {
	fn release_to_pool(self, pools: &Pools) {
		pools.i16_pool().release(self);
	}
}

impl Releasable for NumberContainer<i128> {
	fn release_to_pool(self, pools: &Pools) {
		pools.i128_pool().release(self);
	}
}

impl Releasable for NumberContainer<u8> {
	fn release_to_pool(self, pools: &Pools) {
		pools.u8_pool().release(self);
	}
}

impl Releasable for NumberContainer<u16> {
	fn release_to_pool(self, pools: &Pools) {
		pools.u16_pool().release(self);
	}
}

impl Releasable for NumberContainer<u32> {
	fn release_to_pool(self, pools: &Pools) {
		pools.u32_pool().release(self);
	}
}

impl Releasable for NumberContainer<u64> {
	fn release_to_pool(self, pools: &Pools) {
		pools.u64_pool().release(self);
	}
}

impl Releasable for NumberContainer<u128> {
	fn release_to_pool(self, pools: &Pools) {
		pools.u128_pool().release(self);
	}
}

impl Releasable for TemporalContainer<Date> {
	fn release_to_pool(self, pools: &Pools) {
		pools.date_pool().release(self);
	}
}

impl Releasable for TemporalContainer<DateTime> {
	fn release_to_pool(self, pools: &Pools) {
		pools.datetime_pool().release(self);
	}
}

impl Releasable for TemporalContainer<Time> {
	fn release_to_pool(self, pools: &Pools) {
		pools.time_pool().release(self);
	}
}

impl Releasable for TemporalContainer<Interval> {
	fn release_to_pool(self, pools: &Pools) {
		pools.interval_pool().release(self);
	}
}

impl Releasable for UuidContainer<Uuid4> {
	fn release_to_pool(self, pools: &Pools) {
		pools.uuid4_pool().release(self);
	}
}

impl Releasable for UuidContainer<Uuid7> {
	fn release_to_pool(self, pools: &Pools) {
		pools.uuid7_pool().release(self);
	}
}

/// A guard that automatically returns a container to its pool when dropped
///
/// This implements the RAII pattern for container pooling - when the guard goes
/// out of scope, it automatically releases the contained value back to the pool
/// for reuse.
pub struct PooledGuard<T: Releasable> {
	container: Option<T>,
	pools: Weak<PoolsInner>,
}

impl<T: Releasable> Debug for PooledGuard<T> {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		Debug::fmt(&self.container, f)
	}
}

impl<T: Releasable> PooledGuard<T> {
	/// Create a new pooled guard with the given container and pool
	/// reference
	fn new(container: T, pools: Pools) -> Self {
		Self {
			container: Some(container),
			pools: Rc::downgrade(&pools.0),
		}
	}

	/// Clone the container and release the guard back to the pool
	///
	/// This returns a clone of the container while automatically releasing
	/// the original back to the pool for reuse.
	pub fn to_owned(mut self) -> T {
		let container =
			self.container.take().expect("Container already taken");
		let cloned_container = container.clone();

		// Release the original container back to the pool
		if let Some(pools_inner) = self.pools.upgrade() {
			container.release_to_pool(&Pools(pools_inner));
		}

		cloned_container
	}

	/// Check if the container is still held by this guard
	pub fn is_empty(&self) -> bool {
		self.container.is_none()
	}
}

// Container-specific constructors
impl PooledGuard<BoolContainer> {
	/// Create a new pooled BoolContainer with the specified capacity
	pub fn new_bool(pools: Pools, capacity: usize) -> Self {
		let container = pools.bool_pool().acquire(capacity);
		Self::new(container, pools)
	}
}

impl PooledGuard<Utf8Container> {
	/// Create a new pooled StringContainer with the specified capacity
	pub fn new_string(pools: Pools, capacity: usize) -> Self {
		let container = pools.string_pool().acquire(capacity);
		Self::new(container, pools)
	}
}

impl PooledGuard<BlobContainer> {
	/// Create a new pooled BlobContainer with the specified capacity
	pub fn new_blob(pools: Pools, capacity: usize) -> Self {
		let container = pools.blob_pool().acquire(capacity);
		Self::new(container, pools)
	}
}

impl PooledGuard<RowNumberContainer> {
	/// Create a new pooled RowNumberContainer with the specified capacity
	pub fn new_row_number(pools: Pools, capacity: usize) -> Self {
		let container = pools.row_number_pool().acquire(capacity);
		Self::new(container, pools)
	}
}

impl PooledGuard<UndefinedContainer> {
	/// Create a new pooled UndefinedContainer with the specified capacity
	pub fn new_undefined(pools: Pools, capacity: usize) -> Self {
		let container = pools.undefined_pool().acquire(capacity);
		Self::new(container, pools)
	}
}

// Numeric container constructors
impl PooledGuard<NumberContainer<i32>> {
	/// Create a new pooled NumberContainer<i32> with the specified capacity
	pub fn new_i32(pools: Pools, capacity: usize) -> Self {
		let container = pools.i32_pool().acquire(capacity);
		Self::new(container, pools)
	}
}

impl PooledGuard<NumberContainer<i64>> {
	/// Create a new pooled NumberContainer<i64> with the specified capacity
	pub fn new_i64(pools: Pools, capacity: usize) -> Self {
		let container = pools.i64_pool().acquire(capacity);
		Self::new(container, pools)
	}
}

impl PooledGuard<NumberContainer<f32>> {
	/// Create a new pooled NumberContainer<f32> with the specified capacity
	pub fn new_f32(pools: Pools, capacity: usize) -> Self {
		let container = pools.f32_pool().acquire(capacity);
		Self::new(container, pools)
	}
}

impl PooledGuard<NumberContainer<f64>> {
	/// Create a new pooled NumberContainer<f64> with the specified capacity
	pub fn new_f64(pools: Pools, capacity: usize) -> Self {
		let container = pools.f64_pool().acquire(capacity);
		Self::new(container, pools)
	}
}

impl PooledGuard<NumberContainer<i8>> {
	/// Create a new pooled NumberContainer<i8> with the specified capacity
	pub fn new_i8(pools: Pools, capacity: usize) -> Self {
		let container = pools.i8_pool().acquire(capacity);
		Self::new(container, pools)
	}
}

impl PooledGuard<NumberContainer<i16>> {
	/// Create a new pooled NumberContainer<i16> with the specified capacity
	pub fn new_i16(pools: Pools, capacity: usize) -> Self {
		let container = pools.i16_pool().acquire(capacity);
		Self::new(container, pools)
	}
}

impl PooledGuard<NumberContainer<i128>> {
	/// Create a new pooled NumberContainer<i128> with the specified
	/// capacity
	pub fn new_i128(pools: Pools, capacity: usize) -> Self {
		let container = pools.i128_pool().acquire(capacity);
		Self::new(container, pools)
	}
}

impl PooledGuard<NumberContainer<u8>> {
	/// Create a new pooled NumberContainer<u8> with the specified capacity
	pub fn new_u8(pools: Pools, capacity: usize) -> Self {
		let container = pools.u8_pool().acquire(capacity);
		Self::new(container, pools)
	}
}

impl PooledGuard<NumberContainer<u16>> {
	/// Create a new pooled NumberContainer<u16> with the specified capacity
	pub fn new_u16(pools: Pools, capacity: usize) -> Self {
		let container = pools.u16_pool().acquire(capacity);
		Self::new(container, pools)
	}
}

impl PooledGuard<NumberContainer<u32>> {
	/// Create a new pooled NumberContainer<u32> with the specified capacity
	pub fn new_u32(pools: Pools, capacity: usize) -> Self {
		let container = pools.u32_pool().acquire(capacity);
		Self::new(container, pools)
	}
}

impl PooledGuard<NumberContainer<u64>> {
	/// Create a new pooled NumberContainer<u64> with the specified capacity
	pub fn new_u64(pools: Pools, capacity: usize) -> Self {
		let container = pools.u64_pool().acquire(capacity);
		Self::new(container, pools)
	}
}

impl PooledGuard<NumberContainer<u128>> {
	/// Create a new pooled NumberContainer<u128> with the specified
	/// capacity
	pub fn new_u128(pools: Pools, capacity: usize) -> Self {
		let container = pools.u128_pool().acquire(capacity);
		Self::new(container, pools)
	}
}

// Temporal container constructors
impl PooledGuard<TemporalContainer<Date>> {
	/// Create a new pooled TemporalContainer<Date> with the specified
	/// capacity
	pub fn new_date(pools: Pools, capacity: usize) -> Self {
		let container = pools.date_pool().acquire(capacity);
		Self::new(container, pools)
	}
}

impl PooledGuard<TemporalContainer<DateTime>> {
	/// Create a new pooled TemporalContainer<DateTime> with the specified
	/// capacity
	pub fn new_datetime(pools: Pools, capacity: usize) -> Self {
		let container = pools.datetime_pool().acquire(capacity);
		Self::new(container, pools)
	}
}

impl PooledGuard<TemporalContainer<Time>> {
	/// Create a new pooled TemporalContainer<Time> with the specified
	/// capacity
	pub fn new_time(pools: Pools, capacity: usize) -> Self {
		let container = pools.time_pool().acquire(capacity);
		Self::new(container, pools)
	}
}

impl PooledGuard<TemporalContainer<Interval>> {
	/// Create a new pooled TemporalContainer<Interval> with the specified
	/// capacity
	pub fn new_interval(pools: Pools, capacity: usize) -> Self {
		let container = pools.interval_pool().acquire(capacity);
		Self::new(container, pools)
	}
}

// UUID container constructors
impl PooledGuard<UuidContainer<Uuid4>> {
	/// Create a new pooled UuidContainer<Uuid4> with the specified capacity
	pub fn new_uuid4(pools: Pools, capacity: usize) -> Self {
		let container = pools.uuid4_pool().acquire(capacity);
		Self::new(container, pools)
	}
}

impl PooledGuard<UuidContainer<Uuid7>> {
	/// Create a new pooled UuidContainer<Uuid7> with the specified capacity
	pub fn new_uuid7(pools: Pools, capacity: usize) -> Self {
		let container = pools.uuid7_pool().acquire(capacity);
		Self::new(container, pools)
	}
}

// Implement Drop for automatic pool release
impl<T: Releasable> Drop for PooledGuard<T> {
	fn drop(&mut self) {
		if let (Some(container), Some(pools_inner)) =
			(self.container.take(), self.pools.upgrade())
		{
			container.release_to_pool(&Pools(pools_inner));
		}
	}
}

// Implement Deref for transparent access to the container
impl<T: Releasable> Deref for PooledGuard<T> {
	type Target = T;

	fn deref(&self) -> &Self::Target {
		self.container.as_ref().expect("Container has been taken")
	}
}

// Implement DerefMut for mutable access to the container
impl<T: Releasable> DerefMut for PooledGuard<T> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		self.container.as_mut().expect("Container has been taken")
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_bool_container_guard() {
		let pools = Pools::default();

		// Initial pool should be empty
		let initial_stats = pools.bool_pool().stats();
		assert_eq!(initial_stats.available, 0);

		{
			let guard = PooledGuard::new_bool(pools.clone(), 10);
			assert!(guard.capacity() >= 10);
			assert_eq!(guard.len(), 0);

			// Pool should show one acquired
			let stats = pools.bool_pool().stats();
			assert_eq!(stats.total_acquired, 1);
			assert_eq!(stats.available, 0);
		} // Guard dropped here

		// After drop, container should be returned to pool
		let final_stats = pools.bool_pool().stats();
		assert_eq!(final_stats.total_acquired, 1);
		assert_eq!(final_stats.total_released, 1);
		assert_eq!(final_stats.available, 1);
	}

	#[test]
	fn test_number_container_guard() {
		let pools = Pools::default();

		{
			let mut guard = PooledGuard::new_i32(pools.clone(), 20);
			assert!(guard.capacity() >= 20);

			// Test mutable access
			guard.push(42);
			guard.push(100);
			assert_eq!(guard.len(), 2);
			assert_eq!(guard.get(0), Some(&42));
			assert_eq!(guard.get(1), Some(&100));
		}

		// Container should be returned to pool
		let stats = pools.i32_pool().stats();
		assert_eq!(stats.available, 1);
	}

	#[test]
	fn test_string_container_guard() {
		let pools = Pools::default();

		{
			let mut guard =
				PooledGuard::new_string(pools.clone(), 5);
			guard.push("hello".to_string());
			guard.push("world".to_string());
			assert_eq!(guard.len(), 2);
		}

		let stats = pools.string_pool().stats();
		assert_eq!(stats.available, 1);
	}

	#[test]
	fn test_guard_to_owned() {
		let pools = Pools::default();

		let guard = PooledGuard::new_bool(pools.clone(), 10);
		let container = guard.to_owned(); // Clone container and release guard to pool

		assert!(container.capacity() >= 10);

		// Pool should have received the original container back
		let stats = pools.bool_pool().stats();
		assert_eq!(stats.available, 1);
		assert_eq!(stats.total_released, 1);

		// We now have a cloned container that's independent of the pool
		assert!(container.capacity() >= 10);
	}

	#[test]
	fn test_multiple_guards_same_pool() {
		let pools = Pools::default();

		{
			let _guard1 = PooledGuard::new_f32(pools.clone(), 100);
			let _guard2 = PooledGuard::new_f32(pools.clone(), 200);
			let _guard3 = PooledGuard::new_f32(pools.clone(), 50);

			let stats = pools.f32_pool().stats();
			assert_eq!(stats.total_acquired, 3);
			assert_eq!(stats.available, 0);
		}

		// All should be returned
		let final_stats = pools.f32_pool().stats();
		assert_eq!(final_stats.total_released, 3);
		assert_eq!(final_stats.available, 3);
	}

	#[test]
	fn test_guard_reuse() {
		let pools = Pools::default();

		// First usage
		{
			let mut guard = PooledGuard::new_i64(pools.clone(), 50);
			guard.push(123);
			assert_eq!(guard.len(), 1);
		}

		// Second usage should reuse the same container
		{
			let guard = PooledGuard::new_i64(pools.clone(), 50);
			// Container should be cleared from pool
			assert_eq!(guard.len(), 0);
			assert!(guard.capacity() >= 50);
		}

		let stats = pools.i64_pool().stats();
		assert_eq!(stats.total_acquired, 2);
		assert_eq!(stats.total_released, 2);
		assert_eq!(stats.available, 1);
	}

	#[test]
	fn test_temporal_containers() {
		let pools = Pools::default();

		{
			let _date_guard =
				PooledGuard::new_date(pools.clone(), 10);
			let _datetime_guard =
				PooledGuard::new_datetime(pools.clone(), 20);
			let _time_guard =
				PooledGuard::new_time(pools.clone(), 30);
			let _interval_guard =
				PooledGuard::new_interval(pools.clone(), 40);
		}

		let all_stats = pools.all_stats();
		assert_eq!(all_stats["date"].available, 1);
		assert_eq!(all_stats["datetime"].available, 1);
		assert_eq!(all_stats["time"].available, 1);
		assert_eq!(all_stats["interval"].available, 1);
	}

	#[test]
	fn test_uuid_containers() {
		let pools = Pools::default();

		{
			let _uuid4_guard =
				PooledGuard::new_uuid4(pools.clone(), 15);
			let _uuid7_guard =
				PooledGuard::new_uuid7(pools.clone(), 25);
		}

		let all_stats = pools.all_stats();
		assert_eq!(all_stats["uuid4"].available, 1);
		assert_eq!(all_stats["uuid7"].available, 1);
	}
}
