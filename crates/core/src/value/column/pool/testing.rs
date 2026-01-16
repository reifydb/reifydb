// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Test helpers for pool-based tests
//!
//! This module provides utilities to make testing with pools easier,
//! including automatic pool setup and cleanup for tests.

use super::{config::PoolConfig, scoped::ScopedPools};

/// Test helper that provides isolated pools for each test
/// Automatically sets up test pools and cleans up after the test
pub struct TestPools {
	_guard: ScopedPools,
}

impl TestPools {
	/// Create test pools with default test configuration
	pub fn new() -> Self {
		Self {
			_guard: ScopedPools::test(),
		}
	}

	/// Create test pools with custom configuration
	pub fn with_config(config: PoolConfig) -> Self {
		Self {
			_guard: ScopedPools::with_config(config),
		}
	}

	/// Create test pools with a specific max pool size
	pub fn with_size(max_pool_size: usize) -> Self {
		let config = PoolConfig {
			max_pool_size,
			..PoolConfig::test()
		};
		Self::with_config(config)
	}
}

impl Default for TestPools {
	fn default() -> Self {
		Self::new()
	}
}

/// Macro for tests that need pools
/// This macro automatically sets up test pools before running the test body
#[macro_export]
macro_rules! test_with_pools {
	($name:ident, $body:block) => {
		#[test]
		fn $name() {
			let _pools = $crate::value::column::pool::testing::TestPools::new();
			$body
		}
	};
}

/// Macro for tests that need custom pool configuration
#[macro_export]
macro_rules! test_with_custom_pools {
	($name:ident, $config:expr, $body:block) => {
		#[test]
		fn $name() {
			let _pools = $crate::value::column::pool::testing::TestPools::with_config($config);
			$body
		}
	};
}

/// Run a test function with temporary test pools
pub fn run_with_test_pools<F, R>(f: F) -> R
where
	F: FnOnce() -> R,
{
	let _pools = TestPools::new();
	f()
}

/// Run a test function with custom pools
pub fn run_with_custom_pools<F, R>(config: PoolConfig, f: F) -> R
where
	F: FnOnce() -> R,
{
	let _pools = TestPools::with_config(config);
	f()
}

#[cfg(test)]
pub mod tests {
	use super::*;
	use crate::value::column::{
		ColumnData,
		pool::thread_local::{clear_thread_pools, has_thread_pools},
	};

	#[test]
	fn test_test_pools() {
		// Clear any existing pools
		clear_thread_pools();
		assert!(!has_thread_pools());

		{
			let _test_pools = TestPools::new();
			// Pools should be available in scope
			assert!(has_thread_pools());

			// Should be able to create ColumnData
			// (assuming factory methods will be updated to use
			// thread-local pools)
			let data = ColumnData::int4(vec![1, 2, 3]);
			assert_eq!(data.len(), 3);
		}

		// Pools should be cleaned up
		assert!(!has_thread_pools());
	}

	test_with_pools!(test_macro_usage, {
		// Pools are automatically available
		assert!(has_thread_pools());

		let data = ColumnData::bool(vec![true, false, true]);
		assert_eq!(data.len(), 3);
	});

	test_with_custom_pools!(
		test_custom_macro,
		PoolConfig {
			max_pool_size: 2,
			..PoolConfig::test()
		},
		{
			assert!(has_thread_pools());

			let data = ColumnData::utf8(vec!["hello", "world"]);
			assert_eq!(data.len(), 2);
		}
	);

	#[test]
	fn test_run_with_pools() {
		clear_thread_pools();

		let result = run_with_test_pools(|| {
			assert!(has_thread_pools());
			42
		});

		assert_eq!(result, 42);
		assert!(!has_thread_pools());
	}
}
