// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Lazy initialization support for thread-local pools
//!
//! This module provides support for lazy initialization of thread-local pools,
//! allowing ColumnData operations to work even when pools haven't been
//! explicitly initialized.

use std::sync::RwLock;

use once_cell::sync::Lazy;

use super::{
	Pools,
	config::PoolConfig,
	thread_local::{get_thread_pools, has_thread_pools, set_thread_pools},
};

/// Global default configuration for thread-local pools.
/// Uses RwLock because config is read frequently (on every lazy pool init)
/// but only written during initial configuration.
static DEFAULT_CONFIG: Lazy<RwLock<PoolConfig>> = Lazy::new(|| RwLock::new(PoolConfig::default()));

/// Set the global default pool configuration
/// This configuration will be used when pools are lazily initialized
pub fn set_default_pool_config(config: PoolConfig) {
	*DEFAULT_CONFIG.write().unwrap() = config;
}

/// Get a copy of the current default pool configuration
pub fn get_default_pool_config() -> PoolConfig {
	DEFAULT_CONFIG.read().unwrap().clone()
}

/// Get or create thread-local pools using the default configuration
/// If pools are already initialized, returns them. Otherwise creates new pools
/// with the default configuration.
pub fn get_or_init_pools() -> Pools {
	get_thread_pools().unwrap_or_else(|| {
		let config = DEFAULT_CONFIG.read().unwrap().clone();
		let pools = Pools::new(config.max_pool_size);
		set_thread_pools(pools.clone());
		pools
	})
}

/// Ensure thread-local pools are initialized
/// Does nothing if pools are already initialized
pub fn ensure_thread_pools() {
	if !has_thread_pools() {
		get_or_init_pools();
	}
}

/// Get thread-local pools with lazy initialization
/// This is like thread_pools() but will initialize with defaults instead of
/// panicking
pub fn thread_pools_lazy() -> Pools {
	get_or_init_pools()
}

#[cfg(test)]
pub mod tests {
	use super::*;
	use crate::value::column::pool::thread_local::clear_thread_pools;

	#[test]
	fn test_get_or_init_pools() {
		// Clear any existing pools
		clear_thread_pools();
		assert!(!has_thread_pools());

		// First call should initialize
		let pools1 = get_or_init_pools();
		assert!(has_thread_pools());

		// Second call should return existing
		let pools2 = get_or_init_pools();
		assert!(has_thread_pools());

		// Clean up
		clear_thread_pools();
	}

	#[test]
	fn test_ensure_thread_pools() {
		clear_thread_pools();
		assert!(!has_thread_pools());

		// Should initialize pools
		ensure_thread_pools();
		assert!(has_thread_pools());

		// Should do nothing if already initialized
		ensure_thread_pools();
		assert!(has_thread_pools());

		clear_thread_pools();
	}

	#[test]
	fn test_custom_default_config() {
		// Set custom default config
		let custom_config = PoolConfig {
			max_pool_size: 64,
			enable_statistics: true,
			auto_clear_threshold: Some(5000),
			prewarm_capacities: vec![32, 128],
		};
		set_default_pool_config(custom_config.clone());

		// Get config back
		let retrieved = get_default_pool_config();
		assert_eq!(retrieved.max_pool_size, 64);
		assert_eq!(retrieved.enable_statistics, true);
		assert_eq!(retrieved.auto_clear_threshold, Some(5000));
		assert_eq!(retrieved.prewarm_capacities, vec![32, 128]);

		// Reset to default
		set_default_pool_config(PoolConfig::default());
	}

	#[test]
	fn test_thread_pools_lazy() {
		clear_thread_pools();
		assert!(!has_thread_pools());

		// Should initialize on first use
		let pools = thread_pools_lazy();
		assert!(has_thread_pools());

		clear_thread_pools();
	}
}
