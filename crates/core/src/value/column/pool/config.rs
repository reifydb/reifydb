// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Pool configuration for ftokenizeible initialization
//!
//! This module provides configuration options for thread-local pools,
//! allowing different configurations for test, development, and production.

use super::{Pools, thread_local::set_thread_pools};

/// Configuration for pool initialization
#[derive(Clone, Debug)]
pub struct PoolConfig {
	/// Maximum number of containers per capacity bucket
	pub max_pool_size: usize,

	/// Enable detailed statistics tracking
	pub enable_statistics: bool,

	/// Automatically clear pools when this many containers are stored
	pub auto_clear_threshold: Option<usize>,

	/// Pre-warm pools with containers of these capacities
	pub prewarm_capacities: Vec<usize>,
}

impl Default for PoolConfig {
	fn default() -> Self {
		Self {
			max_pool_size: 16,
			enable_statistics: cfg!(debug_assertions),
			auto_clear_threshold: None,
			prewarm_capacities: vec![],
		}
	}
}

impl PoolConfig {
	/// Create a configuration optimized for testing
	/// Uses smaller pools to catch memory issues faster
	pub fn test() -> Self {
		Self {
			max_pool_size: 4,
			enable_statistics: true,
			auto_clear_threshold: Some(100),
			prewarm_capacities: vec![],
		}
	}

	/// Create a configuration optimized for production
	/// Uses larger pools and pre-warming for better performance
	pub fn production() -> Self {
		Self {
			max_pool_size: 32,
			enable_statistics: false,
			auto_clear_threshold: Some(10000),
			prewarm_capacities: vec![16, 64, 256, 1024],
		}
	}

	/// Create a configuration for development
	/// Balance between test and production
	pub fn development() -> Self {
		Self {
			max_pool_size: 16,
			enable_statistics: true,
			auto_clear_threshold: Some(1000),
			prewarm_capacities: vec![16, 64],
		}
	}
}

/// Initialize thread-local pools with configuration
pub fn init_thread_pools(config: PoolConfig) -> Pools {
	let pools = Pools::new(config.max_pool_size);

	// Pre-warm pools if requested
	if !config.prewarm_capacities.is_empty() {
		// TODO: Implement pre-warming logic
		// This would acquire and immediately release containers of
		// specified capacities to ensure the pools have containers
		// ready for common sizes
	}

	set_thread_pools(pools.clone());
	pools
}

/// Initialize with default configuration
pub fn init_default_thread_pools() -> Pools {
	init_thread_pools(PoolConfig::default())
}

/// Initialize with test configuration
pub fn init_test_pools() -> Pools {
	init_thread_pools(PoolConfig::test())
}

/// Initialize with production configuration
pub fn init_production_pools() -> Pools {
	init_thread_pools(PoolConfig::production())
}

/// Initialize with development configuration
pub fn init_development_pools() -> Pools {
	init_thread_pools(PoolConfig::development())
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::value::column::pool::thread_local::{clear_thread_pools, has_thread_pools};

	#[test]
	fn test_pool_config_defaults() {
		let config = PoolConfig::default();
		assert_eq!(config.max_pool_size, 16);
		assert_eq!(config.enable_statistics, cfg!(debug_assertions));
		assert_eq!(config.auto_clear_threshold, None);
		assert!(config.prewarm_capacities.is_empty());
	}

	#[test]
	fn test_pool_config_test() {
		let config = PoolConfig::test();
		assert_eq!(config.max_pool_size, 4);
		assert!(config.enable_statistics);
		assert_eq!(config.auto_clear_threshold, Some(100));
		assert!(config.prewarm_capacities.is_empty());
	}

	#[test]
	fn test_pool_config_production() {
		let config = PoolConfig::production();
		assert_eq!(config.max_pool_size, 32);
		assert!(!config.enable_statistics);
		assert_eq!(config.auto_clear_threshold, Some(10000));
		assert_eq!(config.prewarm_capacities, vec![16, 64, 256, 1024]);
	}

	#[test]
	fn test_init_thread_pools() {
		// Clear any existing pools
		clear_thread_pools();
		assert!(!has_thread_pools());

		// Initialize with test config
		let pools = init_test_pools();
		assert!(has_thread_pools());

		// Clean up
		clear_thread_pools();
	}

	#[test]
	fn test_init_variations() {
		// Test each initialization variant
		clear_thread_pools();

		init_default_thread_pools();
		assert!(has_thread_pools());
		clear_thread_pools();

		init_development_pools();
		assert!(has_thread_pools());
		clear_thread_pools();

		init_production_pools();
		assert!(has_thread_pools());
		clear_thread_pools();
	}
}
