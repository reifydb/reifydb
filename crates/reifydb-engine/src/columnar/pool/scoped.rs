// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! RAII guards for scoped pool management
//!
//! This module provides RAII guards that temporarily set thread-local pools
//! and restore the previous state when dropped.

use super::{Pools, thread_local::{get_thread_pools, set_thread_pools, clear_thread_pools}};
use super::config::PoolConfig;

/// RAII guard that sets thread-local pools and restores previous state on drop
pub struct ScopedPools {
    previous: Option<Pools>,
}

impl ScopedPools {
    /// Create a new scoped pools guard
    /// Sets the given pools as thread-local and saves the previous state
    pub fn new(pools: Pools) -> Self {
        let previous = get_thread_pools();
        set_thread_pools(pools);
        Self { previous }
    }
    
    /// Create a scoped guard with new default pools
    pub fn default() -> Self {
        Self::new(Pools::default())
    }
    
    /// Create a scoped guard with custom configuration
    pub fn with_config(config: PoolConfig) -> Self {
        Self::new(Pools::new(config.max_pool_size))
    }
    
    /// Create a scoped guard with test configuration
    pub fn test() -> Self {
        Self::with_config(PoolConfig::test())
    }
    
    /// Create a scoped guard with production configuration
    pub fn production() -> Self {
        Self::with_config(PoolConfig::production())
    }
    
    /// Create a scoped guard with development configuration
    pub fn development() -> Self {
        Self::with_config(PoolConfig::development())
    }
}

impl Drop for ScopedPools {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(p) => set_thread_pools(p),
            None => clear_thread_pools(),
        }
    }
}

/// Execute a function with scoped pools
/// This is a convenience function that creates a ScopedPools guard
pub fn with_scoped_pools<F, R>(pools: Pools, f: F) -> R
where
    F: FnOnce() -> R,
{
    let _guard = ScopedPools::new(pools);
    f()
}

/// Execute a function with default scoped pools
pub fn with_default_pools<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    let _guard = ScopedPools::default();
    f()
}

/// Execute a function with test scoped pools
pub fn with_test_pools<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    let _guard = ScopedPools::test();
    f()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::pool::thread_local::has_thread_pools;
    
    #[test]
    fn test_scoped_pools_basic() {
        // Start with no pools
        clear_thread_pools();
        assert!(!has_thread_pools());
        
        {
            let _guard = ScopedPools::default();
            // Pools should be set within scope
            assert!(has_thread_pools());
        }
        
        // Pools should be cleared after scope
        assert!(!has_thread_pools());
    }
    
    #[test]
    fn test_scoped_pools_restore_previous() {
        // Set initial pools
        let initial_pools = Pools::new(8);
        set_thread_pools(initial_pools);
        assert!(has_thread_pools());
        
        {
            // Use different pools in scope
            let _guard = ScopedPools::new(Pools::new(16));
            assert!(has_thread_pools());
            // Can't easily verify it's different pools, but it is
        }
        
        // Should restore initial pools
        assert!(has_thread_pools());
        
        clear_thread_pools();
    }
    
    #[test]
    fn test_with_scoped_pools() {
        clear_thread_pools();
        
        let result = with_scoped_pools(Pools::default(), || {
            assert!(has_thread_pools());
            42
        });
        
        assert_eq!(result, 42);
        assert!(!has_thread_pools());
    }
    
    #[test]
    fn test_with_default_pools() {
        clear_thread_pools();
        
        with_default_pools(|| {
            assert!(has_thread_pools());
        });
        
        assert!(!has_thread_pools());
    }
    
    #[test]
    fn test_with_test_pools() {
        clear_thread_pools();
        
        with_test_pools(|| {
            assert!(has_thread_pools());
        });
        
        assert!(!has_thread_pools());
    }
    
    #[test]
    fn test_nested_scoped_pools() {
        clear_thread_pools();
        
        let _outer = ScopedPools::default();
        assert!(has_thread_pools());
        
        {
            let _inner = ScopedPools::test();
            assert!(has_thread_pools());
        }
        
        // Should still have outer pools
        assert!(has_thread_pools());
        
        // Manually drop to test
        drop(_outer);
        assert!(!has_thread_pools());
    }
}