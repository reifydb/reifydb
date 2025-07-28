// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Complete buffer pool manager that coordinates all buffer pools.

use super::{BufferPool, PoolConfig, PoolStats, bitvec::BitVecPool, numeric::NumericPool};
use std::collections::HashMap;
use std::ops::Deref;
use std::rc::Rc;

/// Inner structure containing all the buffer pools.
#[derive(Debug)]
pub struct BufferPoolManagerInner {
    pub bool_pool: BitVecPool,

    // Numeric type pools
    pub i8_pool: NumericPool<i8>,
    pub i16_pool: NumericPool<i16>,
    pub i32_pool: NumericPool<i32>,
    pub i64_pool: NumericPool<i64>,
    pub i128_pool: NumericPool<i128>,
    pub u8_pool: NumericPool<u8>,
    pub u16_pool: NumericPool<u16>,
    pub u32_pool: NumericPool<u32>,
    pub u64_pool: NumericPool<u64>,
    pub u128_pool: NumericPool<u128>,
    pub f32_pool: NumericPool<f32>,
    pub f64_pool: NumericPool<f64>,

    // String pool for UTF-8 columns
    pub utf8_pool: NumericPool<String>,

    config: PoolConfig,
}

/// Complete buffer pool manager that manages all buffer types used in query execution.
#[derive(Debug)]
pub struct BufferPoolManager(Rc<BufferPoolManagerInner>);

impl BufferPoolManager {

    /// Create a new buffer pool manager with the given configuration.
    pub fn new(config: PoolConfig) -> Self {
        Self(Rc::new(BufferPoolManagerInner {
            bool_pool: BitVecPool::new(config.clone()),
            i8_pool: NumericPool::new(config.clone()),
            i16_pool: NumericPool::new(config.clone()),
            i32_pool: NumericPool::new(config.clone()),
            i64_pool: NumericPool::new(config.clone()),
            i128_pool: NumericPool::new(config.clone()),
            u8_pool: NumericPool::new(config.clone()),
            u16_pool: NumericPool::new(config.clone()),
            u32_pool: NumericPool::new(config.clone()),
            u64_pool: NumericPool::new(config.clone()),
            u128_pool: NumericPool::new(config.clone()),
            f32_pool: NumericPool::new(config.clone()),
            f64_pool: NumericPool::new(config.clone()),
            utf8_pool: NumericPool::new(config.clone()),
            config,
        }))
    }

    /// Get comprehensive statistics for all pools.
    pub fn get_all_stats(&self) -> HashMap<String, PoolStats> {
        let mut stats = HashMap::new();

        stats.insert("bool".to_string(), self.bool_pool.stats());
        stats.insert("i8".to_string(), self.i8_pool.stats());
        stats.insert("i16".to_string(), self.i16_pool.stats());
        stats.insert("i32".to_string(), self.i32_pool.stats());
        stats.insert("i64".to_string(), self.i64_pool.stats());
        stats.insert("i128".to_string(), self.i128_pool.stats());
        stats.insert("u8".to_string(), self.u8_pool.stats());
        stats.insert("u16".to_string(), self.u16_pool.stats());
        stats.insert("u32".to_string(), self.u32_pool.stats());
        stats.insert("u64".to_string(), self.u64_pool.stats());
        stats.insert("u128".to_string(), self.u128_pool.stats());
        stats.insert("f32".to_string(), self.f32_pool.stats());
        stats.insert("f64".to_string(), self.f64_pool.stats());

        stats.insert("utf8".to_string(), self.utf8_pool.stats());

        stats
    }

    /// Get aggregated statistics across all pools.
    pub fn get_aggregate_stats(&self) -> PoolStats {
        let all_stats = self.get_all_stats();
        let mut aggregate = PoolStats::new();

        for stats in all_stats.values() {
            aggregate.hits += stats.hits;
            aggregate.misses += stats.misses;
            aggregate.current_buffers += stats.current_buffers;
            aggregate.total_memory += stats.total_memory;
        }

        aggregate.update_hit_rate();

        // Calculate overall averages
        if aggregate.current_buffers > 0 {
            let total_size: usize = all_stats
                .values()
                .map(|s| (s.avg_buffer_size * s.current_buffers as f64) as usize)
                .sum();
            aggregate.avg_buffer_size = total_size as f64 / aggregate.current_buffers as f64;

            aggregate.max_buffer_size =
                all_stats.values().map(|s| s.max_buffer_size).max().unwrap_or(0);

            aggregate.min_buffer_size =
                all_stats.values().map(|s| s.min_buffer_size).filter(|&s| s > 0).min().unwrap_or(0);
        }

        aggregate
    }

    /// Clear all pools and reset statistics.
    pub fn clear_all(&mut self) {
        self.bool_pool.clear();
        self.i8_pool.clear();
        self.i16_pool.clear();
        self.i32_pool.clear();
        self.i64_pool.clear();
        self.i128_pool.clear();
        self.u8_pool.clear();
        self.u16_pool.clear();
        self.u32_pool.clear();
        self.u64_pool.clear();
        self.u128_pool.clear();
        self.f32_pool.clear();
        self.f64_pool.clear();
        self.utf8_pool.clear();
    }

    /// Trim excess buffers across all pools when memory usage is high.
    pub fn trim_excess(&mut self) {
        let aggregate_stats = self.get_aggregate_stats();
        let memory_mb = aggregate_stats.total_memory / (1024 * 1024);

        // If we're over the memory threshold, clear some pools
        if memory_mb > (self.config.max_memory_mb as f64 * self.config.trim_threshold) as usize {
            // Start with the largest pools first
            let mut pool_sizes: Vec<(String, usize)> = self
                .get_all_stats()
                .into_iter()
                .map(|(name, stats)| (name, stats.total_memory))
                .collect();

            pool_sizes.sort_by(|a, b| b.1.cmp(&a.1));

            // Clear pools starting with the largest until we're under threshold
            for (pool_name, _) in pool_sizes.iter().take(pool_sizes.len() / 2) {
                match pool_name.as_str() {
                    "bool" => self.bool_pool.clear(),
                    "i8" => self.i8_pool.clear(),
                    "i16" => self.i16_pool.clear(),
                    "i32" => self.i32_pool.clear(),
                    "i64" => self.i64_pool.clear(),
                    "i128" => self.i128_pool.clear(),
                    "u8" => self.u8_pool.clear(),
                    "u16" => self.u16_pool.clear(),
                    "u32" => self.u32_pool.clear(),
                    "u64" => self.u64_pool.clear(),
                    "u128" => self.u128_pool.clear(),
                    "f32" => self.f32_pool.clear(),
                    "f64" => self.f64_pool.clear(),
                    "utf8" => self.utf8_pool.clear(),
                    _ => {}
                }
            }
        }
    }

    /// Get a summary report of pool performance.
    pub fn get_performance_report(&self) -> String {
        let aggregate = self.get_aggregate_stats();
        let all_stats = self.get_all_stats();

        let mut report = String::new();
        report.push_str("=== Buffer Pool Performance Report ===\n");
        report.push_str(&format!("Overall Hit Rate: {:.2}%\n", aggregate.hit_rate * 100.0));
        report.push_str(&format!(
            "Total Memory: {:.2} MB\n",
            aggregate.total_memory as f64 / (1024.0 * 1024.0)
        ));
        report.push_str(&format!("Total Buffers: {}\n", aggregate.current_buffers));
        report.push_str(&format!("Hits: {}, Misses: {}\n", aggregate.hits, aggregate.misses));
        report.push_str("\n=== Pool Details ===\n");

        for (pool_name, stats) in all_stats.iter() {
            if stats.hits + stats.misses > 0 {
                report.push_str(&format!(
                    "{}: Hit Rate: {:.1}%, Buffers: {}, Memory: {:.1} KB\n",
                    pool_name,
                    stats.hit_rate * 100.0,
                    stats.current_buffers,
                    stats.total_memory as f64 / 1024.0
                ));
            }
        }

        report
    }
}

impl Deref for BufferPoolManager {
    type Target = BufferPoolManagerInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Clone for BufferPoolManager {
    fn clone(&self) -> Self {
        Self(Rc::clone(&self.0))
    }
}

impl Default for BufferPoolManager {
    fn default() -> Self {
        Self::new(PoolConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pool_manager_creation() {
        let mut manager = BufferPoolManager::new(PoolConfig::default());
        let stats = manager.get_aggregate_stats();

        // Initially all pools should be empty
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 0);
        assert_eq!(stats.current_buffers, 0);
        assert_eq!(stats.total_memory, 0);
    }

    #[test]
    fn test_buffer_pool_manager_stats() {
        let mut manager = BufferPoolManager::new(PoolConfig::default());

        // Generate some activity
        let _buf1 = manager.i32_pool.acquire(100);
        let _buf2 = manager.f64_pool.acquire(200);

        let all_stats = manager.get_all_stats();
        assert!(all_stats.contains_key("i32"));
        assert!(all_stats.contains_key("f64"));

        let aggregate = manager.get_aggregate_stats();
        assert_eq!(aggregate.hits + aggregate.misses, 2);
    }

    #[test]
    fn test_buffer_pool_manager_clear() {
        let mut manager = BufferPoolManager::new(PoolConfig::default());

        // Generate some activity
        {
            let _buf = manager.i32_pool.acquire(100);
        }

        manager.clear_all();

        let aggregate = manager.get_aggregate_stats();
        assert_eq!(aggregate.current_buffers, 0);
        assert_eq!(aggregate.total_memory, 0);
    }

    #[test]
    fn test_buffer_pool_manager_performance_report() {
        let manager = BufferPoolManager::new(PoolConfig::default());

        {
            let _buf1 = manager.i32_pool.acquire(100);
            let _buf2 = manager.f64_pool.acquire(200);
        }

        let report = manager.get_performance_report();
        assert!(report.contains("Buffer Pool Performance Report"));
        assert!(report.contains("Hit Rate"));
        assert!(report.contains("Total Memory"));
    }
}
