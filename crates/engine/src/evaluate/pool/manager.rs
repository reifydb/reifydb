// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Complete buffer pool manager that coordinates all buffer pools.

use super::{BufferPool, PoolConfig, PoolStats, bitvec::BitVecPool, numeric::NumericPool};
use std::collections::HashMap;
use std::sync::Arc;

/// Complete buffer pool manager that manages all buffer types used in query execution.
#[derive(Debug)]
pub struct BufferPoolManager {
    // Numeric type pools
    pub bool_pool: Arc<BitVecPool>,
    pub i8_pool: Arc<NumericPool<i8>>,
    pub i16_pool: Arc<NumericPool<i16>>,
    pub i32_pool: Arc<NumericPool<i32>>,
    pub i64_pool: Arc<NumericPool<i64>>,
    pub i128_pool: Arc<NumericPool<i128>>,
    pub u8_pool: Arc<NumericPool<u8>>,
    pub u16_pool: Arc<NumericPool<u16>>,
    pub u32_pool: Arc<NumericPool<u32>>,
    pub u64_pool: Arc<NumericPool<u64>>,
    pub u128_pool: Arc<NumericPool<u128>>,
    pub f32_pool: Arc<NumericPool<f32>>,
    pub f64_pool: Arc<NumericPool<f64>>,

    // String pool for UTF-8 columns
    pub utf8_pool: Arc<NumericPool<String>>,

    config: PoolConfig,
}

impl BufferPoolManager {
    /// Create a new buffer pool manager with the given configuration.
    pub fn new(config: PoolConfig) -> Self {
        Self {
            bool_pool: Arc::new(BitVecPool::new(config.clone())),
            i8_pool: Arc::new(NumericPool::new(config.clone())),
            i16_pool: Arc::new(NumericPool::new(config.clone())),
            i32_pool: Arc::new(NumericPool::new(config.clone())),
            i64_pool: Arc::new(NumericPool::new(config.clone())),
            i128_pool: Arc::new(NumericPool::new(config.clone())),
            u8_pool: Arc::new(NumericPool::new(config.clone())),
            u16_pool: Arc::new(NumericPool::new(config.clone())),
            u32_pool: Arc::new(NumericPool::new(config.clone())),
            u64_pool: Arc::new(NumericPool::new(config.clone())),
            u128_pool: Arc::new(NumericPool::new(config.clone())),
            f32_pool: Arc::new(NumericPool::new(config.clone())),
            f64_pool: Arc::new(NumericPool::new(config.clone())),
            utf8_pool: Arc::new(NumericPool::new(config.clone())),
            config,
        }
    }

    /// Get configuration.
    pub fn config(&self) -> &PoolConfig {
        &self.config
    }

    /// Update configuration for all pools.
    pub fn update_config(&mut self, config: PoolConfig) {
        self.config = config;
        // Note: Individual pools keep their own config copies,
        // so this mainly affects future operations
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
    pub fn clear_all(&self) {
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
    pub fn trim_excess(&self) {
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

    /// Auto-tune pool configurations based on usage patterns.
    pub fn auto_tune(&mut self, window_stats: &HashMap<String, PoolStats>) {
        // Adjust max_buffers_per_bucket based on hit rates
        let mut new_config = self.config.clone();

        let total_hit_rate: f64 =
            window_stats.values().map(|s| s.hit_rate).sum::<f64>() / window_stats.len() as f64;

        // If hit rate is low, increase buffer limits
        if total_hit_rate < 0.5 {
            new_config.max_buffers_per_bucket =
                (new_config.max_buffers_per_bucket as f64 * 1.5) as usize;
        }
        // If hit rate is very high and memory usage is low, we could decrease limits
        else if total_hit_rate > 0.9 {
            let memory_usage_ratio = self.get_aggregate_stats().total_memory as f64
                / (self.config.max_memory_mb * 1024 * 1024) as f64;

            if memory_usage_ratio < 0.3 {
                new_config.max_buffers_per_bucket =
                    (new_config.max_buffers_per_bucket as f64 * 0.8) as usize;
            }
        }

        // Ensure reasonable bounds
        new_config.max_buffers_per_bucket = new_config.max_buffers_per_bucket.clamp(16, 256);

        self.update_config(new_config);
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
        let manager = BufferPoolManager::new(PoolConfig::default());
        let stats = manager.get_aggregate_stats();

        // Initially all pools should be empty
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 0);
        assert_eq!(stats.current_buffers, 0);
        assert_eq!(stats.total_memory, 0);
    }

    #[test]
    fn test_buffer_pool_manager_stats() {
        let manager = BufferPoolManager::new(PoolConfig::default());

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
        let manager = BufferPoolManager::new(PoolConfig::default());

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

        // Generate some activity
        {
            let _buf1 = manager.i32_pool.acquire(100);
            let _buf2 = manager.f64_pool.acquire(200);
        }

        let report = manager.get_performance_report();
        assert!(report.contains("Buffer Pool Performance Report"));
        assert!(report.contains("Hit Rate"));
        assert!(report.contains("Total Memory"));
    }

    #[test]
    fn test_buffer_pool_manager_auto_tune() {
        let mut manager = BufferPoolManager::new(PoolConfig::default());
        let initial_limit = manager.config.max_buffers_per_bucket;

        // Simulate poor hit rate
        let mut poor_stats = HashMap::new();
        poor_stats.insert(
            "i32".to_string(),
            PoolStats { hits: 1, misses: 9, hit_rate: 0.1, ..PoolStats::new() },
        );

        manager.auto_tune(&poor_stats);
        assert!(manager.config.max_buffers_per_bucket > initial_limit);
    }
}
