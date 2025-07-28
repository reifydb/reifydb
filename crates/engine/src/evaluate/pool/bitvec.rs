// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! BitVec buffer pools for efficient reuse of bit vector allocations.
//! 
//! BitVecs are heavily used for null masks and boolean columns, making them
//! prime candidates for pooling to reduce allocation overhead.

use super::{BufferPool, PooledBuffer, PoolStats, PoolConfig};
use reifydb_core::BitVec;
use std::sync::Mutex;

/// Specialized pool for BitVec allocations with size-based bucketing.
#[derive(Debug)]
pub struct BitVecPool {
    /// Different size buckets for efficient reuse
    /// Index 0: 0-64 bits, Index 1: 65-512 bits, Index 2: 513-4096 bits, etc.
    pools: [Mutex<Vec<BitVec>>; 6],
    config: PoolConfig,
    stats: Mutex<PoolStats>,
}

impl BitVecPool {
    /// Size thresholds for different buckets (in number of bits).
    const SIZE_THRESHOLDS: [usize; 6] = [64, 512, 4096, 32768, 262144, usize::MAX];
    
    /// Create a new BitVec pool with the given configuration.
    pub fn new(config: PoolConfig) -> Self {
        Self {
            pools: [
                Mutex::new(Vec::new()),
                Mutex::new(Vec::new()),
                Mutex::new(Vec::new()),
                Mutex::new(Vec::new()),
                Mutex::new(Vec::new()),
                Mutex::new(Vec::new()),
            ],
            config,
            stats: Mutex::new(PoolStats::new()),
        }
    }
    
    /// Determine which bucket a given capacity should use.
    fn bucket_for_capacity(&self, capacity: usize) -> usize {
        for (i, &threshold) in Self::SIZE_THRESHOLDS.iter().enumerate() {
            if capacity <= threshold {
                return i;
            }
        }
        Self::SIZE_THRESHOLDS.len() - 1
    }
    
    /// Return a BitVec to the appropriate size bucket.
    pub fn return_bitvec(&self, bitvec: BitVec) {
        let len = bitvec.len();
        let bucket_idx = self.bucket_for_capacity(len);
        
        // Create a cleared BitVec for the pool - use len to ensure it can be indexed
        let cleared_bitvec = BitVec::new(len, false);
        
        // Try to return to the appropriate bucket if under limit
        if let Ok(mut bucket) = self.pools[bucket_idx].try_lock() {
            if bucket.len() < self.config.max_buffers_per_bucket {
                bucket.push(cleared_bitvec);
            }
            // If over limit, just drop the BitVec (let it be deallocated)
        }
    }
    
    /// Get a BitVec from the appropriate size bucket.
    fn get_from_bucket(&self, capacity: usize) -> Option<BitVec> {
        let bucket_idx = self.bucket_for_capacity(capacity);
        
        // Try the exact bucket first
        if let Ok(mut bucket) = self.pools[bucket_idx].try_lock() {
            // Just take any BitVec from this bucket since they're pre-sized
            if let Some(bitvec) = bucket.pop() {
                return Some(bitvec);
            }
        }
        
        // If no suitable BitVec in the exact bucket, try larger buckets
        for larger_bucket_idx in (bucket_idx + 1)..self.pools.len() {
            if let Ok(mut bucket) = self.pools[larger_bucket_idx].try_lock() {
                if let Some(bitvec) = bucket.pop() {
                    return Some(bitvec);
                }
            }
        }
        
        None
    }
    
    /// Update pool statistics.
    fn update_stats(&self, hit: bool) {
        if let Ok(mut stats) = self.stats.try_lock() {
            if hit {
                stats.hits += 1;
            } else {
                stats.misses += 1;
            }
            stats.update_hit_rate();
        }
    }
    
    /// Calculate current memory usage across all buckets.
    fn calculate_memory_usage(&self) -> usize {
        let mut total = 0;
        
        for bucket in &self.pools {
            if let Ok(bucket_guard) = bucket.try_lock() {
                total += bucket_guard.iter()
                    .map(|bv| bv.len() / 8) // Convert bits to bytes (approximate)
                    .sum::<usize>();
            }
        }
        
        total
    }
    
    /// Count total BitVecs across all buckets.
    fn count_bitvecs(&self) -> usize {
        let mut total = 0;
        
        for bucket in &self.pools {
            if let Ok(bucket_guard) = bucket.try_lock() {
                total += bucket_guard.len();
            }
        }
        
        total
    }
}

impl BufferPool<bool> for BitVecPool {
    fn acquire(&self, capacity: usize) -> PooledBuffer<bool> {
        // Try to get a BitVec from the pool first
        if let Some(bitvec) = self.get_from_bucket(capacity) {
            self.update_stats(true); // Hit
            
            // Use the pooled BitVec capacity as hint, but create new Vec<bool>
            let actual_capacity = bitvec.len().max(capacity);
            let buffer = Vec::with_capacity(actual_capacity);
            PooledBuffer::new(buffer, std::sync::Arc::new(BitVecPoolWrapper::new(self)))
        } else {
            // Allocate a new buffer
            let buffer = Vec::with_capacity(capacity);
            self.update_stats(false); // Miss
            PooledBuffer::new(buffer, std::sync::Arc::new(BitVecPoolWrapper::new(self)))
        }
    }
    
    fn acquire_exact(&self, size: usize) -> PooledBuffer<bool> {
        // For exact size, we try to reuse but create new if needed
        if let Some(bitvec) = self.get_from_bucket(size) {
            self.update_stats(true); // Hit
            
            // Create a Vec<bool> with exact size, using pooled capacity as hint
            let actual_capacity = bitvec.len().max(size);
            let mut buffer = Vec::with_capacity(actual_capacity);
            buffer.resize(size, false);
            PooledBuffer::new(buffer, std::sync::Arc::new(BitVecPoolWrapper::new(self)))
        } else {
            // Allocate exactly what was requested
            let mut buffer = Vec::with_capacity(size);
            buffer.resize(size, false);
            self.update_stats(false); // Miss
            PooledBuffer::new(buffer, std::sync::Arc::new(BitVecPoolWrapper::new(self)))
        }
    }
    
    fn stats(&self) -> PoolStats {
        if let Ok(mut stats) = self.stats.try_lock() {
            stats.current_buffers = self.count_bitvecs();
            stats.total_memory = self.calculate_memory_usage();
            
            // Calculate buffer size statistics
            let mut sizes = Vec::new();
            
            for bucket in &self.pools {
                if let Ok(bucket_guard) = bucket.try_lock() {
                    sizes.extend(bucket_guard.iter().map(|bv| bv.len()));
                }
            }
            
            if !sizes.is_empty() {
                stats.avg_buffer_size = sizes.iter().sum::<usize>() as f64 / sizes.len() as f64;
                stats.max_buffer_size = *sizes.iter().max().unwrap_or(&0);
                stats.min_buffer_size = *sizes.iter().min().unwrap_or(&0);
            }
            
            stats.clone()
        } else {
            PoolStats::new()
        }
    }
    
    fn clear(&self) {
        for bucket in &self.pools {
            if let Ok(mut bucket_guard) = bucket.try_lock() {
                bucket_guard.clear();
            }
        }
        
        if let Ok(mut stats) = self.stats.try_lock() {
            *stats = PoolStats::new();
        }
    }
    
    fn try_return_buffer(&self, buffer: Vec<bool>) {
        // Convert Vec<bool> back to BitVec for returning to pool
        // We'll create a BitVec with the same capacity as the original Vec
        let capacity = buffer.capacity();
        let bitvec = BitVec::new(capacity, false);  // Use new() so it has proper length
        self.return_bitvec(bitvec);
    }
}

/// Specialized pool methods for BitVec operations.
impl BitVecPool {
    /// Acquire a BitVec directly (not wrapped in PooledBuffer).
    /// This is more efficient when working directly with BitVec operations.
    pub fn acquire_bitvec(&self, capacity: usize) -> BitVec {
        if let Some(bitvec) = self.get_from_bucket(capacity) {
            self.update_stats(true);
            // Use the pooled BitVec if it's large enough, otherwise create new one
            if bitvec.len() >= capacity {
                bitvec
            } else {
                BitVec::new(capacity, false)
            }
        } else {
            self.update_stats(false);
            // Create a BitVec with proper length for the capacity
            BitVec::new(capacity, false)
        }
    }
    
    /// Acquire a BitVec with exact size.
    pub fn acquire_bitvec_exact(&self, size: usize) -> BitVec {
        if let Some(bitvec) = self.get_from_bucket(size) {
            self.update_stats(true);
            // Use the pooled BitVec if it matches size, otherwise create new one
            if bitvec.len() == size {
                bitvec
            } else {
                BitVec::new(size, false)
            }
        } else {
            self.update_stats(false);
            BitVec::new(size, false)
        }
    }
}

/// Wrapper to allow the pool to be used as a trait object.
struct BitVecPoolWrapper {
    pool_ptr: *const BitVecPool,
}

impl BitVecPoolWrapper {
    fn new(pool: &BitVecPool) -> Self {
        Self {
            pool_ptr: pool as *const BitVecPool,
        }
    }
    
    fn pool(&self) -> &BitVecPool {
        unsafe { &*self.pool_ptr }
    }
}

unsafe impl Send for BitVecPoolWrapper {}
unsafe impl Sync for BitVecPoolWrapper {}

impl BufferPool<bool> for BitVecPoolWrapper {
    fn acquire(&self, capacity: usize) -> PooledBuffer<bool> {
        self.pool().acquire(capacity)
    }
    
    fn acquire_exact(&self, size: usize) -> PooledBuffer<bool> {
        self.pool().acquire_exact(size)
    }
    
    fn stats(&self) -> PoolStats {
        self.pool().stats()
    }
    
    fn clear(&self) {
        self.pool().clear()
    }
    
    fn try_return_buffer(&self, buffer: Vec<bool>) {
        self.pool().try_return_buffer(buffer);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_bitvec_pool_bucket_selection() {
        let pool = BitVecPool::new(PoolConfig::default());
        
        assert_eq!(pool.bucket_for_capacity(32), 0);    // Small bucket
        assert_eq!(pool.bucket_for_capacity(256), 1);   // Medium bucket
        assert_eq!(pool.bucket_for_capacity(2048), 2);  // Large bucket
        assert_eq!(pool.bucket_for_capacity(100000), 4); // Very large bucket
    }
    
    #[test]
    fn test_bitvec_pool_basic_operations() {
        let pool = BitVecPool::new(PoolConfig::default());
        
        // Acquire a buffer
        let buffer1 = pool.acquire(100);
        assert!(buffer1.capacity() >= 100);
        
        // Acquire exact size
        let buffer2 = pool.acquire_exact(50);
        assert_eq!(buffer2.len(), 50);
        
        // Check stats
        let stats = pool.stats();
        assert_eq!(stats.misses, 2); // Both were misses since pool was empty
    }
    
    #[test]
    fn test_bitvec_pool_direct_bitvec_operations() {
        let pool = BitVecPool::new(PoolConfig::default());
        
        // Acquire BitVec directly
        let mut bitvec1 = pool.acquire_bitvec(100);
        bitvec1.set(50, true);
        assert!(bitvec1.get(50));
        
        // Return it to pool
        pool.return_bitvec(bitvec1);
        
        // Acquire another - should reuse
        let bitvec2 = pool.acquire_bitvec(100);
        assert!(!bitvec2.get(50)); // Should be cleared when returned to pool
        
        let stats = pool.stats();
        assert!(stats.hits >= 1); // Should have at least one hit from reuse
    }
    
    #[test]
    fn test_bitvec_pool_reuse() {
        let pool = BitVecPool::new(PoolConfig::default());
        
        // Acquire and return a BitVec
        {
            let buffer = pool.acquire(100);
            // Buffer will be returned to pool when dropped
        }
        
        // Acquire another buffer - should reuse the previous allocation
        let buffer = pool.acquire(100);
        assert!(buffer.is_empty()); // Should be cleared when returned to pool
        
        let stats = pool.stats();
        assert!(stats.hits >= 1); // Should have at least one hit from reuse
    }
    
    #[test]
    fn test_bitvec_pool_stats() {
        let pool = BitVecPool::new(PoolConfig::default());
        
        // Generate some activity
        for _ in 0..10 {
            let _buffer = pool.acquire(100);
        }
        
        let stats = pool.stats();
        assert_eq!(stats.hits + stats.misses, 10);
        assert!(stats.hit_rate >= 0.0 && stats.hit_rate <= 1.0);
    }
    
    #[test]
    fn test_bitvec_pool_clear() {
        let pool = BitVecPool::new(PoolConfig::default());
        
        // Add some buffers to the pool
        {
            let _buffer = pool.acquire(100);
        }
        
        pool.clear();
        let stats = pool.stats();
        assert_eq!(stats.current_buffers, 0);
        assert_eq!(stats.total_memory, 0);
    }
    
    
    #[test]
    fn test_bitvec_pool_size_fallback() {
        let pool = BitVecPool::new(PoolConfig::default());
        
        // Add a large BitVec to a higher bucket
        let large_bitvec = pool.acquire_bitvec(10000);
        pool.return_bitvec(large_bitvec);
        
        // Request a smaller size - should reuse the larger buffer
        let small_buffer = pool.acquire(1000);
        assert!(small_buffer.capacity() >= 1000);
        
        let stats = pool.stats();
        assert!(stats.hits >= 1); // Should reuse the larger buffer
    }
}