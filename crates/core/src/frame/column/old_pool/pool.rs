// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::PoolStatistics;
use crate::frame::column::old_pool::container::Container;
use std::cell::RefCell;
use std::rc::Rc;

/// Core trait for buffer pools that manage reusable buffers of a specific type.
pub trait Pool<C, T>
where
    C: Container<T> + 'static,
{
    /// Acquire a buffer with at least the specified capacity.
    /// The returned buffer may have larger capacity for better reuse.
    fn acquire(&self, capacity: usize) -> BufferedPool<C>;
    /// Get current pool statistics for monitoring and tuning.
    fn stats(&self) -> PoolStatistics;
    /// Release all buffers and free memory. Used for cleanup.
    fn clear(&self);
    /// Return a buffer to the pool for reuse (if possible).
    fn release(&self, buffer: C);
}

pub struct BufferedPool<C, T>
where
    C: Container + 'static,
{
    buffer: C,
    pool: Option<Rc<RefCell<dyn Pool<C>>>>,
}

impl<B> BufferedPool<B>
where
    B: Container + 'static,
{
    pub(crate) fn new(buffer: B, pool: Rc<RefCell<dyn Pool<B>>>) -> Self {
        Self { buffer, pool: Some(pool) }
    }
}

impl<B> Drop for BufferedPool<B>
where
    B: Container + 'static,
{
    fn drop(&mut self) {
        if let Some(pool) = self.pool.take() {
            let buffer = std::mem::take(&mut self.buffer);
            pool.borrow_mut().release(buffer);
        }
    }
}

impl<B> Default for BufferedPool<B>
where
    B: 'static + Container,
{
    fn default() -> Self {
        todo!()
    }
}

impl<B> Container for BufferedPool<B>
where
    B: Container + 'static,
{
    fn is_empty(&self) -> bool {
        Container::is_empty(&self.buffer)
    }

    fn capacity(&self) -> usize {
        todo!()
    }
}
