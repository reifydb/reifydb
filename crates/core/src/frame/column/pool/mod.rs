// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::column::container::Container;
use std::cell::RefCell;
use std::rc::Rc;

/// Core trait for pools that manage reusable containers of a specific type.
pub trait Pool<C, T>
where
    T: Default,
    C: Container<T> + 'static,
{
    /// Acquire a container with at least the specified capacity.
    /// The returned container may have larger capacity for better reuse.
    fn acquire(&self, capacity: usize) -> C;
    /// Release all containers and free memory. Used for cleanup.
    fn clear(&self);
    /// Return a container to the pool for reuse (if possible).
    fn release(&self, container: C);
}

/// A pooled container that automatically returns to its pool when dropped.
/// Provides container interface while maintaining pool integration.
pub struct PooledContainer<C, T> {
    container: C,
    pool: Option<Rc<RefCell<dyn Pool<C, T>>>>,
}
