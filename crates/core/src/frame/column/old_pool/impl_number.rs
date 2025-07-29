// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::column::old_pool::pool::{BufferedPool, Pool};
use crate::frame::{PoolConfig, PoolStatistics};
use crate::value::IsNumber;
use std::cell::RefCell;

#[derive(Debug)]
pub struct NumberPool<T>
where
    T: IsNumber + 'static,
{
    pools: [RefCell<Vec<Vec<T>>>; 6],
    config: PoolConfig,
    stats: RefCell<PoolStatistics>,
}

impl<T> NumberPool<T>
where
    T: IsNumber + 'static,
{
    pub fn new(config: PoolConfig) -> Self {
        Self {
            pools: [
                RefCell::new(Vec::new()),
                RefCell::new(Vec::new()),
                RefCell::new(Vec::new()),
                RefCell::new(Vec::new()),
                RefCell::new(Vec::new()),
                RefCell::new(Vec::new()),
            ],
            config,
            stats: RefCell::new(PoolStatistics::new()),
        }
    }
}

impl<T> Pool<Vec<T>> for NumberPool<T>
where
    T: IsNumber + 'static,
{
    fn acquire(&self, capacity: usize) -> BufferedPool<Vec<T>> {
        todo!()
    }

    fn stats(&self) -> PoolStatistics {
        todo!()
    }

    fn clear(&self) {
        todo!()
    }

    fn release(&self, buffer: Vec<T>) {
        todo!()
    }
}
