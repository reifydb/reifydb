// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::BitVec;
use crate::frame::column::old_pool::pool::{BufferedPool, Pool};
use crate::frame::{PoolConfig, PoolStatistics};
use std::cell::RefCell;

#[derive(Debug)]
pub struct BooleanPool {
    pools: [RefCell<Vec<BitVec>>; 6],
    config: PoolConfig,
    stats: RefCell<PoolStatistics>,
}

impl BooleanPool {
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

impl Pool<BitVec> for BooleanPool {
    fn acquire(&self, capacity: usize) -> BufferedPool<BitVec> {
        todo!()
    }

    fn stats(&self) -> PoolStatistics {
        todo!()
    }

    fn clear(&self) {
        todo!()
    }

    fn release(&self, buffer: BitVec) {
        todo!()
    }
}
