// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::query::{Batch, Node};

pub(crate) struct LimitNode {
    input: Box<dyn Node>,
    remaining: usize,
}

impl LimitNode {
    pub(crate) fn new(input: Box<dyn Node>, limit: usize) -> Self {
        Self { input, remaining: limit }
    }
}

impl Node for LimitNode {
    fn next_batch(&mut self) -> Option<Batch> {
        while let Some(mut batch) = self.input.next_batch() {
            let visible: usize = batch.mask.count_ones();
            if visible == 0 {
                continue;
            }
            return if visible <= self.remaining {
                self.remaining -= visible;
                Some(batch)
            } else {
                let mut kept = 0;
                for i in 0..batch.mask.len() {
                    if batch.mask.get(i) {
                        if kept >= self.remaining {
                            batch.mask.set(i, false);
                        } else {
                            kept += 1;
                        }
                    }
                }
                self.remaining = 0;
                Some(batch)
            };
        }
        None
    }
}
