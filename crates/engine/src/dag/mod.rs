// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::frame::{Column, ColumnValues, Frame};
use reifydb_core::BitVec;

#[derive(Debug, Clone)]
pub struct Batch {
    pub frame: Frame,
    pub mask: BitVec,
}

pub trait BatchIter {
    fn next_batch(&mut self) -> Option<Batch>;
}

pub struct ScanNode {
    frame: Option<Frame>,
}

impl ScanNode {
    pub fn new(frame: Frame) -> Self {
        Self { frame: Some(frame) }
    }
}

impl BatchIter for ScanNode {
    fn next_batch(&mut self) -> Option<Batch> {
        let frame = self.frame.take()?;
        let mask = BitVec::new(frame.row_count(), true);
        Some(Batch { frame, mask })
    }
}

pub struct FilterNode<I: BatchIter, F: Fn(&Frame, usize) -> bool> {
    input: I,
    predicate: F,
}

impl<I: BatchIter, F: Fn(&Frame, usize) -> bool> FilterNode<I, F> {
    pub fn new(input: I, predicate: F) -> Self {
        Self { input, predicate }
    }
}

impl<I: BatchIter, F: Fn(&Frame, usize) -> bool> BatchIter for FilterNode<I, F> {
    fn next_batch(&mut self) -> Option<Batch> {
        while let Some(mut batch) = self.input.next_batch() {
            for i in 0..batch.frame.row_count() {
                if batch.mask.get(i) {
                    if !(self.predicate)(&batch.frame, i) {
                        batch.mask.set(i, false);
                    }
                }
            }
            if batch.mask.any(){
                return Some(batch);
            }
        }
        None
    }
}

pub struct LimitNode<I: BatchIter> {
    input: I,
    remaining: usize,
}

impl<I: BatchIter> LimitNode<I> {
    pub fn new(input: I, limit: usize) -> Self {
        Self { input, remaining: limit }
    }
}

impl<I: BatchIter> BatchIter for LimitNode<I> {
    fn next_batch(&mut self) -> Option<Batch> {
        while let Some(mut batch) = self.input.next_batch() {
            let visible: usize = batch.mask.count_ones();
            if visible == 0 {
                continue;
            }

            if visible <= self.remaining {
                self.remaining -= visible;
                return Some(batch);
            } else {
                // Truncate mask to preserve only first N `true` bits
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
                return Some(batch);
            }
        }
        None
    }
}

fn age_filter(frame: &Frame, row: usize) -> bool {
    frame.get_int4("age", row).map_or(false, |v| v > 12)
}

#[test]
fn test() {
    let frame = Frame::new(vec![Column {
        name: "age".to_string(),
        data: ColumnValues::int4([
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
        ]),
    }]);

    let scan = ScanNode::new(frame);
    let filter = FilterNode::new(scan, age_filter);
    let mut limit = LimitNode::new(filter, 100);

    while let Some(batch) = limit.next_batch() {
        println!("Filtered batch:");
        for i in 0..batch.frame.row_count() {
            if batch.mask.get(i) {
                let age = batch
                    .frame
                    .get_int4("age", i)
                    .map(|v| v.to_string())
                    .unwrap_or("UNDEFINED".into());
                println!("  - age: {}", age);
            }
        }
    }
}
