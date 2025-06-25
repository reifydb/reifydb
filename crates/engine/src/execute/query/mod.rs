// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::frame::{Column, ColumnValues, Frame};
use reifydb_core::BitVec;

pub(crate) use compile::compile;
pub(crate) use filter::{FilterNode, FilterFunctionNode};
pub(crate) use limit::LimitNode;
pub(crate) use project::ProjectNode;
pub(crate) use scan::ScanFrameNode;

mod compile;
mod filter;
mod limit;
mod scan;
mod project;

#[derive(Debug, Clone)]
pub(crate) struct Batch {
    pub frame: Frame,
    pub mask: BitVec,
}

pub(crate) trait Node {
    fn next_batch(&mut self) -> Option<Batch>;
}

fn age_filter(frame: &Frame, row: usize) -> bool {
    frame.get_int4("age", row).map_or(false, |v| v > 5)
}

#[test]
fn test() {
    let frame = Frame::new(vec![Column {
        name: "age".to_string(),
        data: ColumnValues::int4([
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
        ]),
    }]);

    let scan = Box::new(ScanFrameNode::new(frame));
    let filter = Box::new(FilterFunctionNode::new(scan, age_filter));
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
