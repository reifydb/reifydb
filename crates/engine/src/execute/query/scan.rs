// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::query::{Batch, Node};
use crate::frame::Frame;
use reifydb_core::BitVec;

pub(crate) struct ScanFrameNode {
    frame: Option<Frame>,
}

impl ScanFrameNode {
    pub fn new(frame: Frame) -> Self {
        Self { frame: Some(frame) }
    }
}

impl Node for ScanFrameNode {
    fn next_batch(&mut self) -> Option<Batch> {
        let frame = self.frame.take()?;
        let mask = BitVec::new(frame.row_count(), true);
        Some(Batch { frame, mask })
    }
}
