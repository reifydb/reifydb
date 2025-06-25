// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::query::{NextBatch, Node};
use crate::frame::{Frame, FrameLayout};
use reifydb_core::BitVec;

pub(crate) struct ScanFrameNode {
    frame: Option<Frame>,
    layout: Option<FrameLayout>,
}

impl ScanFrameNode {
    pub fn new(frame: Frame) -> Self {
        Self { frame: Some(frame), layout: None }
    }
}

impl Node for ScanFrameNode {
    fn next_batch(&mut self) -> crate::Result<NextBatch> {
        if let Some(layout) = &self.layout {
            return Ok(NextBatch::None { layout: layout.clone() });
        }


        let frame = self.frame.take().unwrap();
        self.layout = Some(FrameLayout::from_frame(&frame));
        
        let mask = BitVec::new(frame.row_count(), true);
        Ok(NextBatch::Some { frame, mask })
    }
}
