// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::query::{Batch, Node};
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
    fn next(&mut self) -> crate::Result<Option<Batch>> {
        if let Some(frame) = self.frame.take() {
            self.layout = Some(FrameLayout::from_frame(&frame));
            let mask = BitVec::new(frame.row_count(), true);
            Ok(Some(Batch { frame, mask }))
        } else {
            Ok(None)
        }
    }

    fn layout(&self) -> Option<FrameLayout> {
        self.layout.clone()
    }
}
