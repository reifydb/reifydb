// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::query::{NextBatch, Node};
use crate::frame::FrameLayout;

pub(crate) struct LimitNode {
    input: Box<dyn Node>,
    remaining: usize,
    layout: Option<FrameLayout>,
}

impl LimitNode {
    pub(crate) fn new(input: Box<dyn Node>, limit: usize) -> Self {
        Self { input, remaining: limit, layout: None }
    }
}

impl Node for LimitNode {
    fn next_batch(&mut self) -> crate::Result<NextBatch> {
        loop {
            return match self.input.next_batch()? {
                NextBatch::Some { frame, mut mask } => {
                    let visible: usize = mask.count_ones();
                    if visible == 0 {
                        continue;
                    }
                    if visible <= self.remaining {
                        self.remaining -= visible;
                        Ok(NextBatch::Some { frame, mask })
                    } else {
                        let mut kept = 0;
                        for i in 0..mask.len() {
                            if mask.get(i) {
                                if kept >= self.remaining {
                                    mask.set(i, false);
                                } else {
                                    kept += 1;
                                }
                            }
                        }
                        self.remaining = 0;
                        self.layout = Some(FrameLayout::from_frame(&frame));
                        Ok(NextBatch::Some { frame, mask })
                    }
                }
                NextBatch::None { layout } => {
                    Ok(NextBatch::None { layout: self.layout.clone().unwrap_or(layout) })
                }
            };
        }
    }
}
