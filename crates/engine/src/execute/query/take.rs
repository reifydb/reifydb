// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::execute::{Batch, ExecutionContext, ExecutionPlan};
use reifydb_core::frame::FrameLayout;
use reifydb_core::interface::Rx;

pub(crate) struct TakeNode {
    input: Box<dyn ExecutionPlan>,
    remaining: usize,
}

impl TakeNode {
    pub(crate) fn new(input: Box<dyn ExecutionPlan>, take: usize) -> Self {
        Self { input, remaining: take }
    }
}

impl ExecutionPlan for TakeNode {
    fn next(&mut self, ctx: &ExecutionContext, rx: &mut dyn Rx) -> crate::Result<Option<Batch>> {
        while let Some(Batch { frame, mut mask }) = self.input.next(ctx, rx)? {
            let visible: usize = mask.count_ones();
            if visible == 0 {
                continue;
            }
            return if visible <= self.remaining {
                self.remaining -= visible;
                Ok(Some(Batch { frame, mask }))
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
                Ok(Some(Batch { frame, mask }))
            };
        }
        Ok(None)
    }

    fn layout(&self) -> Option<FrameLayout> {
        self.input.layout()
    }
}
