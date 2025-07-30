// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::layout::FrameLayout;
use crate::execute::{Batch, ExecutionContext, ExecutionPlan};
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
        while let Some(Batch { mut frame }) = self.input.next(ctx, rx)? {
            let row_count = frame.row_count();
            if row_count == 0 {
                continue;
            }
            return if row_count <= self.remaining {
                self.remaining -= row_count;
                Ok(Some(Batch { frame }))
            } else {
                frame.take(self.remaining)?;
                self.remaining = 0;
                Ok(Some(Batch { frame }))
            };
        }
        Ok(None)
    }

    fn layout(&self) -> Option<FrameLayout> {
        self.input.layout()
    }
}
