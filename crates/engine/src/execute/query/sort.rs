// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::execute::{Batch, ExecutionContext, ExecutionPlan};
use reifydb_core::frame::{Frame, FrameLayout};
use reifydb_core::SortDirection::{Asc, Desc};
use reifydb_core::error::diagnostic::query;
use reifydb_core::interface::Rx;
use reifydb_core::{BitVec, SortKey, error};
use std::cmp::Ordering::Equal;

pub(crate) struct SortNode {
    input: Box<dyn ExecutionPlan>,
    by: Vec<SortKey>,
}

impl SortNode {
    pub(crate) fn new(input: Box<dyn ExecutionPlan>, by: Vec<SortKey>) -> Self {
        Self { input, by }
    }
}

impl ExecutionPlan for SortNode {
    fn next(&mut self, ctx: &ExecutionContext, rx: &mut dyn Rx) -> crate::Result<Option<Batch>> {
        let mut frame_opt: Option<Frame> = None;
        let mut mask_opt: Option<BitVec> = None;

        while let Some(Batch { frame, mask }) = self.input.next(ctx, rx)? {
            if let Some(existing_frame) = &mut frame_opt {
                for (i, col) in frame.columns.into_iter().enumerate() {
                    existing_frame.columns[i].values.extend(col.values)?;
                }
            } else {
                frame_opt = Some(frame);
            }

            if let Some(existing_mask) = &mut mask_opt {
                existing_mask.extend(&mask);
            } else {
                mask_opt = Some(mask);
            }
        }

        let mut frame = match frame_opt {
            Some(f) => f,
            None => return Ok(None),
        };

        let key_refs = self
            .by
            .iter()
            .map(|key| {
                let col = frame
                    .columns
                    .iter()
                    .find(|c| c.qualified_name() == key.column.fragment || c.name == key.column.fragment)
                    .ok_or_else(|| error!(query::column_not_found(key.column.clone())))?;
                Ok::<_, reifydb_core::Error>((&col.values, &key.direction))
            })
            .collect::<crate::Result<Vec<_>>>()?;

        let row_count = frame.row_count();
        let mut indices: Vec<usize> = (0..row_count).collect();

        indices.sort_unstable_by(|&l, &r| {
            for (col, dir) in &key_refs {
                let vl = col.get(l);
                let vr = col.get(r);
                let ord = vl.partial_cmp(&vr).unwrap_or(Equal);
                let ord = match dir {
                    Asc => ord,
                    Desc => ord.reverse(),
                };
                if ord != Equal {
                    return ord;
                } else {
                }
            }
            Equal
        });

        for col in &mut frame.columns {
            col.values.reorder(&indices);
        }

        Ok(Some(Batch { frame, mask: mask_opt.unwrap() }))
    }

    fn layout(&self) -> Option<FrameLayout> {
        self.input.layout()
    }
}
