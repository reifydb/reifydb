// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::execute::{Batch, ExecutionContext, ExecutionPlan};
use reifydb_core::SortDirection::{Asc, Desc};
use reifydb_core::error::diagnostic::query;
use reifydb_core::frame::{Frame, FrameLayout};
use reifydb_core::interface::Rx;
use reifydb_core::{SortKey, error};
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

        while let Some(Batch { frame }) = self.input.next(ctx, rx)? {
            if let Some(existing_frame) = &mut frame_opt {
                for (i, col) in frame.columns.into_iter().enumerate() {
                    existing_frame.columns[i].values_mut().extend(col.values().clone())?;
                }
            } else {
                frame_opt = Some(frame);
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
                    .find(|c| {
                        c.qualified_name() == key.column.fragment || c.name() == key.column.fragment
                    })
                    .ok_or_else(|| error!(query::column_not_found(key.column.clone())))?;
                Ok::<_, reifydb_core::Error>((col.values().clone(), key.direction.clone()))
            })
            .collect::<crate::Result<Vec<_>>>()?;

        let row_count = frame.row_count();
        let mut indices: Vec<usize> = (0..row_count).collect();

        indices.sort_unstable_by(|&l, &r| {
            for (col, dir) in &key_refs {
                let vl = col.get_value(l);
                let vr = col.get_value(r);
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
            col.values_mut().reorder(&indices);
        }

        Ok(Some(Batch { frame }))
    }

    fn layout(&self) -> Option<FrameLayout> {
        self.input.layout()
    }
}
