// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::Error;
use crate::execute::query::{Batch, ExecutionPlan};
use crate::frame::{Frame, FrameLayout};
use reifydb_core::OrderDirection::{Asc, Desc};
use reifydb_core::{BitVec, OrderKey};
use reifydb_diagnostic::query;
use std::cmp::Ordering::Equal;

pub(crate) struct OrderNode {
    input: Box<dyn ExecutionPlan>,
    by: Vec<OrderKey>,
}

impl OrderNode {
    pub(crate) fn new(input: Box<dyn ExecutionPlan>, by: Vec<OrderKey>) -> Self {
        Self { input, by }
    }
}

impl ExecutionPlan for OrderNode {
    fn next(&mut self) -> crate::Result<Option<Batch>> {
        let mut frame_opt: Option<Frame> = None;
        let mut mask_opt: Option<BitVec> = None;

        while let Some(Batch { frame, mask }) = self.input.next()? {
            if let Some(existing_frame) = &mut frame_opt {
                for (i, col) in frame.columns.into_iter().enumerate() {
                    existing_frame.columns[i].data.extend(col.data)?;
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
                    .find(|c| c.name == key.column.fragment)
                    .ok_or_else(|| Error(query::column_not_found(key.column.clone())))?;
                Ok::<_, crate::Error>((&col.data, &key.direction))
            })
            .collect::<Result<Vec<_>, _>>()?;

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
            col.data.reorder(&indices);
        }

        Ok(Some(Batch { frame, mask: mask_opt.unwrap() }))
    }

    fn layout(&self) -> Option<FrameLayout> {
        self.input.layout()
    }
}
