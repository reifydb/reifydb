// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::columnar::Columns;
use crate::columnar::layout::ColumnsLayout;
use crate::execute::{Batch, ExecutionContext, ExecutionPlan};
use reifydb_core::SortDirection::{Asc, Desc};
use reifydb_core::interface::Rx;
use reifydb_core::result::error::diagnostic::query;
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
        let mut columns_opt: Option<Columns> = None;

        while let Some(Batch { columns }) = self.input.next(ctx, rx)? {
            if let Some(existing_columns) = &mut columns_opt {
                for (i, col) in columns.into_iter().enumerate() {
                    existing_columns[i].data_mut().extend(col.data().clone())?;
                }
            } else {
                columns_opt = Some(columns);
            }
        }

        let mut columns = match columns_opt {
            Some(f) => f,
            None => return Ok(None),
        };

        let key_refs = self
            .by
            .iter()
            .map(|key| {
                let col = columns
                    .iter()
                    .find(|c| {
                        c.qualified_name() == key.column.fragment || c.name() == key.column.fragment
                    })
                    .ok_or_else(|| error!(query::column_not_found(key.column.clone())))?;
                Ok::<_, reifydb_core::Error>((col.data().clone(), key.direction.clone()))
            })
            .collect::<crate::Result<Vec<_>>>()?;

        let row_count = columns.row_count();
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

        for col in columns.iter_mut() {
            col.data_mut().reorder(&indices);
        }

        Ok(Some(Batch { columns }))
    }

    fn layout(&self) -> Option<ColumnsLayout> {
        self.input.layout()
    }
}
