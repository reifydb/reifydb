// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub use convert::Convert;
pub use demote::Demote;
pub use promote::Promote;

mod arith;
mod convert;
mod demote;
mod promote;

use reifydb_core::frame::FrameColumn;
use reifydb_core::{
    BitVec, ColumnDescriptor,
    interface::{ColumnPolicyKind, ColumnSaturationPolicy, DEFAULT_COLUMN_SATURATION_POLICY},
};

#[derive(Debug)]
pub(crate) struct EvaluationContext<'a> {
    pub(crate) target_column: Option<ColumnDescriptor<'a>>,
    pub(crate) column_policies: Vec<ColumnPolicyKind>,
    pub(crate) mask: BitVec,
    pub(crate) columns: Vec<FrameColumn>,
    pub(crate) row_count: usize,
    pub(crate) take: Option<usize>,
}

impl<'a> EvaluationContext<'a> {
    #[cfg(test)]
    pub fn testing() -> Self {
        Self {
            target_column: None,
            column_policies: Vec::new(),
            mask: BitVec::new(0, false),
            columns: vec![],
            row_count: 1,
            take: None,
        }
    }

    pub(crate) fn saturation_policy(&self) -> &ColumnSaturationPolicy {
        self.column_policies
            .iter()
            .find_map(|p| match p {
                ColumnPolicyKind::Saturation(policy) => Some(policy),
            })
            .unwrap_or(&DEFAULT_COLUMN_SATURATION_POLICY)
    }
}
