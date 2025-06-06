// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_catalog::{ColumnOverflowPolicy, ColumnPolicy, DEFAULT_COLUMN_OVERFLOW_POLICY};
use reifydb_core::ValueKind;
use reifydb_core::num::SafeAdd;
use reifydb_diagnostic::Span;
use reifydb_diagnostic::policy::{ColumnOverflow, column_overflow};
use reifydb_frame::Frame;

#[derive(Debug)]
pub(crate) struct EvaluationColumn {
    pub(crate) name: String,
    pub(crate) value: ValueKind,
    pub(crate) policies: Vec<ColumnPolicy>,
}

#[derive(Debug)]
pub(crate) struct Context {
    pub(crate) column: Option<EvaluationColumn>,
    pub(crate) frame: Option<Frame>,
}

impl Context {
    pub(crate) fn row_count_or_one(&self) -> usize {
        self.frame.as_ref().map(|f| f.row_count()).unwrap_or(1)
    }

    pub(crate) fn overflow_policy(&self) -> &ColumnOverflowPolicy {
        self.column
            .as_ref()
            .map(|c| {
                c.policies
                    .iter()
                    .find_map(|p| match p {
                        ColumnPolicy::Overflow(policy) => Some(policy),
                        _ => None,
                    })
                    .unwrap_or(&DEFAULT_COLUMN_OVERFLOW_POLICY)
            })
            .unwrap_or(&DEFAULT_COLUMN_OVERFLOW_POLICY)
    }
}

impl Context {
    pub(crate) fn add<T: SafeAdd>(
        &self,
        l: T,
        r: T,
        span: &Span,
    ) -> crate::evaluate::Result<Option<T>> {
        match self.overflow_policy() {
            ColumnOverflowPolicy::Error => l
                .checked_add(r)
                .ok_or_else(|| {
                    if let Some(column) = &self.column {
                        return crate::evaluate::Error(column_overflow(ColumnOverflow {
                            span: span.clone(),
                            column: column.name.to_string(),
                            value: column.value,
                        }));
                    }
                    // expression_overflow
                    unimplemented!()
                })
                .map(Some),
            // OverflowPolicy::Saturate => Ok(a.saturating_add(b)),
            // OverflowPolicy::Wrap => Ok(a.wrapping_add(b)),
            ColumnOverflowPolicy::Undefined => Ok(None),
        }
    }
}
