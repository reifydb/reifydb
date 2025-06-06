// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_catalog::ColumnPolicy;
use reifydb_core::ValueKind;
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
}
