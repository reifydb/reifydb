// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::frame::{ColumnLayout, Frame};

#[derive(Debug, Clone)]
pub struct FrameLayout {
    pub columns: Vec<ColumnLayout>,
}

impl FrameLayout {
    pub fn from_frame(frame: &Frame) -> Self {
        Self { columns: frame.columns.iter().map(|c| ColumnLayout::from_column(c)).collect() }
    }
}
