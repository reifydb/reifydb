// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::result::frame::{Frame, FrameColumnLayout};

#[derive(Debug, Clone)]
pub struct FrameLayout {
    pub columns: Vec<FrameColumnLayout>,
}

impl FrameLayout {
    pub fn from_frame(frame: &Frame) -> Self {
        Self { columns: frame.columns.iter().map(|c| FrameColumnLayout::from_column(c)).collect() }
    }
}
