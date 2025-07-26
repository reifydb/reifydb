// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file


#![cfg_attr(not(debug_assertions), deny(warnings))]


pub use reifydb_core::Result;

pub use engine::Engine;
pub use execute::{execute_rx, execute_tx};
use reifydb_core::frame::ColumnValues;

mod engine;
mod evaluate;
pub(crate) mod execute;

// Helper function for creating FrameColumn for expressions (no source frame)
pub(crate) fn create_frame_column(name: impl Into<String>, values: ColumnValues) -> reifydb_core::frame::FrameColumn {
    let name = name.into();
    reifydb_core::frame::FrameColumn::new(
        None, // Expressions have no source frame
        name,
        values,
    )
}

#[allow(dead_code)]
mod function;
mod system;
#[allow(dead_code)]
pub(crate) mod view;
