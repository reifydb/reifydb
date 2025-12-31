// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// Presentation-only module (AGPL-specific)
mod render;

// Re-export core Frame types from reifydb-type
pub use reifydb_type::{
	Frame, FrameColumn, FrameColumnData, FrameError, FrameRow, FrameRows, FromFrame, FromFrameError,
};
// Local exports
pub use render::FrameRenderer;
