// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// Presentation-only module (AGPL-specific)
mod render;

// Re-export core Frame types from reifydb-type
pub use reifydb_type::{
	Frame, FrameColumn, FrameColumnData, FrameError, FrameRow, FrameRows, FromFrame, FromFrameError,
};
// Local exports
pub use render::FrameRenderer;
