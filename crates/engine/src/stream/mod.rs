// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

//! Streaming result types for async query execution.
//!
//! This module provides channel-based stream implementations and re-exports
//! core stream types.

mod channel;

pub use channel::{ChannelFrameStream, FrameSender, StreamHandle, StreamId};
// Re-export core stream types
pub use reifydb_core::stream::{FrameSchema, SendableFrameStream, StreamConfig, StreamError, StreamResult};
