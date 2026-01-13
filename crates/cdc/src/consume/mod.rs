// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! CDC consumption module
//!
//! This module provides the consumer-side functionality for CDC:
//! - Consumer traits for processing CDC events
//! - Checkpoint management for tracking consumer progress
//! - Poll-based consumer implementation
//! - Watermark computation for retention coordination

mod checkpoint;
mod consumer;
mod host;
mod poll;
mod watermark;

pub use checkpoint::CdcCheckpoint;
pub use consumer::{CdcConsume, CdcConsumer};
pub use host::CdcHost;
pub use poll::{PollConsumer, PollConsumerConfig};
pub use watermark::compute_watermark;
