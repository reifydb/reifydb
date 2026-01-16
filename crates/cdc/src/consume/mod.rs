// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! CDC consumption module
//!
//! This module provides the consumer-side functionality for CDC:
//! - Consumer traits for processing CDC events
//! - Checkpoint management for tracking consumer progress
//! - Poll-based consumer implementation
//! - Watermark computation for retention coordination

pub mod checkpoint;
pub mod consumer;
pub mod host;
pub mod poll;
pub mod watermark;
