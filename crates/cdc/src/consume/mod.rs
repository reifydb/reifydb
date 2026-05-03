// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Consumer side of the CDC stream. A consumer registers with the host actor, polls for new records past its
//! checkpoint, and advances a watermark so the producer side knows what is safe to compact. Each subscriber holds
//! its own checkpoint independently; a slow consumer never blocks a fast one.
//!
//! The checkpoint and watermark are persisted alongside the CDC log so a consumer that disappears and comes back
//! resumes from where it left off rather than re-reading.

pub mod actor;
pub mod checkpoint;
pub mod consumer;
pub mod host;
pub mod poll;
pub mod watermark;
