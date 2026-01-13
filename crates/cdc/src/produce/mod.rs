// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! CDC Production module.
//!
//! This module provides the infrastructure for generating Change Data Capture events
//! from database commits. It is designed to be independent of the MVCC storage layer,
//! using traits for version resolution.
//!
//! Key components:
//! - `CdcWorker`: Single-threaded worker for background CDC generation
//! - `CdcEventListener`: Listens to PostCommitEvent and forwards to worker

mod listener;
mod worker;

pub use worker::{CdcWorker, CdcWorkItem};
pub use listener::CdcEventListener;
