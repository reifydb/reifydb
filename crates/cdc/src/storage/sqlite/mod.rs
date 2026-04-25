// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! SQLite-backed CDC storage.
//!
//! Provides on-disk CDC persistence. RAM is bounded by SQLite's page cache;
//! disk is bounded only by retention policy (keep-forever by default).

pub mod config;
pub mod connection;
pub mod storage;
