// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Transactional (inline) view processing subsystem.
//!
//! Transactional views are updated synchronously within the same transaction
//! that writes to their source tables, providing immediate consistency.

pub mod interceptor;
pub mod registrar;
